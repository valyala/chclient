package chclient

import (
	"bytes"
	"context"
	"fmt"
	"github.com/valyala/tsvreader"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Client is http client for clickhouse.
//
// The client is optimized for SELECT queries.
//
// Best used with github.com/Vertamedia/chproxy.
type Client struct {
	// Addr is clickhouse address to connect to.
	//
	// localhost:8123 is used by default.
	Addr string

	// FallbackAddr is a fallback clickhouse address that is used
	// if request to Addr fails.
	//
	// By default there is no fallback address.
	FallbackAddr string

	// User to use when connecting to clickhouse.
	//
	// User is `default` if not set.
	User string

	// Password to use when connecting to clickhouse.
	//
	// Password is empty if not set.
	Password string

	// Database to use.
	//
	// Database is `default` if not set.
	Database string

	// Whether to send requests over https.
	//
	// Requests are sent over http by default.
	UseHTTPS bool

	// Whether to request compressed responses from clickhouse.
	//
	// Response compression may reduce network usage.
	//
	// Response compression is disabled by default.
	CompressResponse bool

	// Timeout is the maximum duration for the query.
	//
	// DefaultTimeout is used by default.
	Timeout time.Duration

	// URLParams to add to URL before requesting clickhouse.
	//
	// For instance,
	//
	//     Client.URLParams = []string{
	//         "default_format=Pretty",
	//         "no_cache=1",
	//     }
	URLParams []string
}

// DefaultTimeout is the default timeout for Client.
var DefaultTimeout = 30 * time.Second

// ReadRowsFunc must read rows from r.
type ReadRowsFunc func(r *tsvreader.Reader) error

// Ping verifies that the client can connect to clickhouse.
func (c *Client) Ping() error {
	return c.Do("SELECT 1", nil)
}

// Do sends the given query to clickhouse and calls f for reading query results.
//
// The maximum query duration is limited by Client.Timeout.
//
// f may be nil if query result isn't needed.
func (c *Client) Do(query string, f ReadRowsFunc) error {
	deadline := time.Now().Add(c.timeout())
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	return c.DoContext(ctx, query, f)
}

// DoContext sends the given query using the given ctx to clickhouse
// and calls f for reading query results.
//
// The maximum query duration may be limited with the ctx.
//
// f may be nil if query result isn't needed.
func (c *Client) DoContext(ctx context.Context, query string, f ReadRowsFunc) error {
	addr := c.addr()
	resp, err := c.doRequest(ctx, addr, query)
	if err != nil {
		// Try requesting fallback address.
		addr = c.FallbackAddr
		if len(addr) == 0 {
			// There is no fallback address. Just return the error.
			return err
		}
		resp2, err2 := c.doRequest(ctx, addr, query)
		if err2 != nil {
			return fmt.Errorf("cannot request neither primary nor fallback address: %q and %q", err, err2)
		}
		resp = resp2
	}
	defer resp.Body.Close()

	if f == nil {
		return nil
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/tab-separated-values") {
		return fmt.Errorf("unexpected Content-Type for query %q sent to %q: %q. Expecting %q",
			query, addr, ct, "text/tab-separated-values")
	}

	r := tsvreader.New(resp.Body)
	if err := f(r); err != nil {
		return err
	}
	return r.Error()
}

func (c *Client) doRequest(ctx context.Context, addr, query string) (*http.Response, error) {
	req := c.prepareRequest(addr, query)
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		var re = regexp.MustCompile(`(?U)(password=).*(: |&.*)`)
		errWithoutPassword := re.ReplaceAllString(err.Error(), `${1}*removed*${2}`)
		return nil, fmt.Errorf("error when performing query %q at %q: %s", query, addr, errWithoutPassword)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code for query %q sent to %q: %d. Response body: %q",
			query, addr, resp.StatusCode, respBody)
	}
	return resp, nil
}

func (c *Client) prepareRequest(addr, query string) *http.Request {
	scheme := "http"
	if c.UseHTTPS {
		scheme = "https"
	}

	args := make([]string, 0, len(c.URLParams)+4)
	for _, p := range c.URLParams {
		args = append(args, p)
	}

	args = append(args, fmt.Sprintf("user=%s", url.QueryEscape(c.user())))
	if c.Password != "" {
		args = append(args, fmt.Sprintf("password=%s", url.QueryEscape(c.Password)))
	}
	if c.Database != "" {
		args = append(args, fmt.Sprintf("database=%s", url.QueryEscape(c.Database)))
	}
	if c.CompressResponse {
		args = append(args, "enable_http_compression=1")
	}
	xurl := fmt.Sprintf("%s://%s/?%s", scheme, addr, strings.Join(args, "&"))

	body := bytes.NewBufferString(query)
	req, err := http.NewRequest("POST", xurl, body)
	if err != nil {
		panic(fmt.Sprintf("BUG: cannot create request from xurl=%q, query=%q", xurl, query))
	}
	if !c.CompressResponse {
		// Explicitly disable response compression if it isn't enabled,
		// since net/http client by default transparently enables
		// response compression.
		// See DisableCompression at https://golang.org/pkg/net/http/ .
		req.Header.Set("Accept-Encoding", "identity")
	}

	return req
}

func (c *Client) addr() string {
	if c.Addr == "" {
		return "localhost:8123"
	}
	return c.Addr
}

func (c *Client) user() string {
	if c.User == "" {
		return "default"
	}
	return c.User
}

func (c *Client) timeout() time.Duration {
	if c.Timeout <= 0 {
		return DefaultTimeout
	}
	return c.Timeout
}
