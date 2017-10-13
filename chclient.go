package chclient

import (
	"bytes"
	"context"
	"fmt"
	"github.com/valyala/tsvreader"
	"io/ioutil"
	"net/http"
	"net/url"
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

	// Timeout is the maximum duration for the query.
	//
	// DefaultTimeout is used by default.
	Timeout time.Duration
}

// DefaultTimeout is the default timeout for Client.
var DefaultTimeout = 30 * time.Second

// Ping verifies that the client can connect to clickhouse.
func (c *Client) Ping() error {
	return c.Do("SELECT 1", nil)
}

// Do sends the given query to clickhouse and calls f for reading query results.
//
// f may be nil if query result isn't needed.
func (c *Client) Do(query string, f func(*tsvreader.Reader)) error {
	deadline := time.Now().Add(c.timeout())
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	req := c.prepareRequest(query)
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error when performing query %q at %q: %s", query, c.addr(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code for query %q sent to %q: %d. Response body: %q",
			query, c.addr(), resp.StatusCode, respBody)
	}
	if f == nil {
		return nil
	}

	r := tsvreader.New(resp.Body)
	ch := make(chan struct{})
	go func() {
		f(r)
		close(ch)
	}()

	select {
	case <-ch:
		return r.Error()
	case <-ctx.Done():
		// wait until f finishes
		<-ch
		return ctx.Err()
	}
}

func (c *Client) prepareRequest(query string) *http.Request {
	scheme := "http"
	if c.UseHTTPS {
		scheme = "https"
	}
	xurl := fmt.Sprintf("%s://%s/?user=%s", scheme, c.addr(), url.QueryEscape(c.user()))
	if c.Password != "" {
		xurl += fmt.Sprintf("&password=%s", url.QueryEscape(c.Password))
	}
	if c.Database != "" {
		xurl += fmt.Sprintf("&database=%s", url.QueryEscape(c.Database))
	}
	body := bytes.NewBufferString(query)
	req, err := http.NewRequest("POST", xurl, body)
	if err != nil {
		panic(fmt.Sprintf("BUG: cannot create request from xurl=%q, query=%q", xurl, query))
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
