package chclient

import (
	"fmt"
	"github.com/valyala/tsvreader"
	"testing"
	"time"
)

// This test works only if local clickhouse is installed
func TestClientDoNoCompression(t *testing.T) {
	c := &Client{
		Timeout:          5 * time.Second,
		CompressResponse: false,
	}
	testClientDo(t, c)
}

// This test works only if local clickhouse is installed
func TestClientDoWithCompression(t *testing.T) {
	c := &Client{
		Timeout:          5 * time.Second,
		CompressResponse: true,
	}
	testClientDo(t, c)
}

func testClientDo(t *testing.T, c *Client) {
	expectedRows := int(1e6)
	query := fmt.Sprintf("SELECT number, number+1 FROM system.numbers LIMIT %d", expectedRows)

	if err := c.Ping(); err != nil {
		t.Fatalf("error in ping: %s", err)
	}
	err := c.Do(query, func(r *tsvreader.Reader) error {
		i := 0
		for r.Next() {
			n := r.Int()
			if n != i {
				return fmt.Errorf("unexpected col1: %d. Expecting %d", n, i)
			}
			m := r.Int()
			if m != n+1 {
				return fmt.Errorf("unexpected col2: %d. Expecting %d", m, n+1)
			}
			i++
		}
		if err := r.Error(); err != nil {
			return err
		}
		if i != expectedRows {
			return fmt.Errorf("unexpected rows number: %d. Expecting %d", i, expectedRows)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestPrepareRequest(t *testing.T) {
	testCases := []struct {
		name     string
		params   []string
		expected string
	}{
		{
			name:     "empty params",
			expected: "http://localhost:8123/?user=default",
		},
		{
			name: "set params",
			params: []string{
				"no_cache=1",
				"default_format=Pretty",
			},
			expected: "http://localhost:8123/?no_cache=1&default_format=Pretty&user=default",
		},
		{
			name: "overriding params",
			params: []string{
				"user=foo",
			},
			expected: "http://localhost:8123/?user=foo&user=default",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{
				URLParams: tc.params,
			}
			req := c.prepareRequest(c.addr(), "SELECT * FROM system.numbers LIMIT 10")
			got := req.URL.String()
			if got != tc.expected {
				t.Fatalf("got: %q; expected: %q", got, tc.expected)
			}
		})
	}
}
