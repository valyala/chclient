package chclient

import (
	"fmt"
	"github.com/valyala/tsvreader"
	"testing"
	"time"
)

// This test works only if local clickhouse is installed
func TestClientDo(t *testing.T) {
	expectedRows := int(1e6)
	query := fmt.Sprintf("SELECT * FROM system.numbers LIMIT %d", expectedRows)

	c := &Client{
		Timeout: 5 * time.Second,
	}
	if err := c.Ping(); err != nil {
		t.Fatalf("error in ping: %s", err)
	}
	err := c.Do(query, func(r *tsvreader.Reader) error {
		i := 0
		for r.Next() {
			n := r.Int()
			if n != i {
				return fmt.Errorf("unexpected result: %d. Expecting %d", n, i)
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
