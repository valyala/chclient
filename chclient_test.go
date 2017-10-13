package chclient

import (
	"fmt"
	"github.com/valyala/tsvreader"
	"testing"
)

// This test works only if local clickhouse is installed
func TestClientDo(t *testing.T) {
	expectedRows := int(1e6)
	query := fmt.Sprintf("SELECT * from system.numbers limit %d", expectedRows)

	c := &Client{}
	err := c.Do(query, func(r *tsvreader.Reader) {
		i := 0
		for r.Next() {
			n := r.Int()
			if n != i {
				t.Fatalf("unexpected result: %d. Expecting %d", n, i)
			}
			i++
		}
		if i != expectedRows {
			t.Fatalf("unexpected rows number: %d. Expecting %d", i, expectedRows)
		}
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}
