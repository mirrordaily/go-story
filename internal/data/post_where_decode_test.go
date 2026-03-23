package data

import (
	"testing"
)

func TestDecodePostWhere_PublishedDateLt(t *testing.T) {
	in := map[string]any{
		"publishedDate": map[string]any{
			"lt": "2026-01-01T00:00:00.000Z",
		},
	}

	where, err := DecodePostWhere(in)
	if err != nil {
		t.Fatalf("DecodePostWhere: %v", err)
	}
	if where == nil {
		t.Fatalf("where is nil")
	}
	if where.PublishedDate == nil {
		t.Fatalf("PublishedDate is nil")
	}
	if where.PublishedDate.Lt == nil {
		t.Fatalf("PublishedDate.Lt is nil")
	}
	if *where.PublishedDate.Lt != "2026-01-01T00:00:00.000Z" {
		t.Fatalf("PublishedDate.Lt mismatch: %q", *where.PublishedDate.Lt)
	}
}

