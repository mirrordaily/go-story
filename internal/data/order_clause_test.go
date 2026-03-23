package data

import "testing"

func TestBuildOrderClause_CreatedAt(t *testing.T) {
	got := buildOrderClause(OrderRule{Field: "createdAt", Direction: "desc"})
	if got != `"createdAt" DESC` {
		t.Fatalf("buildOrderClause mismatch: got %q", got)
	}
}

