package data

import (
	"database/sql"
	"testing"
)

func TestRelatedImageMeta(t *testing.T) {
	meta := relatedImageMeta(sql.NullInt64{Int64: 7, Valid: true}, sql.NullInt64{})
	if getMetaInt(meta, "heroImageID") != 7 {
		t.Fatalf("heroImageID mismatch: %v", meta)
	}
	if _, ok := meta["ogImageID"]; ok {
		t.Fatalf("ogImageID should be absent: %v", meta)
	}
}

func TestApplyRelatedDetails(t *testing.T) {
	hero := &Photo{ID: "10"}
	og := &Photo{ID: "20"}
	sections := []Section{{ID: "1", Name: "即時"}}
	posts := []Post{
		{ID: "100", Metadata: map[string]any{"heroImageID": 10, "ogImageID": 20}},
		{ID: "200", Metadata: map[string]any{}},
	}

	applyRelatedDetails(posts, map[int]*Photo{10: hero, 20: og}, map[int][]Section{100: sections})

	if posts[0].HeroImage != hero || posts[0].OgImage != og {
		t.Fatalf("images not applied: %+v", posts[0])
	}
	if len(posts[0].Sections) != 1 || len(posts[0].SectionsInInputOrder) != 1 {
		t.Fatalf("sections not applied: %+v", posts[0])
	}
	if posts[1].HeroImage != nil || posts[1].OgImage != nil || posts[1].Sections != nil {
		t.Fatalf("post without images or sections changed: %+v", posts[1])
	}
}
