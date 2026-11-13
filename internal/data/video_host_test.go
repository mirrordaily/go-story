package data

import "testing"

func TestVideoFilesBase(t *testing.T) {
	cases := map[string]string{
		"https://statics-prod.mirrordaily.news/images/":    "https://statics-prod.mirrordaily.news/video-files",
		"https://statics-staging.mirrordaily.news/images/": "https://statics-staging.mirrordaily.news/video-files",
		"https://statics-dev.mirrordaily.news/images":      "https://statics-dev.mirrordaily.news/video-files",
	}
	for in, want := range cases {
		if got := videoFilesBase(in); got != want {
			t.Errorf("videoFilesBase(%q) = %q, want %q", in, got, want)
		}
	}
}
