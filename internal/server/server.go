package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/graphql-go/graphql"
)

func NewGraphQLHandler(schema graphql.Schema) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("only POST is supported at /api/graphql"))
			return
		}

		var payload struct {
			Query         string                 `json:"query"`
			Variables     map[string]interface{} `json:"variables"`
			OperationName string                 `json:"operationName"`
		}

		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}

		result := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  payload.Query,
			VariableValues: payload.Variables,
			OperationName:  payload.OperationName,
			Context:        r.Context(),
		})

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})
}

type ProbeResult struct {
	Name       string          `json:"name"`
	StatusCode int             `json:"statusCode"`
	Body       json.RawMessage `json:"body,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// ProbeHandler runs a set of built-in GQL queries against target URL.
func ProbeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST", http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.URL == "" {
		http.Error(w, "invalid payload, need {\"url\": \"https://original-gql\"}", http.StatusBadRequest)
		return
	}

	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		scheme = "http"
	}
	selfURL := fmt.Sprintf("%s://%s/api/graphql", scheme, r.Host)

	targetResults := runProbeTests(payload.URL)
	selfResults := runProbeTests(selfURL)

	selfMap := map[string]ProbeResult{}
	for _, r := range selfResults {
		selfMap[r.Name] = r
	}

	type compare struct {
		Name         string `json:"name"`
		Match        bool   `json:"match"`
		TargetStatus int    `json:"targetStatus"`
		SelfStatus   int    `json:"selfStatus"`
		TargetError  string `json:"targetError,omitempty"`
		SelfError    string `json:"selfError,omitempty"`
		Note         string `json:"note,omitempty"`
	}

	results := []compare{}
	for _, tr := range targetResults {
		sr := selfMap[tr.Name]
		match, note := compareBodies(tr, sr)
		results = append(results, compare{
			Name:         tr.Name,
			Match:        match,
			TargetStatus: tr.StatusCode,
			SelfStatus:   sr.StatusCode,
			TargetError:  tr.Error,
			SelfError:    sr.Error,
			Note:         note,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"target":  payload.URL,
		"self":    selfURL,
		"results": results,
	})
}

func runProbeTests(target string) []ProbeResult {
	client := &http.Client{Timeout: 10 * time.Second}

	const postGQL = `query GetPostsBySectionSlug(
  $skip: Int!
  $take: Int
  $slug: String!
  $withAmount: Boolean! = false
) {
  posts(
    skip: $skip
    take: $take
    where: {
      state: { equals: "published" }
      sections: { some: { slug: { equals: $slug } } }
    }
    orderBy: [{ publishedDate: desc }]
  ) {
    ...PostOverview
  }
  postsCount(
    where: {
      state: { equals: "published" }
      sections: { some: { slug: { equals: $slug } } }
    }
  ) @include(if: $withAmount)
}

query GetPostsByCategorySlug(
  $skip: Int!
  $take: Int
  $slug: String!
  $withAmount: Boolean! = false
) {
  posts(
    skip: $skip
    take: $take
    where: {
      state: { equals: "published" }
      categories: { some: { slug: { equals: $slug } } }
    }
    orderBy: [{ publishedDate: desc }]
  ) {
    ...PostOverview
  }
  postsCount(
    where: {
      state: { equals: "published" }
      categories: { some: { slug: { equals: $slug } } }
    }
  ) @include(if: $withAmount)
}

query GetPostsByAuthorId(
  $skip: Int!
  $take: Int!
  $id: ID!
  $withAmount: Boolean! = false
) {
  posts(
    skip: $skip
    take: $take
    where: {
      state: { equals: "published" }
      OR: [
        { writers: { some: { id: { equals: $id } } } }
        { photographers: { some: { id: { equals: $id } } } }
        { designers: { some: { id: { equals: $id } } } }
        { engineers: { some: { id: { equals: $id } } } }
      ]
    }
    orderBy: [{ publishedDate: desc }]
  ) {
    sections {
      name
      color
    }
    ...PostOverview
  }
  postsCount(
    where: {
      state: { equals: "published" }
      OR: [
        { writers: { some: { id: { equals: $id } } } }
        { photographers: { some: { id: { equals: $id } } } }
        { designers: { some: { id: { equals: $id } } } }
        { engineers: { some: { id: { equals: $id } } } }
      ]
    }
  ) @include(if: $withAmount)
}

query GetPostsByTagSlug(
  $skip: Int!
  $take: Int!
  $slug: String!
  $withAmount: Boolean! = false
) {
  posts(
    skip: $skip
    take: $take
    where: {
      state: { equals: "published" }
      OR: [
        { tags: { some: { slug: { equals: $slug } } } }
        { tags_algo: { some: { slug: { equals: $slug } } } }
      ]
    }
    orderBy: [{ publishedDate: desc }]
  ) {
    sections {
      name
      color
    }
    ...PostOverview
  }
  postsCount(
    where: {
      state: { equals: "published" }
      OR: [
        { tags: { some: { slug: { equals: $slug } } } }
        { tags_algo: { some: { slug: { equals: $slug } } } }
      ]
    }
  ) @include(if: $withAmount)
}

query GetPostById($id: ID!) {
  post(where: { id: $id }) {
    id
    title
    subtitle
    heroCaption
    publishedDate
    hiddenAdvertised
    heroImage {
      ...ImageData
    }
    og_image {
      ...ImageData
    }
    tags {
      slug
      name
    }
    tags_algo {
      slug
      name
    }
    sections {
      name
      color
      slug
    }
    categories {
      name
      slug
    }
    writers {
      id
      name
    }
    photographers {
      id
      name
    }
    designers {
      id
      name
    }
    engineers {
      id
      name
    }
    apiData
    apiDataBrief
    Warning {
      id
      content
    }
    Warnings {
      id
      content
    }
    isAdult
  }
}

query GetRelatedPostsById($id: ID!) {
  post(where: { id: $id }) {
    relatedsOne {
      ...RelatedPost
    }
    relatedsTwo {
      ...RelatedPost
    }
    relatedsThree {
      ...RelatedPost
    }
    relateds {
      ...RelatedPost
    }
  }
}`

	const externalGQL = `query GetExternalById($id: ID!) {
  external(where: { id: $id }) {
    id
    title
    thumb
    thumbCaption
    publishedDate
    brief
    content
    tags {
      name
      slug
    }
    partner {
      name
      slug
    }
    sections {
      name
      color
      slug
    }
    categories {
      name
      slug
    }
  }
}

query GetRelatedPostsByExternalId($id: ID!) {
  external(where: { id: $id }) {
    relateds {
      ...RelatedPost
    }
  }
}

query GetExternalsByPartnerSlug(
  $skip: Int!
  $take: Int!
  $slug: String!
  $withAmount: Boolean! = false
) {
  externals(
    skip: $skip
    take: $take
    where: { partner: { slug: { equals: $slug } } }
    orderBy: { publishedDate: desc }
  ) {
    id
    title
    brief
    publishedDate
    thumb
  }
  externalsCount(where: { partner: { slug: { equals: $slug } } })
    @include(if: $withAmount)
}`

	tests := []struct {
		name string
		body map[string]any
	}{
		{
			name: "posts_list",
			body: map[string]any{
				"query": `query ($take:Int,$skip:Int,$orderBy:[PostOrderByInput!]!,$filter:PostWhereInput!){
					postsCount(where:$filter)
					posts(take:$take,skip:$skip,orderBy:$orderBy,where:$filter){
						id slug title publishedDate state
					}
				}`,
				"variables": map[string]any{
					"take":    3,
					"skip":    0,
					"orderBy": []map[string]string{{"publishedDate": "desc"}},
					"filter": map[string]any{
						"state": map[string]any{"equals": "published"},
					},
				},
			},
		},
		// Full post.gql: use operation GetPostById to覆蓋所有 fragment / 欄位行為
		{
			name: "post_gql_GetPostById",
			body: map[string]any{
				"query":         postGQL,
				"operationName": "GetPostById",
				"variables": map[string]any{
					"id": "1",
				},
			},
		},
		{
			name: "post_by_slug",
			body: map[string]any{
				"query": `query ($slug:String){ post(where:{slug:$slug}){ id slug title state } }`,
				"variables": map[string]any{
					"slug": "20251212-4-173036",
				},
			},
		},
		{
			name: "externals_list",
			body: map[string]any{
				"query": `query ($take:Int,$skip:Int,$orderBy:[ExternalOrderByInput!]!,$filter:ExternalWhereInput!){
					externals(take:$take,skip:$skip,orderBy:$orderBy,where:$filter){
						id slug title thumb brief publishedDate partner{ id slug name showOnIndex }
					}
				}`,
				"variables": map[string]any{
					"take":    3,
					"skip":    0,
					"orderBy": []map[string]string{{"publishedDate": "desc"}},
					"filter": map[string]any{
						"state":         map[string]any{"equals": "published"},
						"publishedDate": map[string]any{"not": map[string]any{"equals": nil}},
					},
				},
			},
		},
		{
			name: "external_by_slug",
			body: map[string]any{
				"query": `query ($slug:String){
					externals(where:{slug:{equals:$slug},state:{equals:"published"}}){
						id slug title thumb brief content publishedDate extend_byline thumbCaption
						partner{ id slug name showOnIndex showThumb showBrief }
						updatedAt
					}
				}`,
				"variables": map[string]any{
					"slug": "mirrordaily_35695",
				},
			},
		},
		// Full external.gql: use operation GetExternalById 覆蓋完整欄位
		{
			name: "external_gql_GetExternalById",
			body: map[string]any{
				"query":         externalGQL,
				"operationName": "GetExternalById",
				"variables": map[string]any{
					"id": "1",
				},
			},
		},
	}

	results := make([]ProbeResult, 0, len(tests))
	for _, t := range tests {
		res := ProbeResult{Name: t.name}
		b, _ := json.Marshal(t.body)
		req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(b))
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		res.StatusCode = resp.StatusCode
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			res.Error = err.Error()
		} else {
			res.Body = json.RawMessage(body)
		}
		results = append(results, res)
	}
	return results
}

func compareBodies(target ProbeResult, self ProbeResult) (bool, string) {
	// If either has transport error
	if target.Error != "" || self.Error != "" {
		return target.Error == "" && self.Error == "", "transport error"
	}
	if target.StatusCode != self.StatusCode {
		return false, "status code differ"
	}

	tObj, tErr := normalizeJSON(target.Body)
	sObj, sErr := normalizeJSON(self.Body)
	if tErr == nil && sErr == nil {
		if reflect.DeepEqual(tObj, sObj) {
			return true, ""
		}
		return false, "body JSON differ"
	}

	// fallback raw compare
	if bytes.Equal(target.Body, self.Body) {
		return true, ""
	}
	return false, "body differ"
}

func normalizeJSON(raw []byte) (interface{}, error) {
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}
