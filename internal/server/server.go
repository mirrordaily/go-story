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
	GQLErrors  []string        `json:"gqlErrors,omitempty"` // GraphQL errors 的簡要資訊
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
		Name            string   `json:"name"`
		Match           bool     `json:"match"`
		TargetStatus    int      `json:"targetStatus"`
		SelfStatus      int      `json:"selfStatus"`
		TargetError     string   `json:"targetError,omitempty"`
		SelfError       string   `json:"selfError,omitempty"`
		TargetGQLErrors []string `json:"targetGQLErrors,omitempty"`
		SelfGQLErrors   []string `json:"selfGQLErrors,omitempty"`
		Note            string   `json:"note,omitempty"`
	}

	results := []compare{}
	for _, tr := range targetResults {
		sr := selfMap[tr.Name]
		match, note := compareBodies(tr, sr)
		results = append(results, compare{
			Name:            tr.Name,
			Match:           match,
			TargetStatus:    tr.StatusCode,
			SelfStatus:      sr.StatusCode,
			TargetError:     tr.Error,
			SelfError:       sr.Error,
			TargetGQLErrors: tr.GQLErrors,
			SelfGQLErrors:   sr.GQLErrors,
			Note:            note,
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

	// 簡化版 postGQL：只保留 GetPostById，移除所有 fragments 和 OR 查詢
	const postGQL = `query GetPostById($id: ID!) {
  post(where: { id: $id }) {
    id
    title
    subtitle
    heroCaption
    publishedDate
    hiddenAdvertised
    heroImage {
      id
      imageFile {
        width
        height
      }
      resized {
        original
        w480
        w800
        w1200
        w1600
        w2400
      }
      resizedWebp {
        original
        w480
        w800
        w1200
        w1600
        w2400
      }
    }
    og_image {
      id
      imageFile {
        width
        height
      }
      resized {
        original
        w480
        w800
        w1200
        w1600
        w2400
      }
      resizedWebp {
        original
        w480
        w800
        w1200
        w1600
        w2400
      }
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
      id
      slug
      title
      heroImage {
        id
        imageFile {
          width
          height
        }
        resized {
          original
          w480
          w800
          w1200
          w1600
          w2400
        }
      }
    }
    relatedsTwo {
      id
      slug
      title
      heroImage {
        id
        imageFile {
          width
          height
        }
        resized {
          original
          w480
          w800
          w1200
          w1600
          w2400
        }
      }
    }
    relateds {
      id
      slug
      title
      heroImage {
        id
        imageFile {
          width
          height
        }
        resized {
          original
          w480
          w800
          w1200
          w1600
          w2400
        }
      }
    }
  }
}

const postGQLOriginal = `query GetPostsBySectionSlug(
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
      id
      slug
      title
      heroImage {
        id
        imageFile {
          width
          height
        }
        resized {
          original
          w480
          w800
          w1200
          w1600
          w2400
        }
      }
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

	// probeSampleVars 會對 target GraphQL 發出簡單查詢，取得一組可用的 post / external / partner 參考值
	// 若失敗則回傳空字串，後續測試會用空變數，讓差異顯示在比對結果中。
	samples := probeSampleVars(client, target)
	postID := samples["postID"]
	postSlug := samples["postSlug"]
	externalID := samples["externalID"]
	externalSlug := samples["externalSlug"]

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
					"id": postID,
				},
			},
		},
		{
			name: "post_by_slug",
			body: map[string]any{
				"query": `query ($slug:String){ 
					posts(where:{slug:{equals:$slug}}){
						id slug title state 
					} 
				}`,
				"variables": map[string]any{
					"slug": postSlug,
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
						partner{ id slug name showOnIndex }
						updatedAt
					}
				}`,
				"variables": map[string]any{
					"slug": externalSlug,
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
					"id": externalID,
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
			// 嘗試解析 GraphQL errors
			var gqlResp struct {
				Errors []struct {
					Message string      `json:"message"`
					Path    interface{} `json:"path,omitempty"`
				} `json:"errors"`
			}
			if json.Unmarshal(body, &gqlResp) == nil && len(gqlResp.Errors) > 0 {
				res.GQLErrors = make([]string, 0, len(gqlResp.Errors))
				for _, e := range gqlResp.Errors {
					res.GQLErrors = append(res.GQLErrors, e.Message)
				}
			}
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

	// 解析 GraphQL response 結構
	type gqlResponse struct {
		Data   interface{} `json:"data"`
		Errors interface{} `json:"errors"`
	}

	var targetResp, selfResp gqlResponse
	if err := json.Unmarshal(target.Body, &targetResp); err != nil {
		return false, fmt.Sprintf("target JSON parse error: %v", err)
	}
	if err := json.Unmarshal(self.Body, &selfResp); err != nil {
		return false, fmt.Sprintf("self JSON parse error: %v", err)
	}

	// 檢查 errors：如果兩邊都有 errors 或都沒有 errors，繼續比對 data
	// 如果一邊有 errors 另一邊沒有，則不 match
	targetHasErrors := targetResp.Errors != nil && !isEmptyValue(targetResp.Errors)
	selfHasErrors := selfResp.Errors != nil && !isEmptyValue(selfResp.Errors)
	if targetHasErrors != selfHasErrors {
		return false, fmt.Sprintf("errors mismatch: target has errors=%v, self has errors=%v", targetHasErrors, selfHasErrors)
	}

	// 比對 data 部分（使用深度比對，但忽略順序差異）
	if deepEqualData(targetResp.Data, selfResp.Data) {
		return true, ""
	}

	// 如果 data 不同，嘗試提供更詳細的差異資訊
	diff := findDataDifference(targetResp.Data, selfResp.Data)
	if diff != "" {
		return false, fmt.Sprintf("data differ: %s", diff)
	}
	return false, "data structure differs"
}

// isEmptyValue 檢查值是否為空（nil, 空陣列, 空 map）
func isEmptyValue(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() == 0
	case reflect.String:
		return rv.Len() == 0
	}
	return false
}

// deepEqualData 比對兩個 data 物件，忽略 JSON 欄位順序
func deepEqualData(a, b interface{}) bool {
	// 先做標準深度比對
	if reflect.DeepEqual(a, b) {
		return true
	}

	// 如果都是 map，遞迴比對每個 key
	aMap, aIsMap := a.(map[string]interface{})
	bMap, bIsMap := b.(map[string]interface{})
	if aIsMap && bIsMap {
		if len(aMap) != len(bMap) {
			return false
		}
		for k, av := range aMap {
			bv, ok := bMap[k]
			if !ok {
				return false
			}
			if !deepEqualData(av, bv) {
				return false
			}
		}
		return true
	}

	// 如果都是 slice，比對每個元素
	aSlice, aIsSlice := a.([]interface{})
	bSlice, bIsSlice := b.([]interface{})
	if aIsSlice && bIsSlice {
		if len(aSlice) != len(bSlice) {
			return false
		}
		// 對 slice 做寬鬆比對：允許順序不同（如果元素可比較）
		matched := make([]bool, len(bSlice))
		for _, ae := range aSlice {
			found := false
			for i, be := range bSlice {
				if !matched[i] && deepEqualData(ae, be) {
					matched[i] = true
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}

	return false
}

// findDataDifference 找出 data 差異的簡要描述
func findDataDifference(a, b interface{}) string {
	aMap, aIsMap := a.(map[string]interface{})
	bMap, bIsMap := b.(map[string]interface{})
	if aIsMap && bIsMap {
		for k := range aMap {
			if _, ok := bMap[k]; !ok {
				return fmt.Sprintf("missing key in self: %s", k)
			}
		}
		for k := range bMap {
			if _, ok := aMap[k]; !ok {
				return fmt.Sprintf("extra key in self: %s", k)
			}
		}
	}
	return "structure or value differs"
}

func normalizeJSON(raw []byte) (interface{}, error) {
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// probeSampleVars 從 target GraphQL 取一組實際存在的 post / external / partner 參考值，
// 以避免硬編 id / slug 造成 400 或比對失敗。
func probeSampleVars(client *http.Client, target string) map[string]string {
	out := map[string]string{}

	type gqlPayload struct {
		Query     string                 `json:"query"`
		Variables map[string]interface{} `json:"variables,omitempty"`
	}
	type gqlResponse struct {
		Data   map[string]json.RawMessage `json:"data"`
		Errors interface{}                `json:"errors"`
	}

	// 1) 抓一篇 post（已發佈）
	postReqBody := gqlPayload{
		Query: `query ($take:Int,$skip:Int,$orderBy:[PostOrderByInput],$filter:PostWhereInput){
  posts(take:$take,skip:$skip,orderBy:$orderBy,where:$filter){
    id
    slug
  }
}`,
		Variables: map[string]interface{}{
			"take":    1,
			"skip":    0,
			"orderBy": []map[string]string{{"publishedDate": "desc"}},
			"filter": map[string]interface{}{
				"state": map[string]interface{}{"equals": "published"},
			},
		},
	}
	if b, err := json.Marshal(postReqBody); err == nil {
		if req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(b)); err == nil {
			req.Header.Set("Content-Type", "application/json")
			if resp, err := client.Do(req); err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var gr gqlResponse
				if err := json.Unmarshal(body, &gr); err == nil && gr.Data != nil {
					if rawPosts, ok := gr.Data["posts"]; ok {
						var posts []struct {
							ID   string `json:"id"`
							Slug string `json:"slug"`
						}
						if err := json.Unmarshal(rawPosts, &posts); err == nil && len(posts) > 0 {
							out["postID"] = posts[0].ID
							out["postSlug"] = posts[0].Slug
						}
					}
				}
			}
		}
	}

	// 2) 抓一篇 external（已發佈，且有 partner）
	extReqBody := gqlPayload{
		Query: `query ($take:Int,$skip:Int,$orderBy:[ExternalOrderByInput],$filter:ExternalWhereInput){
  externals(take:$take,skip:$skip,orderBy:$orderBy,where:$filter){
    id
    slug
    partner{ slug }
  }
}`,
		Variables: map[string]interface{}{
			"take":    1,
			"skip":    0,
			"orderBy": []map[string]string{{"publishedDate": "desc"}},
			"filter": map[string]interface{}{
				"state":         map[string]interface{}{"equals": "published"},
				"publishedDate": map[string]interface{}{"not": map[string]interface{}{"equals": nil}},
			},
		},
	}
	if b, err := json.Marshal(extReqBody); err == nil {
		if req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(b)); err == nil {
			req.Header.Set("Content-Type", "application/json")
			if resp, err := client.Do(req); err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var gr gqlResponse
				if err := json.Unmarshal(body, &gr); err == nil && gr.Data != nil {
					if rawExts, ok := gr.Data["externals"]; ok {
						var exts []struct {
							ID      string `json:"id"`
							Slug    string `json:"slug"`
							Partner *struct {
								Slug string `json:"slug"`
							} `json:"partner"`
						}
						if err := json.Unmarshal(rawExts, &exts); err == nil && len(exts) > 0 {
							out["externalID"] = exts[0].ID
							out["externalSlug"] = exts[0].Slug
							if exts[0].Partner != nil {
								out["partnerSlug"] = exts[0].Partner.Slug
							}
						}
					}
				}
			}
		}
	}

	return out
}
