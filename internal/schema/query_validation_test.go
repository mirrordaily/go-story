package schema

import (
	"testing"

	"go-story/internal/data"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/gqlerrors"
)

func TestGraphQLQueries_Validate(t *testing.T) {
	// 只做 schema 驗證（不執行 resolver），因此 repo 不需要連 DB。
	repo := &data.Repo{}
	sch, err := Build(repo)
	if err != nil {
		t.Fatalf("Build schema: %v", err)
	}

	fragmentPostDetail := `
	fragment PostDetail on Post {
		id
		title
		publishedDate
		createdAt
		heroImage {
			id
			imageFile { width height }
			resized { original w480 }
			resizedWebp { original w480 }
		}
	}
	`

	getPostByID := `
		query GetPostById($id: ID!) {
			post(where: { id: $id }) {
				...PostDetail
			}
		}
		` + fragmentPostDetail

	getPostsBySameSection := `
		query GetPostsBySameSection(
			$take: Int!
			$slug: String
			$publishedDate: DateTime
		) {
			posts(
				take: $take
				where: {
					state: { equals: "published" }
					sections: { some: { slug: { equals: $slug } } }
					publishedDate: { lt: $publishedDate }
				}
				orderBy: [{ createdAt: desc }]
			) {
				...PostDetail
			}
		}
		` + fragmentPostDetail

	testCases := []struct {
		name  string
		query string
	}{
		{"GetPostById", getPostByID},
		{"GetPostsBySameSection", getPostsBySameSection},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo := graphql.NewTypeInfo(&graphql.TypeInfoConfig{Schema: &sch})

			astDoc, err := parser.Parse(parser.ParseParams{Source: tc.query})
			if err != nil {
				t.Fatalf("Parse query: %v", err)
			}

			// VisitUsingRules 會做 schema 驗證；不執行 resolver。
			errors := graphql.VisitUsingRules(&sch, typeInfo, astDoc, graphql.SpecifiedRules)
			if len(errors) > 0 {
				msgs := make([]string, 0, len(errors))
				for _, e := range errors {
					msgs = append(msgs, formatGQLError(e))
				}
				t.Fatalf("GraphQL validation errors: %v", msgs)
			}
		})
	}
}

func formatGQLError(e gqlerrors.FormattedError) string {
	if e.Message != "" {
		return e.Message
	}
	return e.Error()
}

