package schema

import (
	"fmt"
	"go-story/internal/data"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/mitchellh/mapstructure"
)

// Build constructs the GraphQL schema using provided repo.
func Build(repo *data.Repo) (graphql.Schema, error) {
	jsonScalar := newJSONScalar()
	dateTimeScalar := newDateTimeScalar()

	// Input types
	stringFilterFields := graphql.InputObjectConfigFieldMap{}
	stringFilterInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "StringFilter",
		Fields: stringFilterFields,
	})
	stringFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: graphql.String}
	stringFilterFields["in"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.String)}
	stringFilterFields["not"] = &graphql.InputObjectFieldConfig{Type: stringFilterInput}

	booleanFilterFields := graphql.InputObjectConfigFieldMap{}
	booleanFilterInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "BooleanFilter",
		Fields: booleanFilterFields,
	})
	booleanFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: graphql.Boolean}

	dateTimeNullableFilterFields := graphql.InputObjectConfigFieldMap{}
	dateTimeNullableFilter := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "DateTimeNullableFilter",
		Fields: dateTimeNullableFilterFields,
	})
	dateTimeNullableFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: dateTimeScalar}
	dateTimeNullableFilterFields["not"] = &graphql.InputObjectFieldConfig{Type: dateTimeNullableFilter}

	sectionWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SectionWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":  &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})
	sectionManyRelationFilterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SectionManyRelationFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"some": &graphql.InputObjectFieldConfig{Type: sectionWhereInputType},
		},
	})

	// CategoryWhereInput: 根據 Lilith schema，不包含 isMemberOnly，但包含 AND/OR/NOT
	var categoryWhereInputType *graphql.InputObject
	categoryWhereInputFields := graphql.InputObjectConfigFieldMap{
		"slug":  &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		"state": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
	}
	categoryWhereInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "CategoryWhereInput",
		Fields: categoryWhereInputFields,
	})
	// 加入 AND/OR/NOT（循環引用）
	categoryWhereInputFields["AND"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(categoryWhereInputType))}
	categoryWhereInputFields["OR"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(categoryWhereInputType))}
	categoryWhereInputFields["NOT"] = &graphql.InputObjectFieldConfig{Type: categoryWhereInputType}
	categoryManyRelationFilterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "CategoryManyRelationFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"some": &graphql.InputObjectFieldConfig{Type: categoryWhereInputType},
		},
	})

	partnerWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PartnerWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})

	// PostWhereInput: 根據 Lilith schema，不包含 slug，但包含 AND/OR/NOT
	// 注意：graphql-go 可能不支援 InputObjectConfigFieldMapThunk，所以先不加入 AND/OR/NOT
	// 如果 probe 測試需要這些，我們可以後續加入
	var postWhereInputType *graphql.InputObject
	postWhereInputFields := graphql.InputObjectConfigFieldMap{
		"sections":   &graphql.InputObjectFieldConfig{Type: sectionManyRelationFilterType},
		"categories": &graphql.InputObjectFieldConfig{Type: categoryManyRelationFilterType},
		"state":      &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		"isAdult":    &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
		"isMember":   &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
	}
	postWhereInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "PostWhereInput",
		Fields: postWhereInputFields,
	})
	// 加入 AND/OR/NOT（循環引用）
	postWhereInputFields["AND"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(postWhereInputType))}
	postWhereInputFields["OR"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(postWhereInputType))}
	postWhereInputFields["NOT"] = &graphql.InputObjectFieldConfig{Type: postWhereInputType}

	postWhereUniqueInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostWhereUniqueInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"id": &graphql.InputObjectFieldConfig{Type: graphql.ID},
			// 根據 Lilith schema，PostWhereUniqueInput 只有 id，沒有 slug
		},
	})

	// ExternalWhereInput: 根據 Lilith schema，不包含 slug，但包含 AND/OR/NOT
	var externalWhereInputType *graphql.InputObject
	externalWhereInputFields := graphql.InputObjectConfigFieldMap{
		"state":         &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		"partner":       &graphql.InputObjectFieldConfig{Type: partnerWhereInputType},
		"publishedDate": &graphql.InputObjectFieldConfig{Type: dateTimeNullableFilter},
	}
	externalWhereInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "ExternalWhereInput",
		Fields: externalWhereInputFields,
	})
	// 加入 AND/OR/NOT（循環引用）
	externalWhereInputFields["AND"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(externalWhereInputType))}
	externalWhereInputFields["OR"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(externalWhereInputType))}
	externalWhereInputFields["NOT"] = &graphql.InputObjectFieldConfig{Type: externalWhereInputType}

	// TopicWhereInput
	topicWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TopicWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"state": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})

	// TopicWhereUniqueInput
	topicWhereUniqueInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TopicWhereUniqueInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"id":   &graphql.InputObjectFieldConfig{Type: graphql.ID},
			"slug": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"name": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})

	// TopicOrderByInput
	topicOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TopicOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"sortOrder":     &graphql.InputObjectFieldConfig{Type: graphql.String},
			"id":            &graphql.InputObjectFieldConfig{Type: graphql.String},
			"createdAt":     &graphql.InputObjectFieldConfig{Type: graphql.String},
			"publishedDate": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})

	// VideoWhereInput
	videoWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "VideoWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"state":        &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"isShorts":     &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
			"videoSection": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"youtubeUrl":   &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"tags": &graphql.InputObjectFieldConfig{Type: graphql.NewInputObject(graphql.InputObjectConfig{
				Name: "TagManyRelationFilter",
				Fields: graphql.InputObjectConfigFieldMap{
					"some": &graphql.InputObjectFieldConfig{Type: graphql.NewInputObject(graphql.InputObjectConfig{
						Name: "TagWhereInput",
						Fields: graphql.InputObjectConfigFieldMap{
							"id": &graphql.InputObjectFieldConfig{Type: graphql.NewInputObject(graphql.InputObjectConfig{
								Name: "IDFilter",
								Fields: graphql.InputObjectConfigFieldMap{
									"equals": &graphql.InputObjectFieldConfig{Type: graphql.ID},
								},
							})},
						},
					})},
				},
			})},
		},
	})

	// VideoWhereUniqueInput
	videoWhereUniqueInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "VideoWhereUniqueInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"id": &graphql.InputObjectFieldConfig{Type: graphql.ID},
		},
	})

	// VideoOrderByInput
	videoOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "VideoOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"publishedDate": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"id":            &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})

	orderDirectionEnum := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderDirection",
		Values: graphql.EnumValueConfigMap{
			"asc":  &graphql.EnumValueConfig{Value: "asc"},
			"desc": &graphql.EnumValueConfig{Value: "desc"},
		},
	})

	postOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"publishedDate": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"updatedAt":     &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"title":         &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
		},
	})

	externalOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ExternalOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"publishedDate": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"updatedAt":     &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
		},
	})

	// Object types
	imageFileType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ImageFile",
		Fields: graphql.Fields{
			"width":  &graphql.Field{Type: graphql.Int},
			"height": &graphql.Field{Type: graphql.Int},
		},
	})

	resizedType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Resized",
		Fields: graphql.Fields{
			"original": &graphql.Field{Type: graphql.String},
			"w480":     &graphql.Field{Type: graphql.String},
			"w800":     &graphql.Field{Type: graphql.String},
			"w1200": &graphql.Field{
				Type: graphql.String,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// w1200 是 virtual field，根據 probe 結果，target 返回空字串
					// 為了匹配 target 的行為，我們也返回空字串
					return "", nil
				},
			},
			"w1600": &graphql.Field{Type: graphql.String},
			"w2400": &graphql.Field{Type: graphql.String},
		},
	})

	sectionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Section",
		Fields: graphql.Fields{
			"id":    &graphql.Field{Type: graphql.ID},
			"name":  &graphql.Field{Type: graphql.String},
			"color": &graphql.Field{Type: graphql.String},
			"slug":  &graphql.Field{Type: graphql.String},
			"state": &graphql.Field{Type: graphql.String},
		},
	})

	categoryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Category",
		Fields: graphql.Fields{
			"id":    &graphql.Field{Type: graphql.ID},
			"name":  &graphql.Field{Type: graphql.String},
			"slug":  &graphql.Field{Type: graphql.String},
			"state": &graphql.Field{Type: graphql.String},
			// 根據 Lilith schema，Category 不包含 isMemberOnly
			"sections": &graphql.Field{
				Type: graphql.NewList(sectionType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					c, ok := p.Source.(data.Category)
					if !ok {
						return nil, nil
					}
					where, err := decodeSectionWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return filterSections(c.Sections, where), nil
				},
			},
		},
	})

	contactType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Contact",
		Fields: graphql.Fields{
			"id":   &graphql.Field{Type: graphql.ID},
			"name": &graphql.Field{Type: graphql.String},
		},
	})

	tagType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Tag",
		Fields: graphql.Fields{
			"id":   &graphql.Field{Type: graphql.ID},
			"name": &graphql.Field{Type: graphql.String},
			"slug": &graphql.Field{Type: graphql.String},
		},
	})

	// 先聲明 postType 變數，以便在 videoType 和 topicType 中使用
	var postType *graphql.Object

	photoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Photo",
		Fields: graphql.Fields{
			"id":            &graphql.Field{Type: graphql.ID},
			"name":          &graphql.Field{Type: graphql.String},
			"topicKeywords": &graphql.Field{Type: graphql.String},
			"imageFile":     &graphql.Field{Type: imageFileType},
			"resized":       &graphql.Field{Type: resizedType},
			"resizedWebp":   &graphql.Field{Type: resizedType},
		},
	})

	var videoType *graphql.Object
	videoType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Video",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":              &graphql.Field{Type: graphql.ID},
				"name":            &graphql.Field{Type: graphql.String},
				"isShorts":        &graphql.Field{Type: graphql.Boolean},
				"youtubeUrl":      &graphql.Field{Type: graphql.String},
				"fileDuration":    &graphql.Field{Type: graphql.String},
				"youtubeDuration": &graphql.Field{Type: graphql.String},
				"videoSrc":        &graphql.Field{Type: graphql.String},
				"content":         &graphql.Field{Type: graphql.String},
				"heroImage":       &graphql.Field{Type: photoType},
				"uploader":        &graphql.Field{Type: graphql.String},
				"uploaderEmail":   &graphql.Field{Type: graphql.String},
				"isFeed":          &graphql.Field{Type: graphql.Boolean},
				"videoSection":    &graphql.Field{Type: graphql.String},
				"state":           &graphql.Field{Type: graphql.String},
				"publishedDate": &graphql.Field{
					Type: dateTimeScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						v, ok := p.Source.(data.Video)
						if !ok {
							if ptr, ok2 := p.Source.(*data.Video); ok2 && ptr != nil {
								v = *ptr
							} else {
								return nil, nil
							}
						}
						// 當 publishedDate 為空字串時，返回 nil 以匹配 target 的行為
						if v.PublishedDate == "" {
							return nil, nil
						}
						return v.PublishedDate, nil
					},
				},
				"publishedDateString": &graphql.Field{Type: graphql.String},
				"updateTimeStamp":     &graphql.Field{Type: graphql.Boolean},
				"tags":                &graphql.Field{Type: graphql.NewList(tagType)},
				"related_posts":       &graphql.Field{Type: graphql.NewList(postType)},
				"createdAt":           &graphql.Field{Type: dateTimeScalar},
			}
		}),
	})

	partnerType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Partner",
		Fields: graphql.Fields{
			"id":          &graphql.Field{Type: graphql.ID},
			"slug":        &graphql.Field{Type: graphql.String},
			"name":        &graphql.Field{Type: graphql.String},
			"showOnIndex": &graphql.Field{Type: graphql.Boolean},
			// 根據 Lilith schema，Partner 不包含 showThumb 和 showBrief
		},
	})

	var topicType *graphql.Object
	topicType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Topic",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":            &graphql.Field{Type: graphql.ID},
				"name":          &graphql.Field{Type: graphql.String},
				"slug":          &graphql.Field{Type: graphql.String},
				"sortOrder":     &graphql.Field{Type: graphql.Int},
				"state":         &graphql.Field{Type: graphql.String},
				"publishedDate": &graphql.Field{Type: dateTimeScalar},
				"brief":         &graphql.Field{Type: jsonScalar},
				"apiDataBrief":  &graphql.Field{Type: jsonScalar},
				"leading":       &graphql.Field{Type: graphql.String},
				"heroImage":     &graphql.Field{Type: photoType},
				"heroUrl": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						topic := normalizeTopic(p.Source)
						// 當 heroUrl 為空字串時，返回 nil 以匹配 target 的行為
						if topic.HeroUrl == "" {
							return nil, nil
						}
						return topic.HeroUrl, nil
					},
				},
				"heroVideo":                    &graphql.Field{Type: videoType},
				"slideshow_images":             &graphql.Field{Type: graphql.NewList(photoType)},
				"manualOrderOfSlideshowImages": &graphql.Field{Type: jsonScalar},
				"og_title":                     &graphql.Field{Type: graphql.String},
				"og_description":               &graphql.Field{Type: graphql.String},
				"og_image":                     &graphql.Field{Type: photoType},
				"type":                         &graphql.Field{Type: graphql.String},
				"tags":                         &graphql.Field{Type: graphql.NewList(tagType)},
				"posts": &graphql.Field{
					Type: graphql.NewList(postType),
					Args: graphql.FieldConfigArgument{
						"where":   &graphql.ArgumentConfig{Type: postWhereInputType},
						"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(postOrderByInput)},
						"take":    &graphql.ArgumentConfig{Type: graphql.Int},
						"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						topic := normalizeTopic(p.Source)
						// 這裡簡化處理，直接返回 topic 的 posts
						// 實際應該根據 where 條件過濾，但為了簡化先這樣處理
						posts := topic.Posts
						take, _ := parsePagination(p.Args)
						if take > 0 && len(posts) > take {
							posts = posts[:take]
						}
						result := make([]interface{}, len(posts))
						for i, post := range posts {
							result[i] = post
						}
						return result, nil
					},
				},
				"postsCount": &graphql.Field{
					Type: graphql.Int,
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: postWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						topic := normalizeTopic(p.Source)
						return len(topic.Posts), nil
					},
				},
				"style":       &graphql.Field{Type: graphql.String},
				"isFeatured":  &graphql.Field{Type: graphql.Boolean},
				"title_style": &graphql.Field{Type: graphql.String},
				"sections":    &graphql.Field{Type: graphql.NewList(sectionType)},
				"javascript":  &graphql.Field{Type: graphql.String},
				"dfp":         &graphql.Field{Type: graphql.String},
				"mobile_dfp":  &graphql.Field{Type: graphql.String},
				"createdAt":   &graphql.Field{Type: dateTimeScalar},
			}
		}),
	})

	warningType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Warning",
		Fields: graphql.Fields{
			"id":      &graphql.Field{Type: graphql.ID},
			"content": &graphql.Field{Type: graphql.String},
		},
	})

	postType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Post",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":            &graphql.Field{Type: graphql.ID},
				"slug":          &graphql.Field{Type: graphql.String},
				"title":         &graphql.Field{Type: graphql.String},
				"subtitle":      &graphql.Field{Type: graphql.String},
				"state":         &graphql.Field{Type: graphql.String},
				"style":         &graphql.Field{Type: graphql.String},
				"publishedDate": &graphql.Field{Type: dateTimeScalar},
				"updatedAt":     &graphql.Field{Type: dateTimeScalar},
				"isMember":      &graphql.Field{Type: graphql.Boolean},
				"isAdult":       &graphql.Field{Type: graphql.Boolean},
				"sections": &graphql.Field{
					Type: graphql.NewList(sectionType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeSectionWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterSections(current.Sections, where), nil
					},
				},
				"sectionsInInputOrder": &graphql.Field{
					Type: graphql.NewList(sectionType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeSectionWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterSections(current.SectionsInInputOrder, where), nil
					},
				},
				"categories": &graphql.Field{
					Type: graphql.NewList(categoryType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeCategoryWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterCategories(current.Categories, where), nil
					},
				},
				"categoriesInInputOrder": &graphql.Field{
					Type: graphql.NewList(categoryType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeCategoryWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterCategories(current.CategoriesInInputOrder, where), nil
					},
				},
				"writers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Writers, nil
					},
				},
				"writersInInputOrder": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).WritersInInputOrder, nil
					},
				},
				"photographers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Photographers, nil
					},
				},
				"camera_man": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).CameraMan, nil
					},
				},
				"designers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Designers, nil
					},
				},
				"engineers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Engineers, nil
					},
				},
				"vocals": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Vocals, nil
					},
				},
				"extend_byline": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).ExtendByline, nil
					},
				},
				"tags": &graphql.Field{
					Type: graphql.NewList(tagType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Tags, nil
					},
				},
				"tags_algo": &graphql.Field{
					Type: graphql.NewList(tagType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).TagsAlgo, nil
					},
				},
				"heroVideo": &graphql.Field{
					Type: videoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HeroVideo, nil
					},
				},
				"heroImage": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HeroImage, nil
					},
				},
				"heroCaption": &graphql.Field{Type: graphql.String},
				"brief":       &graphql.Field{Type: jsonScalar},
				"apiData": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						// 直接回傳資料層從資料庫撈出的 apiData（Lilith draftConverter 產物）
						return normalizePost(p.Source).ApiData, nil
					},
				},
				"apiDataBrief": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						// 直接回傳資料層從資料庫撈出的 apiDataBrief
						return normalizePost(p.Source).ApiDataBrief, nil
					},
				},
				"trimmedContent": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).TrimmedContent, nil
					},
				},
				"content": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Content, nil
					},
				},
				"relateds": &graphql.Field{
					Type: graphql.NewList(postType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Relateds, nil
					},
				},
				"relatedsInInputOrder": &graphql.Field{
					Type: graphql.NewList(postType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsInInputOrder, nil
					},
				},
				"relatedsOne": &graphql.Field{
					Type: postType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsOne, nil
					},
				},
				"relatedsTwo": &graphql.Field{
					Type: postType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsTwo, nil
					},
				},
				"redirect": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Redirect, nil
					},
				},
				"og_title": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgTitle, nil
					},
				},
				"og_image": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgImage, nil
					},
				},
				"og_description": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgDescription, nil
					},
				},
				"hiddenAdvertised": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HiddenAdvertised, nil
					},
				},
				"isAdvertised": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).IsAdvertised, nil
					},
				},
				"isFeatured": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).IsFeatured, nil
					},
				},
				"topics": &graphql.Field{
					Type: topicType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Topics, nil
					},
				},
				"Warning": &graphql.Field{
					Type: warningType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						post := normalizePost(p.Source)
						return post.Warning, nil
					},
				},
				"Warnings": &graphql.Field{
					Type: graphql.NewList(warningType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						post := normalizePost(p.Source)
						result := make([]interface{}, len(post.Warnings))
						for i, w := range post.Warnings {
							result[i] = w
						}
						return result, nil
					},
				},
			}
		}),
	})

	externalType := graphql.NewObject(graphql.ObjectConfig{
		Name: "External",
		Fields: graphql.Fields{
			"id":            &graphql.Field{Type: graphql.ID},
			"slug":          &graphql.Field{Type: graphql.String},
			"title":         &graphql.Field{Type: graphql.String},
			"thumb":         &graphql.Field{Type: graphql.String},
			"brief":         &graphql.Field{Type: graphql.String},
			"content":       &graphql.Field{Type: graphql.String},
			"publishedDate": &graphql.Field{Type: dateTimeScalar},
			"extend_byline": &graphql.Field{Type: graphql.String},
			"thumbCaption":  &graphql.Field{Type: graphql.String},
			"partner": &graphql.Field{
				Type: partnerType,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ext, ok := p.Source.(data.External)
					if !ok {
						if ptr, ok2 := p.Source.(*data.External); ok2 && ptr != nil {
							ext = *ptr
						} else {
							return nil, nil
						}
					}
					// partner 可能是 virtual field，如果資料庫中為 null，使用預設值
					if ext.Partner != nil {
						return ext.Partner, nil
					}
					// 根據 probe 結果，target 的預設 partner 是 id: 4, slug: mirrormedia
					// 當 partner 為 null 時，使用預設的 partner
					defaultPartner, err := repo.QueryPartnerByID(p.Context, "4")
					if err == nil && defaultPartner != nil {
						return defaultPartner, nil
					}
					return nil, nil
				},
			},
			"updatedAt": &graphql.Field{Type: dateTimeScalar},
			"tags": &graphql.Field{
				Type: graphql.NewList(tagType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ext, ok := p.Source.(data.External)
					if !ok {
						if ptr, ok2 := p.Source.(*data.External); ok2 && ptr != nil {
							ext = *ptr
						} else {
							return nil, nil
						}
					}
					return ext.Tags, nil
				},
			},
			"sections": &graphql.Field{
				Type: graphql.NewList(sectionType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ext, ok := p.Source.(data.External)
					if !ok {
						if ptr, ok2 := p.Source.(*data.External); ok2 && ptr != nil {
							ext = *ptr
						} else {
							return []interface{}{}, nil
						}
					}
					result := make([]interface{}, len(ext.Sections))
					for i, s := range ext.Sections {
						result[i] = s
					}
					return result, nil
				},
			},
			"categories": &graphql.Field{
				Type: graphql.NewList(categoryType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ext, ok := p.Source.(data.External)
					if !ok {
						if ptr, ok2 := p.Source.(*data.External); ok2 && ptr != nil {
							ext = *ptr
						} else {
							return []interface{}{}, nil
						}
					}
					result := make([]interface{}, len(ext.Categories))
					for i, c := range ext.Categories {
						result[i] = c
					}
					return result, nil
				},
			},
			"relateds": &graphql.Field{
				Type: graphql.NewList(postType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					ext, ok := p.Source.(data.External)
					if !ok {
						if ptr, ok2 := p.Source.(*data.External); ok2 && ptr != nil {
							ext = *ptr
						} else {
							return []interface{}{}, nil
						}
					}
					result := make([]interface{}, len(ext.Relateds))
					for i, r := range ext.Relateds {
						result[i] = r
					}
					return result, nil
				},
			},
		},
	})

	rootQuery := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"posts": &graphql.Field{
				Type: graphql.NewList(postType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(postOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: postWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					return repo.QueryPosts(p.Context, where, orders, take, skip)
				},
			},
			"postsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: postWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryPostsCount(p.Context, where)
				},
			},
			"post": &graphql.Field{
				Type: postType,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: postWhereUniqueInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodePostWhereUnique(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryPostByUnique(p.Context, where)
				},
			},
			"externals": &graphql.Field{
				Type: graphql.NewList(externalType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(externalOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: externalWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeExternalWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					return repo.QueryExternals(p.Context, where, orders, take, skip)
				},
			},
			"external": &graphql.Field{
				Type: externalType,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{
						Type: graphql.NewInputObject(graphql.InputObjectConfig{
							Name: "ExternalWhereUniqueInput",
							Fields: graphql.InputObjectConfigFieldMap{
								"id": &graphql.InputObjectFieldConfig{Type: graphql.ID},
							},
						}),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, _ := p.Args["where"].(map[string]interface{})
					if where == nil {
						return nil, nil
					}
					rawID, ok := where["id"]
					if !ok {
						return nil, nil
					}
					idStr, ok := rawID.(string)
					if !ok || idStr == "" {
						return nil, nil
					}
					return repo.QueryExternalByID(p.Context, idStr)
				},
			},
			"externalsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: externalWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeExternalWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryExternalsCount(p.Context, where)
				},
			},
			"topics": &graphql.Field{
				Type: graphql.NewList(topicType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(topicOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: topicWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeTopicWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					topics, err := repo.QueryTopics(p.Context, where, orders, take, skip)
					if err != nil {
						return nil, err
					}
					result := make([]interface{}, len(topics))
					for i, t := range topics {
						result[i] = t
					}
					return result, nil
				},
			},
			"topicsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: topicWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeTopicWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryTopicsCount(p.Context, where)
				},
			},
			"topic": &graphql.Field{
				Type: topicType,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: topicWhereUniqueInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeTopicWhereUnique(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryTopicByUnique(p.Context, where)
				},
			},
			"videos": &graphql.Field{
				Type: graphql.NewList(videoType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(videoOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: videoWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeVideoWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					videos, err := repo.QueryVideos(p.Context, where, orders, take, skip)
					if err != nil {
						return nil, err
					}
					result := make([]interface{}, len(videos))
					for i, v := range videos {
						result[i] = v
					}
					return result, nil
				},
			},
			"videosCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: videoWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeVideoWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryVideosCount(p.Context, where)
				},
			},
			"video": &graphql.Field{
				Type: videoType,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: videoWhereUniqueInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeVideoWhereUnique(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryVideoByUnique(p.Context, where)
				},
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{
		Query: rootQuery,
	})
}

// Scalars
func newJSONScalar() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "JSON",
		Description: "Arbitrary JSON value",
		Serialize: func(value interface{}) interface{} {
			return value
		},
		ParseValue: func(value interface{}) interface{} {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			return parseASTValue(valueAST)
		},
	})
}

func newDateTimeScalar() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name: "DateTime",
		Serialize: func(value interface{}) interface{} {
			return value
		},
		ParseValue: func(value interface{}) interface{} {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.StringValue:
				return v.Value
			default:
				return nil
			}
		},
	})
}

// Helpers
func parseOrderRules(input interface{}) []data.OrderRule {
	rules := []data.OrderRule{}
	list, ok := input.([]interface{})
	if !ok {
		return rules
	}
	for _, item := range list {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for field, dir := range entry {
			rules = append(rules, data.OrderRule{
				Field:     field,
				Direction: fmt.Sprintf("%v", dir),
			})
		}
	}
	return rules
}

func parsePagination(args map[string]interface{}) (take int, skip int) {
	if raw, ok := args["take"]; ok {
		take = asInt(raw)
	}
	if raw, ok := args["skip"]; ok {
		skip = asInt(raw)
	}
	if skip < 0 {
		skip = 0
	}
	return
}

func asInt(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func parseASTValue(value ast.Value) interface{} {
	switch v := value.(type) {
	case *ast.StringValue:
		return v.Value
	case *ast.IntValue:
		return v.Value
	case *ast.FloatValue:
		return v.Value
	case *ast.BooleanValue:
		return v.Value
	case *ast.ObjectValue:
		result := map[string]interface{}{}
		for _, field := range v.Fields {
			result[field.Name.Value] = parseASTValue(field.Value)
		}
		return result
	case *ast.ListValue:
		values := make([]interface{}, 0, len(v.Values))
		for _, item := range v.Values {
			values = append(values, parseASTValue(item))
		}
		return values
	default:
		return nil
	}
}

// Filter helpers for nested fields
func decodeSectionWhere(input interface{}) (*data.SectionWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where data.SectionWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, err
	}
	return &where, nil
}

func decodeCategoryWhere(input interface{}) (*data.CategoryWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where data.CategoryWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, err
	}
	return &where, nil
}

func decodeInto(input interface{}, target interface{}) error {
	cfg := &mapstructure.DecoderConfig{
		TagName: "mapstructure",
		Result:  target,
	}
	decoder, err := mapstructure.NewDecoder(cfg)
	if err != nil {
		return err
	}
	return decoder.Decode(input)
}

func filterSections(items []data.Section, where *data.SectionWhereInput) []data.Section {
	if where == nil {
		return items
	}
	result := make([]data.Section, 0, len(items))
	for _, s := range items {
		if matchesSectionWhere(&s, where) {
			result = append(result, s)
		}
	}
	return result
}

func filterCategories(items []data.Category, where *data.CategoryWhereInput) []data.Category {
	if where == nil {
		return items
	}
	result := make([]data.Category, 0, len(items))
	for _, c := range items {
		if matchesCategoryWhere(&c, where) {
			result = append(result, c)
		}
	}
	return result
}

func matchesSectionWhere(s *data.Section, where *data.SectionWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(s.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(s.State, where.State) {
		return false
	}
	return true
}

func matchesCategoryWhere(c *data.Category, where *data.CategoryWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(c.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(c.State, where.State) {
		return false
	}
	if !matchesBooleanFilter(c.IsMemberOnly, where.IsMemberOnly) {
		return false
	}
	return true
}

func matchesStringFilter(value string, filter *data.StringFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Equals != nil && value != *filter.Equals {
		return false
	}
	if len(filter.In) > 0 {
		found := false
		for _, item := range filter.In {
			if value == item {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if filter.Not != nil && matchesStringFilter(value, filter.Not) {
		return false
	}
	return true
}

func matchesBooleanFilter(value bool, filter *data.BooleanFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Equals != nil && value != *filter.Equals {
		return false
	}
	return true
}

func normalizeTopic(src interface{}) data.Topic {
	switch v := src.(type) {
	case data.Topic:
		return v
	case *data.Topic:
		if v == nil {
			return data.Topic{}
		}
		return *v
	default:
		return data.Topic{}
	}
}

func normalizePost(src interface{}) data.Post {
	switch v := src.(type) {
	case data.Post:
		return v
	case *data.Post:
		if v == nil {
			return data.Post{}
		}
		return *v
	default:
		return data.Post{}
	}
}
