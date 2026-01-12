package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mitchellh/mapstructure"
)

// Domain models
type ImageFile struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

type Resized struct {
	Original string `json:"original"`
	W480     string `json:"w480"`
	W800     string `json:"w800"`
	W1200    string `json:"w1200"`
	W1600    string `json:"w1600"`
	W2400    string `json:"w2400"`
}

type Photo struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	TopicKeywords string         `json:"topicKeywords"`
	ImageFile     ImageFile      `json:"imageFile"`
	Resized       Resized        `json:"resized"`
	ResizedWebp   Resized        `json:"resizedWebp"`
	Metadata      map[string]any `json:"-"`
}

type Section struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Slug  string `json:"slug"`
	State string `json:"state"`
	// Color 對應 Lilith 的 Section.color（可能為空字串，如果資料庫沒有這個欄位）
	Color string `json:"color"`
}

type Category struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Slug         string    `json:"slug"`
	State        string    `json:"state"`
	IsMemberOnly bool      `json:"isMemberOnly"`
	Sections     []Section `json:"sections"`
}

type Contact struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Tag struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type Warning struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

type Video struct {
	ID                  string         `json:"id"`
	Name                string         `json:"name"`
	IsShorts            bool           `json:"isShorts"`
	YoutubeUrl          string         `json:"youtubeUrl"`
	FileDuration        string         `json:"fileDuration"`
	YoutubeDuration     string         `json:"youtubeDuration"`
	VideoSrc            string         `json:"videoSrc"`
	Content             string         `json:"content"`
	HeroImage           *Photo         `json:"heroImage"`
	Uploader            string         `json:"uploader"`
	UploaderEmail       string         `json:"uploaderEmail"`
	IsFeed              bool           `json:"isFeed"`
	VideoSection        string         `json:"videoSection"`
	State               string         `json:"state"`
	PublishedDate       string         `json:"publishedDate"`
	PublishedDateString string         `json:"publishedDateString"`
	UpdateTimeStamp     bool           `json:"updateTimeStamp"`
	Tags                []Tag          `json:"tags"`
	RelatedPosts        []Post         `json:"related_posts"`
	CreatedAt           string         `json:"createdAt"`
	Metadata            map[string]any `json:"-"`
}

type Partner struct {
	ID          string `json:"id"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	ShowOnIndex bool   `json:"showOnIndex"`
	ShowThumb   bool   `json:"showThumb"`
	ShowBrief   bool   `json:"showBrief"`
}

type Topic struct {
	ID                           string         `json:"id"`
	Name                         string         `json:"name"`
	Slug                         string         `json:"slug"`
	SortOrder                    *int           `json:"sortOrder"`
	State                        string         `json:"state"`
	PublishedDate                string         `json:"publishedDate"`
	Brief                        map[string]any `json:"brief"`
	ApiDataBrief                 interface{}    `json:"apiDataBrief"`
	Leading                      string         `json:"leading"`
	HeroImage                    *Photo         `json:"heroImage"`
	HeroUrl                      string         `json:"heroUrl"`
	HeroVideo                    *Video         `json:"heroVideo"`
	SlideshowImages              []Photo        `json:"slideshow_images"`
	ManualOrderOfSlideshowImages map[string]any `json:"manualOrderOfSlideshowImages"`
	OgTitle                      string         `json:"og_title"`
	OgDescription                string         `json:"og_description"`
	OgImage                      *Photo         `json:"og_image"`
	Type                         string         `json:"type"`
	Tags                         []Tag          `json:"tags"`
	Posts                        []Post         `json:"posts"`
	Style                        string         `json:"style"`
	IsFeatured                   bool           `json:"isFeatured"`
	TitleStyle                   string         `json:"title_style"`
	Sections                     []Section      `json:"sections"`
	Javascript                   string         `json:"javascript"`
	Dfp                          string         `json:"dfp"`
	MobileDfp                    string         `json:"mobile_dfp"`
	CreatedAt                    string         `json:"createdAt"`
	Metadata                     map[string]any `json:"-"`
}

type Post struct {
	ID                     string         `json:"id"`
	Slug                   string         `json:"slug"`
	Title                  string         `json:"title"`
	Subtitle               string         `json:"subtitle"`
	State                  string         `json:"state"`
	Style                  string         `json:"style"`
	PublishedDate          string         `json:"publishedDate"`
	UpdatedAt              string         `json:"updatedAt"`
	IsMember               bool           `json:"isMember"`
	IsAdult                bool           `json:"isAdult"`
	Sections               []Section      `json:"sections"`
	SectionsInInputOrder   []Section      `json:"sectionsInInputOrder"`
	Categories             []Category     `json:"categories"`
	CategoriesInInputOrder []Category     `json:"categoriesInInputOrder"`
	Writers                []Contact      `json:"writers"`
	WritersInInputOrder    []Contact      `json:"writersInInputOrder"`
	Photographers          []Contact      `json:"photographers"`
	CameraMan              []Contact      `json:"camera_man"`
	Designers              []Contact      `json:"designers"`
	Engineers              []Contact      `json:"engineers"`
	Vocals                 []Contact      `json:"vocals"`
	ExtendByline           string         `json:"extend_byline"`
	Tags                   []Tag          `json:"tags"`
	TagsAlgo               []Tag          `json:"tags_algo"`
	HeroVideo              *Video         `json:"heroVideo"`
	HeroImage              *Photo         `json:"heroImage"`
	HeroCaption            string         `json:"heroCaption"`
	Brief                  map[string]any `json:"brief"`
	// ApiData / ApiDataBrief 對應 Lilith hooks 中 draftConverter 產生的 JSON 結構
	ApiDataBrief         interface{}    `json:"apiDataBrief"`
	ApiData              interface{}    `json:"apiData"`
	TrimmedContent       map[string]any `json:"trimmedContent"`
	Content              map[string]any `json:"content"`
	Relateds             []Post         `json:"relateds"`
	RelatedsInInputOrder []Post         `json:"relatedsInInputOrder"`
	RelatedsOne          *Post          `json:"relatedsOne"`
	RelatedsTwo          *Post          `json:"relatedsTwo"`
	RelatedsThree        *Post          `json:"relatedsThree"`
	Redirect             string         `json:"redirect"`
	OgTitle              string         `json:"og_title"`
	OgImage              *Photo         `json:"og_image"`
	OgDescription        string         `json:"og_description"`
	HiddenAdvertised     bool           `json:"hiddenAdvertised"`
	IsAdvertised         bool           `json:"isAdvertised"`
	IsFeatured           bool           `json:"isFeatured"`
	Topics               *Topic         `json:"topics"`
	Warning              *Warning       `json:"warning"`
	Warnings             []Warning      `json:"warnings"`
	Metadata             map[string]any `json:"-"`
}

type External struct {
	ID            string         `json:"id"`
	Slug          string         `json:"slug"`
	Partner       *Partner       `json:"partner"`
	Title         string         `json:"title"`
	State         string         `json:"state"`
	PublishedDate string         `json:"publishedDate"`
	ExtendByline  string         `json:"extend_byline"`
	Thumb         string         `json:"thumb"`
	ThumbCaption  string         `json:"thumbCaption"`
	Brief         string         `json:"brief"`
	Content       string         `json:"content"`
	UpdatedAt     string         `json:"updatedAt"`
	Tags          []Tag          `json:"tags"`
	Sections      []Section      `json:"sections"`
	Categories    []Category     `json:"categories"`
	Relateds      []Post         `json:"relateds"`
	Metadata      map[string]any `json:"metadata"`
}

// Filters
type StringFilter struct {
	Equals *string       `mapstructure:"equals"`
	In     []string      `mapstructure:"in"`
	Not    *StringFilter `mapstructure:"not"`
}

type BooleanFilter struct {
	Equals *bool `mapstructure:"equals"`
}

type SectionWhereInput struct {
	Slug  *StringFilter `mapstructure:"slug"`
	State *StringFilter `mapstructure:"state"`
}

type SectionManyRelationFilter struct {
	Some *SectionWhereInput `mapstructure:"some"`
}

type CategoryWhereInput struct {
	Slug         *StringFilter  `mapstructure:"slug"`
	State        *StringFilter  `mapstructure:"state"`
	IsMemberOnly *BooleanFilter `mapstructure:"isMemberOnly"`
}

type CategoryManyRelationFilter struct {
	Some *CategoryWhereInput `mapstructure:"some"`
}

type PartnerWhereInput struct {
	Slug *StringFilter `mapstructure:"slug"`
}

type DateTimeNullableFilter struct {
	Equals *string                 `mapstructure:"equals"`
	Not    *DateTimeNullableFilter `mapstructure:"not"`
}

type PostWhereInput struct {
	Slug       *StringFilter               `mapstructure:"slug"`
	Sections   *SectionManyRelationFilter  `mapstructure:"sections"`
	Categories *CategoryManyRelationFilter `mapstructure:"categories"`
	State      *StringFilter               `mapstructure:"state"`
	IsAdult    *BooleanFilter              `mapstructure:"isAdult"`
	IsMember   *BooleanFilter              `mapstructure:"isMember"`
}

type PostWhereUniqueInput struct {
	ID   *string `mapstructure:"id"`
	Slug *string `mapstructure:"slug"`
}

type ExternalWhereInput struct {
	Slug          *StringFilter           `mapstructure:"slug"`
	State         *StringFilter           `mapstructure:"state"`
	Partner       *PartnerWhereInput      `mapstructure:"partner"`
	PublishedDate *DateTimeNullableFilter `mapstructure:"publishedDate"`
}

type TopicWhereInput struct {
	State *StringFilter `mapstructure:"state"`
}

type TopicWhereUniqueInput struct {
	ID   *string `mapstructure:"id"`
	Slug *string `mapstructure:"slug"`
	Name *string `mapstructure:"name"`
}

type VideoWhereInput struct {
	State        *StringFilter          `mapstructure:"state"`
	IsShorts     *BooleanFilter         `mapstructure:"isShorts"`
	VideoSection *StringFilter          `mapstructure:"videoSection"`
	YoutubeUrl   *StringFilter          `mapstructure:"youtubeUrl"`
	Tags         *TagManyRelationFilter `mapstructure:"tags"`
}

type TagManyRelationFilter struct {
	Some *TagWhereInput `mapstructure:"some"`
}

type TagWhereInput struct {
	ID *IDFilter `mapstructure:"id"`
}

type IDFilter struct {
	Equals *string `mapstructure:"equals"`
}

type VideoWhereUniqueInput struct {
	ID *string `mapstructure:"id"`
}

type OrderRule struct {
	Field     string
	Direction string
}

// Repo wraps DB access.
type Repo struct {
	db          *sql.DB
	staticsHost string
	cache       *Cache
}

const timeLayoutMilli = "2006-01-02T15:04:05.000Z07:00"

func NewDB(dsn string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn := stdlib.OpenDB(*cfg)
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return conn, nil
}

func NewRepo(db *sql.DB, staticsHost string, cache *Cache) *Repo {
	return &Repo{db: db, staticsHost: staticsHost, cache: cache}
}

// Decode helpers
func DecodePostWhere(input interface{}) (*PostWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where PostWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("post where: %w", err)
	}
	return &where, nil
}

func DecodePostWhereUnique(input interface{}) (*PostWhereUniqueInput, error) {
	if input == nil {
		return nil, nil
	}
	// GraphQL ID 可能以 string 或數字形式傳入（特別是在 probe 時我們會傳 Int 給 target），
	// 這裡手動處理 id 欄位，確保最終轉成字串，避免出現空字串導致 SQL 轉型錯誤。
	if m, ok := input.(map[string]interface{}); ok {
		if rawID, ok2 := m["id"]; ok2 && rawID != nil {
			var idStr string
			switch v := rawID.(type) {
			case string:
				idStr = v
			case int:
				idStr = strconv.Itoa(v)
			case int64:
				idStr = strconv.FormatInt(v, 10)
			case float64:
				idStr = strconv.FormatInt(int64(v), 10)
			default:
				idStr = fmt.Sprintf("%v", v)
			}
			if idStr != "" {
				return &PostWhereUniqueInput{ID: &idStr}, nil
			}
		}
	}

	// 回退到一般的 mapstructure 解碼（主要支援 slug 等欄位）
	var where PostWhereUniqueInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("post unique where: %w", err)
	}
	return &where, nil
}

func DecodeExternalWhere(input interface{}) (*ExternalWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where ExternalWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("external where: %w", err)
	}
	return &where, nil
}

func DecodeTopicWhere(input interface{}) (*TopicWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where TopicWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("topic where: %w", err)
	}
	return &where, nil
}

func DecodeTopicWhereUnique(input interface{}) (*TopicWhereUniqueInput, error) {
	if input == nil {
		return nil, nil
	}
	var where TopicWhereUniqueInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("topic where unique: %w", err)
	}
	return &where, nil
}

func DecodeVideoWhere(input interface{}) (*VideoWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where VideoWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("video where: %w", err)
	}
	return &where, nil
}

func DecodeVideoWhereUnique(input interface{}) (*VideoWhereUniqueInput, error) {
	if input == nil {
		return nil, nil
	}
	var where VideoWhereUniqueInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("video where unique: %w", err)
	}
	return &where, nil
}

// Public queries
func (r *Repo) QueryPosts(ctx context.Context, where *PostWhereInput, orders []OrderRule, take, skip int) ([]Post, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensurePostPublished(where)

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("posts", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		var cachedPosts []Post
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedPosts); found {
			return cachedPosts, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, "apiDataBrief", "apiData", content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo", "relatedsThree" FROM "Post" p`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
		if len(f.In) > 0 {
			conds = append(conds, fmt.Sprintf(`%s = ANY($%d)`, field, argIdx))
			args = append(args, f.In)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("slug", where.Slug)
		buildStringFilter("state", where.State)
		if where.IsAdult != nil && where.IsAdult.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isAdult" = $%d`, argIdx))
			args = append(args, *where.IsAdult.Equals)
			argIdx++
		}
		if where.IsMember != nil && where.IsMember.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isMember" = $%d`, argIdx))
			args = append(args, *where.IsMember.Equals)
			argIdx++
		}
		if where.Sections != nil && where.Sections.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Post_sections\" ps JOIN \"Section\" s ON s.id = ps.\"B\" WHERE ps.\"A\" = p.id"
			if where.Sections.Some.Slug != nil && where.Sections.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND s.slug = $%d", argIdx)
				args = append(args, *where.Sections.Some.Slug.Equals)
				argIdx++
			}
			if where.Sections.Some.State != nil && where.Sections.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND s.state = $%d", argIdx)
				args = append(args, *where.Sections.Some.State.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
		if where.Categories != nil && where.Categories.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Category_posts\" cp JOIN \"Category\" c ON c.id = cp.\"A\" WHERE cp.\"B\" = p.id"
			if where.Categories.Some.Slug != nil && where.Categories.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND c.slug = $%d", argIdx)
				args = append(args, *where.Categories.Some.Slug.Equals)
				argIdx++
			}
			if where.Categories.Some.State != nil && where.Categories.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND c.state = $%d", argIdx)
				args = append(args, *where.Categories.Some.State.Equals)
				argIdx++
			}
			// isMemberOnly 欄位在資料庫中不存在，跳過此過濾條件
			// if where.Categories.Some.IsMemberOnly != nil && where.Categories.Some.IsMemberOnly.Equals != nil {
			// 	sub += fmt.Sprintf(" AND c.\"isMemberOnly\" = $%d", argIdx)
			// 	args = append(args, *where.Categories.Some.IsMemberOnly.Equals)
			// 	argIdx++
			// }
			sub += ")"
			conds = append(conds, sub)
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(buildOrderClause(orders[0]))
	} else {
		sb.WriteString(` ORDER BY "publishedDate" DESC`)
	}

	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	posts := []Post{}
	for rows.Next() {
		var (
			p               Post
			dbID            int
			publishedAt     sql.NullTime
			updatedAt       sql.NullTime
			heroImageID     sql.NullInt64
			heroVideoID     sql.NullInt64
			ogImageID       sql.NullInt64
			topicsID        sql.NullInt64
			relatedsOneID   sql.NullInt64
			relatedsTwoID   sql.NullInt64
			relatedsThreeID sql.NullInt64
			briefRaw        []byte
			apiDataBrief    []byte
			apiData         []byte
			contentRaw      []byte
		)
		if err := rows.Scan(
			&dbID,
			&p.Slug,
			&p.Title,
			&p.Subtitle,
			&p.State,
			&p.Style,
			&p.IsMember,
			&p.IsAdult,
			&publishedAt,
			&updatedAt,
			&p.HeroCaption,
			&p.ExtendByline,
			&heroImageID,
			&heroVideoID,
			&briefRaw,
			&apiDataBrief,
			&apiData,
			&contentRaw,
			&p.Redirect,
			&p.OgTitle,
			&p.OgDescription,
			&p.HiddenAdvertised,
			&p.IsAdvertised,
			&p.IsFeatured,
			&topicsID,
			&ogImageID,
			&relatedsOneID,
			&relatedsTwoID,
			&relatedsThreeID,
		); err != nil {
			return nil, err
		}
		p.ID = strconv.Itoa(dbID)
		if publishedAt.Valid {
			p.PublishedDate = publishedAt.Time.UTC().Format(timeLayoutMilli)
		}
		if updatedAt.Valid {
			p.UpdatedAt = updatedAt.Time.UTC().Format(timeLayoutMilli)
		}
		p.Brief = decodeJSONBytes(briefRaw)
		p.ApiDataBrief = decodeJSONBytesAny(apiDataBrief)
		p.ApiData = decodeJSONBytesAny(apiData)
		p.Content = decodeJSONBytes(contentRaw)
		p.TrimmedContent = p.Content
		p.Metadata = map[string]any{
			"heroImageID":     nullableInt(heroImageID),
			"ogImageID":       nullableInt(ogImageID),
			"heroVideoID":     nullableInt(heroVideoID),
			"topicsID":        nullableInt(topicsID),
			"relatedsOneID":   nullableInt(relatedsOneID),
			"relatedsTwoID":   nullableInt(relatedsTwoID),
			"relatedsThreeID": nullableInt(relatedsThreeID),
		}
		posts = append(posts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(posts) == 0 {
		return posts, nil
	}
	if err := r.enrichPosts(ctx, posts); err != nil {
		return nil, err
	}

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("posts", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		_ = r.cache.Set(ctx, cacheKey, posts)
	}

	return posts, nil
}

func (r *Repo) QueryPostsCount(ctx context.Context, where *PostWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	where = ensurePostPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "Post" p`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}
	if where != nil {
		buildStringFilter("slug", where.Slug)
		buildStringFilter("state", where.State)
		if where.IsAdult != nil && where.IsAdult.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isAdult" = $%d`, argIdx))
			args = append(args, *where.IsAdult.Equals)
			argIdx++
		}
		if where.IsMember != nil && where.IsMember.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isMember" = $%d`, argIdx))
			args = append(args, *where.IsMember.Equals)
			argIdx++
		}
		if where.Sections != nil && where.Sections.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Post_sections\" ps JOIN \"Section\" s ON s.id = ps.\"B\" WHERE ps.\"A\" = p.id"
			if where.Sections.Some.Slug != nil && where.Sections.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND s.slug = $%d", argIdx)
				args = append(args, *where.Sections.Some.Slug.Equals)
				argIdx++
			}
			if where.Sections.Some.State != nil && where.Sections.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND s.state = $%d", argIdx)
				args = append(args, *where.Sections.Some.State.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
		if where.Categories != nil && where.Categories.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Category_posts\" cp JOIN \"Category\" c ON c.id = cp.\"A\" WHERE cp.\"B\" = p.id"
			if where.Categories.Some.Slug != nil && where.Categories.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND c.slug = $%d", argIdx)
				args = append(args, *where.Categories.Some.Slug.Equals)
				argIdx++
			}
			if where.Categories.Some.State != nil && where.Categories.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND c.state = $%d", argIdx)
				args = append(args, *where.Categories.Some.State.Equals)
				argIdx++
			}
			// isMemberOnly 欄位在資料庫中不存在，跳過此過濾條件
			// if where.Categories.Some.IsMemberOnly != nil && where.Categories.Some.IsMemberOnly.Equals != nil {
			// 	sub += fmt.Sprintf(" AND c.\"isMemberOnly\" = $%d", argIdx)
			// 	args = append(args, *where.Categories.Some.IsMemberOnly.Equals)
			// 	argIdx++
			// }
			sub += ")"
			conds = append(conds, sub)
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	var count int
	if err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Repo) QueryPostByUnique(ctx context.Context, where *PostWhereUniqueInput) (*Post, error) {
	if where == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("post:unique", where)
		var cachedPost *Post
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedPost); found {
			return cachedPost, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, "apiDataBrief", "apiData", content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo", "relatedsThree" FROM "Post" p WHERE `)
	args := []interface{}{}
	argIdx := 1
	if where.ID != nil {
		sb.WriteString(fmt.Sprintf("id = $%d", argIdx))
		args = append(args, *where.ID)
		argIdx++
	} else if where.Slug != nil {
		sb.WriteString(fmt.Sprintf("slug = $%d", argIdx))
		args = append(args, *where.Slug)
		argIdx++
	} else {
		return nil, nil
	}
	sb.WriteString(" LIMIT 1")

	var (
		p               Post
		dbID            int
		publishedAt     sql.NullTime
		updatedAt       sql.NullTime
		heroImageID     sql.NullInt64
		heroVideoID     sql.NullInt64
		ogImageID       sql.NullInt64
		topicsID        sql.NullInt64
		relatedsOneID   sql.NullInt64
		relatedsTwoID   sql.NullInt64
		relatedsThreeID sql.NullInt64
		briefRaw        []byte
		apiDataBrief    []byte
		apiData         []byte
		contentRaw      []byte
	)

	err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(
		&dbID,
		&p.Slug,
		&p.Title,
		&p.Subtitle,
		&p.State,
		&p.Style,
		&p.IsMember,
		&p.IsAdult,
		&publishedAt,
		&updatedAt,
		&p.HeroCaption,
		&p.ExtendByline,
		&heroImageID,
		&heroVideoID,
		&briefRaw,
		&apiDataBrief,
		&apiData,
		&contentRaw,
		&p.Redirect,
		&p.OgTitle,
		&p.OgDescription,
		&p.HiddenAdvertised,
		&p.IsAdvertised,
		&p.IsFeatured,
		&topicsID,
		&ogImageID,
		&relatedsOneID,
		&relatedsTwoID,
		&relatedsThreeID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.ID = strconv.Itoa(dbID)
	if publishedAt.Valid {
		p.PublishedDate = publishedAt.Time.UTC().Format(timeLayoutMilli)
	}
	if updatedAt.Valid {
		p.UpdatedAt = updatedAt.Time.UTC().Format(timeLayoutMilli)
	}
	p.Brief = decodeJSONBytes(briefRaw)
	p.ApiDataBrief = decodeJSONBytesAny(apiDataBrief)
	p.ApiData = decodeJSONBytesAny(apiData)
	p.Content = decodeJSONBytes(contentRaw)
	p.TrimmedContent = p.Content
	p.Metadata = map[string]any{
		"heroImageID":     nullableInt(heroImageID),
		"ogImageID":       nullableInt(ogImageID),
		"heroVideoID":     nullableInt(heroVideoID),
		"topicsID":        nullableInt(topicsID),
		"relatedsOneID":   nullableInt(relatedsOneID),
		"relatedsTwoID":   nullableInt(relatedsTwoID),
		"relatedsThreeID": nullableInt(relatedsThreeID),
	}
	posts := []Post{p}
	if err := r.enrichPosts(ctx, posts); err != nil {
		return nil, err
	}
	p = posts[0]

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("post:unique", where)
		_ = r.cache.Set(ctx, cacheKey, &p)
	}

	return &p, nil
}

func (r *Repo) QueryExternals(ctx context.Context, where *ExternalWhereInput, orders []OrderRule, take, skip int) ([]External, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensureExternalPublished(where)

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("externals", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		var cachedExternals []External
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedExternals); found {
			return cachedExternals, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT e.id, e.slug, e.title, e.state, e."publishedDate", e."extend_byline", e.thumb, e."thumbCaption", e.brief, e.content, e.partner, e."updatedAt" FROM "External" e`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	orderUsesPublished := len(orders) == 0 || (len(orders) > 0 && orders[0].Field == "publishedDate")
	if orderUsesPublished {
		conds = append(conds, `e."publishedDate" IS NOT NULL`)
	}

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}
	if where != nil {
		buildStringFilter("e.slug", where.Slug)
		buildStringFilter("e.state", where.State)
		if where.PublishedDate != nil {
			if where.PublishedDate.Equals != nil {
				conds = append(conds, fmt.Sprintf(`e."publishedDate" = $%d`, argIdx))
				args = append(args, *where.PublishedDate.Equals)
				argIdx++
			}
			if where.PublishedDate.Not != nil {
				if where.PublishedDate.Not.Equals == nil {
					conds = append(conds, `e."publishedDate" IS NOT NULL`)
				} else {
					conds = append(conds, fmt.Sprintf(`e."publishedDate" <> $%d`, argIdx))
					args = append(args, *where.PublishedDate.Not.Equals)
					argIdx++
				}
			}
		}
		if where.Partner != nil && where.Partner.Slug != nil && where.Partner.Slug.Equals != nil {
			sb.WriteString(` JOIN "Partner" p ON p.id = e.partner`)
			conds = append(conds, fmt.Sprintf(`p.slug = $%d`, argIdx))
			args = append(args, *where.Partner.Slug.Equals)
			argIdx++
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(buildExternalOrder(orders[0]))
	} else {
		sb.WriteString(` ORDER BY e."publishedDate" DESC`)
	}
	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []External{}
	partnerIDs := []int{}
	externalIDs := []int{}
	for rows.Next() {
		var ext External
		var partnerID sql.NullInt64
		var dbID int
		var pubAt, updAt sql.NullTime
		if err := rows.Scan(&dbID, &ext.Slug, &ext.Title, &ext.State, &pubAt, &ext.ExtendByline, &ext.Thumb, &ext.ThumbCaption, &ext.Brief, &ext.Content, &partnerID, &updAt); err != nil {
			return nil, err
		}
		ext.ID = strconv.Itoa(dbID)
		if pubAt.Valid {
			ext.PublishedDate = pubAt.Time.UTC().Format(timeLayoutMilli)
		}
		if updAt.Valid {
			ext.UpdatedAt = updAt.Time.UTC().Format(timeLayoutMilli)
		}
		externalIDs = append(externalIDs, dbID)
		if partnerID.Valid {
			ext.Metadata = map[string]any{"partnerID": int(partnerID.Int64)}
			partnerIDs = append(partnerIDs, int(partnerID.Int64))
		}
		result = append(result, ext)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	partners, _ := r.fetchPartners(ctx, partnerIDs)
	tagsMap, _ := r.fetchExternalTags(ctx, "_External_tags", externalIDs)
	sectionsMap, _ := r.fetchExternalSections(ctx, externalIDs)
	categoriesMap, err := r.fetchExternalCategories(ctx, externalIDs)
	if err != nil {
		// 查詢失敗時記錄錯誤，但繼續處理
		_ = err
	}
	relatedsMap, _, _ := r.fetchExternalRelateds(ctx, externalIDs)
	for i := range result {
		if pid := getMetaInt(result[i].Metadata, "partnerID"); pid > 0 {
			result[i].Partner = partners[pid]
		}
		idInt, _ := strconv.Atoi(result[i].ID)
		result[i].Tags = tagsMap[idInt]
		if sections, ok := sectionsMap[idInt]; ok {
			result[i].Sections = sections
		} else {
			result[i].Sections = []Section{}
		}
		if categories, ok := categoriesMap[idInt]; ok {
			result[i].Categories = categories
		} else {
			result[i].Categories = []Category{}
		}
		if relateds, ok := relatedsMap[idInt]; ok {
			result[i].Relateds = relateds
		} else {
			result[i].Relateds = []Post{}
		}
	}

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("externals", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		_ = r.cache.Set(ctx, cacheKey, result)
	}

	return result, nil
}

func (r *Repo) QueryExternalsCount(ctx context.Context, where *ExternalWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	where = ensureExternalPublished(where)
	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "External" e`)
	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}
	if where != nil {
		buildStringFilter("e.slug", where.Slug)
		buildStringFilter("e.state", where.State)
		if where.Partner != nil && where.Partner.Slug != nil && where.Partner.Slug.Equals != nil {
			sb.WriteString(` JOIN "Partner" p ON p.id = e.partner`)
			conds = append(conds, fmt.Sprintf(`p.slug = $%d`, argIdx))
			args = append(args, *where.Partner.Slug.Equals)
			argIdx++
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	var count int
	if err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// QueryExternalByID 依照 ID 取得單一 External，對應前端 external(where: { id: $id }) 查詢。
// Lilith 的 GraphQL schema 對 External.id 使用 Int 型別的 filter，
// 而在 probe 時我們會用 GraphQL ID 變數呼叫自己與 target。
// 為了同時支援：
//   - 前端以字串形式傳入的 ID（例如 "1378586"）
//   - GraphQL 解析變數時產生的科學記號字串（例如 "1.378586e+06"）
//
// 這裡統一將傳入的 id 轉成整數後再帶入 SQL。
func (r *Repo) QueryExternalByID(ctx context.Context, id string) (*External, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if id == "" {
		return nil, nil
	}

	// 將字串 ID 轉為整數，容忍科學記號格式
	var idInt int
	if i, err := strconv.Atoi(id); err == nil {
		idInt = i
	} else {
		if f, err2 := strconv.ParseFloat(id, 64); err2 == nil {
			idInt = int(f)
		} else {
			return nil, fmt.Errorf("invalid external id %q: %w", id, err)
		}
	}

	query := `SELECT e.id, e.slug, e.title, e.state, e."publishedDate", e."extend_byline", e.thumb, e."thumbCaption", e.brief, e.content, e.partner, e."updatedAt" FROM "External" e WHERE e.id = $1 LIMIT 1`

	var (
		ext       External
		dbID      int
		pubAt     sql.NullTime
		updAt     sql.NullTime
		partnerID sql.NullInt64
	)

	if err := r.db.QueryRowContext(ctx, query, idInt).Scan(
		&dbID,
		&ext.Slug,
		&ext.Title,
		&ext.State,
		&pubAt,
		&ext.ExtendByline,
		&ext.Thumb,
		&ext.ThumbCaption,
		&ext.Brief,
		&ext.Content,
		&partnerID,
		&updAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	ext.ID = strconv.Itoa(dbID)
	if pubAt.Valid {
		ext.PublishedDate = pubAt.Time.UTC().Format(timeLayoutMilli)
	}
	if updAt.Valid {
		ext.UpdatedAt = updAt.Time.UTC().Format(timeLayoutMilli)
	}
	if partnerID.Valid {
		ext.Metadata = map[string]any{"partnerID": int(partnerID.Int64)}
	}

	// 補上 partner 與 tags（與 QueryExternals 的行為一致）
	if pid := getMetaInt(ext.Metadata, "partnerID"); pid > 0 {
		partners, err := r.fetchPartners(ctx, []int{pid})
		if err == nil {
			if p, ok := partners[pid]; ok {
				ext.Partner = p
			}
		}
	}
	tagsMap, _ := r.fetchExternalTags(ctx, "_External_tags", []int{dbID})
	ext.Tags = tagsMap[dbID]

	// 補上 sections, categories, relateds
	sectionsMap, _ := r.fetchExternalSections(ctx, []int{dbID})
	if sections, ok := sectionsMap[dbID]; ok {
		ext.Sections = sections
	} else {
		ext.Sections = []Section{}
	}
	categoriesMap, err := r.fetchExternalCategories(ctx, []int{dbID})
	if err != nil {
		// 查詢失敗時記錄錯誤，但繼續處理
		_ = err
	}
	if categories, ok := categoriesMap[dbID]; ok {
		ext.Categories = categories
	} else {
		ext.Categories = []Category{}
	}
	relatedsMap, _, _ := r.fetchExternalRelateds(ctx, []int{dbID})
	if relateds, ok := relatedsMap[dbID]; ok {
		ext.Relateds = relateds
	} else {
		ext.Relateds = []Post{}
	}

	return &ext, nil
}

// Internal helpers
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

func ensurePostPublished(where *PostWhereInput) *PostWhereInput {
	if where == nil {
		where = &PostWhereInput{}
	}
	if where.State == nil {
		where.State = &StringFilter{Equals: ptrString("published")}
	}
	return where
}

func ensureExternalPublished(where *ExternalWhereInput) *ExternalWhereInput {
	if where == nil {
		where = &ExternalWhereInput{}
	}
	if where.State == nil {
		where.State = &StringFilter{Equals: ptrString("published")}
	}
	return where
}

func ptrString(s string) *string { return &s }

func decodeJSONBytes(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func decodeJSONBytesAny(raw []byte) interface{} {
	if len(raw) == 0 {
		// 當資料為空時，返回空陣列而不是 nil，以匹配 target 的行為
		return []interface{}{}
	}
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		// 解析失敗時也返回空陣列，以匹配 target 的行為
		return []interface{}{}
	}
	// 如果解析結果是 nil，返回空陣列
	if v == nil {
		return []interface{}{}
	}
	return v
}

func nullableInt(v sql.NullInt64) int {
	if v.Valid {
		return int(v.Int64)
	}
	return 0
}

func getMetaInt(m map[string]any, key string) int {
	if m == nil {
		return 0
	}
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		}
	}
	return 0
}

func buildOrderClause(rule OrderRule) string {
	dir := strings.ToUpper(rule.Direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}
	switch rule.Field {
	case "publishedDate":
		return fmt.Sprintf(`"publishedDate" %s`, dir)
	case "updatedAt":
		return fmt.Sprintf(`"updatedAt" %s`, dir)
	case "title":
		return fmt.Sprintf(`"title" %s`, dir)
	default:
		return `"publishedDate" DESC`
	}
}

func buildExternalOrder(rule OrderRule) string {
	dir := strings.ToUpper(rule.Direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}
	switch rule.Field {
	case "publishedDate":
		return fmt.Sprintf(`e."publishedDate" %s`, dir)
	case "updatedAt":
		return fmt.Sprintf(`e."updatedAt" %s`, dir)
	default:
		return `e."publishedDate" DESC`
	}
}

func (r *Repo) enrichPosts(ctx context.Context, posts []Post) error {
	if len(posts) == 0 {
		return nil
	}
	postIDs := make([]int, 0, len(posts))
	for _, p := range posts {
		id, _ := strconv.Atoi(p.ID)
		if id == 0 {
			continue
		}
		postIDs = append(postIDs, id)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	sectionsMap, err := r.fetchSections(ctx, postIDs)
	if err != nil {
		return err
	}
	categoriesMap, err := r.fetchCategories(ctx, postIDs)
	if err != nil {
		return err
	}
	roleMapWriters, _ := r.fetchContacts(ctx, "_Post_writers", postIDs)
	roleMapPhotographers, _ := r.fetchContacts(ctx, "_Post_photographers", postIDs)
	roleMapCamera, _ := r.fetchContacts(ctx, "_Post_camera_man", postIDs)
	roleMapDesigners, _ := r.fetchContacts(ctx, "_Post_designers", postIDs)
	roleMapEngineers, _ := r.fetchContacts(ctx, "_Post_engineers", postIDs)
	roleMapVocals, _ := r.fetchContacts(ctx, "_Post_vocals", postIDs)

	tagsMap, _ := r.fetchTags(ctx, "_Post_tags", postIDs)
	tagsAlgoMap, _ := r.fetchTags(ctx, "_Post_tags_algo", postIDs)
	warningsMap, err := r.fetchPostWarnings(ctx, postIDs)
	if err != nil {
		// 如果查詢失敗，記錄錯誤但繼續處理（可能是表不存在或其他問題）
		// 在開發環境中可以考慮記錄日誌
		_ = err
	}

	relatedsMap, relatedImageIDs, err := r.fetchRelatedPosts(ctx, postIDs)
	if err != nil {
		return err
	}
	imageIDs := append([]int{}, relatedImageIDs...)

	relatedOneIDs := []int{}
	relatedTwoIDs := []int{}
	relatedThreeIDs := []int{}
	for _, p := range posts {
		if id := getMetaInt(p.Metadata, "relatedsOneID"); id > 0 {
			relatedOneIDs = append(relatedOneIDs, id)
		}
		if id := getMetaInt(p.Metadata, "relatedsTwoID"); id > 0 {
			relatedTwoIDs = append(relatedTwoIDs, id)
		}
		if id := getMetaInt(p.Metadata, "relatedsThreeID"); id > 0 {
			relatedThreeIDs = append(relatedThreeIDs, id)
		}
	}
	relatedSinglesIDs := append(append(relatedOneIDs, relatedTwoIDs...), relatedThreeIDs...)
	relatedSinglePosts := map[int]Post{}
	if len(relatedSinglesIDs) > 0 {
		sps, imgIDs, err := r.fetchPostsByIDs(ctx, relatedSinglesIDs)
		if err != nil {
			return err
		}
		for _, sp := range sps {
			id, _ := strconv.Atoi(sp.ID)
			relatedSinglePosts[id] = sp
		}
		imageIDs = append(imageIDs, imgIDs...)
	}

	videoIDs := []int{}
	topicIDs := []int{}
	for _, p := range posts {
		if id := getMetaInt(p.Metadata, "heroVideoID"); id > 0 {
			videoIDs = append(videoIDs, id)
		}
		if id := getMetaInt(p.Metadata, "topicsID"); id > 0 {
			topicIDs = append(topicIDs, id)
		}
		if id := getMetaInt(p.Metadata, "heroImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
		if id := getMetaInt(p.Metadata, "ogImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
	}

	videoMap, videoImageIDs, _ := r.fetchVideos(ctx, videoIDs)
	imageIDs = append(imageIDs, videoImageIDs...)
	topicMap, _ := r.fetchTopics(ctx, topicIDs)
	imageMap, err := r.fetchImages(ctx, imageIDs)
	if err != nil {
		return err
	}

	for i := range posts {
		p := &posts[i]
		id, _ := strconv.Atoi(p.ID)
		p.Sections = sectionsMap[id]
		p.SectionsInInputOrder = sectionsMap[id]
		p.Categories = categoriesMap[id]
		p.CategoriesInInputOrder = categoriesMap[id]
		p.Writers = roleMapWriters[id]
		p.WritersInInputOrder = roleMapWriters[id]
		p.Photographers = roleMapPhotographers[id]
		p.CameraMan = roleMapCamera[id]
		p.Designers = roleMapDesigners[id]
		p.Engineers = roleMapEngineers[id]
		p.Vocals = roleMapVocals[id]
		p.Tags = tagsMap[id]
		p.TagsAlgo = tagsAlgoMap[id]
		p.Warnings = warningsMap[id]
		// Warning 是 Post 表的獨立欄位（單數），不應從 Warnings 陣列自動取得
		// 如果 Post 表有 Warning 欄位，應該在 SELECT 語句中讀取並填充
		// 目前 Post 表沒有 Warning 欄位，所以保持為 nil
		p.Relateds = relatedsMap[id]
		p.RelatedsInInputOrder = relatedsMap[id]
		if idImg := getMetaInt(p.Metadata, "heroImageID"); idImg > 0 {
			p.HeroImage = imageMap[idImg]
		}
		if idImg := getMetaInt(p.Metadata, "ogImageID"); idImg > 0 {
			p.OgImage = imageMap[idImg]
		}
		if vid := getMetaInt(p.Metadata, "heroVideoID"); vid > 0 {
			p.HeroVideo = videoMap[vid]
		}
		if tid := getMetaInt(p.Metadata, "topicsID"); tid > 0 {
			if t, ok := topicMap[tid]; ok {
				p.Topics = &t
			}
		}
		if r1 := getMetaInt(p.Metadata, "relatedsOneID"); r1 > 0 {
			if rp, ok := relatedSinglePosts[r1]; ok {
				p.RelatedsOne = &rp
			}
		}
		if r2 := getMetaInt(p.Metadata, "relatedsTwoID"); r2 > 0 {
			if rp, ok := relatedSinglePosts[r2]; ok {
				p.RelatedsTwo = &rp
			}
		}
		if r3 := getMetaInt(p.Metadata, "relatedsThreeID"); r3 > 0 {
			if rp, ok := relatedSinglePosts[r3]; ok {
				p.RelatedsThree = &rp
			}
		}
	}
	return nil
}

func (r *Repo) fetchSections(ctx context.Context, postIDs []int) (map[int][]Section, error) {
	result := map[int][]Section{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := `SELECT ps."A" as post_id, s.id, s.name, s.slug, s.state, COALESCE(s.color, '') as color FROM "_Post_sections" ps JOIN "Section" s ON s.id = ps."B" WHERE ps."A" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var s Section
		if err := rows.Scan(&pid, &s.ID, &s.Name, &s.Slug, &s.State, &s.Color); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], s)
	}
	return result, rows.Err()
}

func (r *Repo) fetchCategories(ctx context.Context, postIDs []int) (map[int][]Category, error) {
	result := map[int][]Category{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := `SELECT cp."B" as post_id, c.id, c.name, c.slug, c.state FROM "_Category_posts" cp JOIN "Category" c ON c.id = cp."A" WHERE cp."B" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var c Category
		if err := rows.Scan(&pid, &c.ID, &c.Name, &c.Slug, &c.State); err != nil {
			return result, err
		}
		// isMemberOnly 欄位在資料庫中不存在，設為預設值 false
		c.IsMemberOnly = false
		result[pid] = append(result[pid], c)
	}
	return result, rows.Err()
}

func (r *Repo) fetchContacts(ctx context.Context, table string, postIDs []int) (map[int][]Contact, error) {
	result := map[int][]Contact{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := fmt.Sprintf(`SELECT t."B" as post_id, c.id, c.name FROM "%s" t JOIN "Contact" c ON c.id = t."A" WHERE t."B" = ANY($1)`, table)
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var c Contact
		if err := rows.Scan(&pid, &c.ID, &c.Name); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], c)
	}
	return result, rows.Err()
}

func (r *Repo) fetchTags(ctx context.Context, table string, postIDs []int) (map[int][]Tag, error) {
	result := map[int][]Tag{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := fmt.Sprintf(`SELECT t."A" as post_id, tg.id, tg.name, tg.slug FROM "%s" t JOIN "Tag" tg ON tg.id = t."B" WHERE t."A" = ANY($1)`, table)
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var t Tag
		if err := rows.Scan(&pid, &t.ID, &t.Name, &t.Slug); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], t)
	}
	return result, rows.Err()
}

func (r *Repo) fetchPostWarnings(ctx context.Context, postIDs []int) (map[int][]Warning, error) {
	result := map[int][]Warning{}
	if len(postIDs) == 0 {
		return result, nil
	}
	// Warnings 應該直接從 Post 本身的 _Post_Warnings 表取得
	// 根據實際表名，表名是 _Post_Warnings（大寫 W），A 是 Post ID，B 是 Warning ID
	query := `
		SELECT pw."A" as post_id, w.id, w.content
		FROM "_Post_Warnings" pw
		JOIN "Warning" w ON w.id = pw."B"
		WHERE pw."A" = ANY($1)
		ORDER BY pw."A", w.id
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		// 如果查詢失敗，返回空結果（不返回錯誤，因為可能是表不存在）
		return result, nil
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var w Warning
		var warningID int
		if err := rows.Scan(&pid, &warningID, &w.Content); err != nil {
			return result, err
		}
		w.ID = strconv.Itoa(warningID)
		result[pid] = append(result[pid], w)
	}
	return result, rows.Err()
}

func (r *Repo) fetchRelatedPosts(ctx context.Context, postIDs []int) (map[int][]Post, []int, error) {
	result := map[int][]Post{}
	imageIDs := []int{}
	if len(postIDs) == 0 {
		return result, imageIDs, nil
	}
	query := `
		SELECT r."A" as post_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."B"
		WHERE r."A" = ANY($1)
		UNION
		SELECT r."B" as post_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."A"
		WHERE r."B" = ANY($1)
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var rp Post
		var dbID int
		var heroID sql.NullInt64
		if err := rows.Scan(&pid, &dbID, &rp.Slug, &rp.Title, &heroID); err != nil {
			return result, imageIDs, err
		}
		rp.ID = strconv.Itoa(dbID)
		if heroID.Valid {
			imageIDs = append(imageIDs, int(heroID.Int64))
			rp.Metadata = map[string]any{"heroImageID": int(heroID.Int64)}
		}
		result[pid] = append(result[pid], rp)
	}
	return result, imageIDs, rows.Err()
}

func (r *Repo) fetchPostsByIDs(ctx context.Context, ids []int) ([]Post, []int, error) {
	result := []Post{}
	imageIDs := []int{}
	if len(ids) == 0 {
		return result, imageIDs, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug, title, "heroImage" FROM "Post" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var p Post
		var dbID int
		var hero sql.NullInt64
		if err := rows.Scan(&dbID, &p.Slug, &p.Title, &hero); err != nil {
			return result, imageIDs, err
		}
		p.ID = strconv.Itoa(dbID)
		if hero.Valid {
			imageIDs = append(imageIDs, int(hero.Int64))
			p.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
		}
		result = append(result, p)
	}
	return result, imageIDs, rows.Err()
}

func (r *Repo) fetchVideos(ctx context.Context, videoIDs []int) (map[int]*Video, []int, error) {
	result := map[int]*Video{}
	imageIDs := []int{}
	if len(videoIDs) == 0 {
		return result, imageIDs, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, "urlOriginal", "heroImage" FROM "Video" WHERE id = ANY($1)`, pqIntArray(videoIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var v Video
		var dbID int
		var hero sql.NullInt64
		if err := rows.Scan(&dbID, &v.VideoSrc, &hero); err != nil {
			return result, imageIDs, err
		}
		v.ID = strconv.Itoa(dbID)
		if hero.Valid {
			imageIDs = append(imageIDs, int(hero.Int64))
			v.HeroImage = &Photo{}
			v.HeroImage.ImageFile = ImageFile{}
			v.HeroImage.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
		}
		result[dbID] = &v
	}
	return result, imageIDs, rows.Err()
}

func (r *Repo) fetchTopics(ctx context.Context, ids []int) (map[int]Topic, error) {
	result := map[int]Topic{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug FROM "Topic" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var t Topic
		if err := rows.Scan(&id, &t.Slug); err != nil {
			return result, err
		}
		result[id] = t
	}
	return result, rows.Err()
}

func (r *Repo) fetchImages(ctx context.Context, ids []int) (map[int]*Photo, error) {
	result := map[int]*Photo{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, COALESCE(name, '') as name, COALESCE("topicKeywords", '') as topicKeywords, COALESCE("imageFile_id", ''), COALESCE("imageFile_extension", ''), "imageFile_width", "imageFile_height" FROM "Image" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var im struct {
			id            int
			name          string
			topicKeywords string
			fileID        string
			ext           string
			width         sql.NullInt64
			height        sql.NullInt64
		}
		if err := rows.Scan(&im.id, &im.name, &im.topicKeywords, &im.fileID, &im.ext, &im.width, &im.height); err != nil {
			return result, err
		}
		photo := Photo{
			ID:            strconv.Itoa(im.id),
			Name:          im.name,
			TopicKeywords: im.topicKeywords,
			ImageFile: ImageFile{
				Width:  int(im.width.Int64),
				Height: int(im.height.Int64),
			},
		}
		photo.Resized = r.buildResizedURLs(im.fileID, im.ext)
		photo.ResizedWebp = r.buildResizedURLs(im.fileID, "webP")
		result[im.id] = &photo
	}
	return result, rows.Err()
}

func (r *Repo) fetchPartners(ctx context.Context, ids []int) (map[int]*Partner, error) {
	result := map[int]*Partner{}
	if len(ids) == 0 {
		return result, nil
	}
	// 根據 schema.prisma，Partner 只有 id, slug, name, showOnIndex 欄位
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug, name, "showOnIndex" FROM "Partner" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var p Partner
		var dbID int
		if err := rows.Scan(&dbID, &p.Slug, &p.Name, &p.ShowOnIndex); err != nil {
			return result, err
		}
		p.ID = strconv.Itoa(dbID)
		result[dbID] = &p
	}
	return result, rows.Err()
}

// QueryPartnerByID 查詢單一 Partner by ID（用於預設值邏輯）
func (r *Repo) QueryPartnerByID(ctx context.Context, id string) (*Partner, error) {
	idInt, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	partners, err := r.fetchPartners(ctx, []int{idInt})
	if err != nil {
		return nil, err
	}
	if p, ok := partners[idInt]; ok {
		return p, nil
	}
	return nil, nil
}

func (r *Repo) fetchExternalSections(ctx context.Context, externalIDs []int) (map[int][]Section, error) {
	result := map[int][]Section{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	query := `SELECT es."A" as external_id, s.id, s.name, s.slug, s.state, COALESCE(s.color, '') as color FROM "_External_sections" es JOIN "Section" s ON s.id = es."B" WHERE es."A" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var s Section
		if err := rows.Scan(&eid, &s.ID, &s.Name, &s.Slug, &s.State, &s.Color); err != nil {
			return result, err
		}
		result[eid] = append(result[eid], s)
	}
	return result, rows.Err()
}

func (r *Repo) fetchExternalCategories(ctx context.Context, externalIDs []int) (map[int][]Category, error) {
	result := map[int][]Category{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	// 根據實際表名，External 的 categories 是直接關聯 _Category_externals 表
	// 其中 A 是 Category ID，B 是 External ID
	query := `
		SELECT DISTINCT ce."B" as external_id, c.id, c.name, c.slug, c.state
		FROM "_Category_externals" ce
		JOIN "Category" c ON c.id = ce."A"
		WHERE ce."B" = ANY($1)
		ORDER BY ce."B", c.id
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		// 如果查詢失敗，返回空結果
		return result, nil
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var c Category
		if err := rows.Scan(&eid, &c.ID, &c.Name, &c.Slug, &c.State); err != nil {
			return result, err
		}
		// isMemberOnly 欄位在資料庫中不存在，設為預設值 false
		c.IsMemberOnly = false
		result[eid] = append(result[eid], c)
	}
	return result, rows.Err()
}

func (r *Repo) fetchExternalRelateds(ctx context.Context, externalIDs []int) (map[int][]Post, []int, error) {
	result := map[int][]Post{}
	imageIDs := []int{}
	if len(externalIDs) == 0 {
		return result, imageIDs, nil
	}
	query := `
		SELECT er."A" as external_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_External_relateds" er
		JOIN "Post" p ON p.id = er."B"
		WHERE er."A" = ANY($1)
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var rp Post
		var dbID int
		var heroID sql.NullInt64
		if err := rows.Scan(&eid, &dbID, &rp.Slug, &rp.Title, &heroID); err != nil {
			return result, imageIDs, err
		}
		rp.ID = strconv.Itoa(dbID)
		if heroID.Valid {
			imageIDs = append(imageIDs, int(heroID.Int64))
			rp.Metadata = map[string]any{"heroImageID": int(heroID.Int64)}
		}
		result[eid] = append(result[eid], rp)
	}
	return result, imageIDs, rows.Err()
}

func (r *Repo) fetchExternalTags(ctx context.Context, table string, externalIDs []int) (map[int][]Tag, error) {
	result := map[int][]Tag{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`SELECT t."A" as external_id, tg.id, tg.name, tg.slug FROM "%s" t JOIN "Tag" tg ON tg.id = t."B" WHERE t."A" = ANY($1)`, table), pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var tg Tag
		if err := rows.Scan(&eid, &tg.ID, &tg.Name, &tg.Slug); err != nil {
			return result, err
		}
		result[eid] = append(result[eid], tg)
	}
	return result, rows.Err()
}

func pqIntArray(ids []int) interface{} {
	arr := make([]int64, len(ids))
	for i, id := range ids {
		arr[i] = int64(id)
	}
	return arr
}

func (r *Repo) buildResizedURLs(fileID, ext string) Resized {
	if fileID == "" {
		return Resized{}
	}
	if ext == "" {
		ext = "jpg"
	}
	host := strings.TrimSuffix(r.staticsHost, "/")
	makeURL := func(size string, extension string) string {
		// staticsHost 已經包含 images 路徑，不需要再加 images/ 前綴
		// 如果 target 的 w1200 是空字串，表示可能不需要生成該尺寸的 URL
		// 但我們還是生成，以保持一致性
		filename := fileID
		if size != "" {
			filename = fmt.Sprintf("%s-%s", fileID, size)
		}
		return fmt.Sprintf("%s/%s.%s", host, filename, extension)
	}
	return Resized{
		Original: makeURL("", ext),
		W480:     makeURL("w480", ext),
		W800:     makeURL("w800", ext),
		W1200:    makeURL("w1200", ext),
		W1600:    makeURL("w1600", ext),
		W2400:    makeURL("w2400", ext),
	}
}

// ensureTopicPublished 確保查詢只返回 published 的 topics
func ensureTopicPublished(where *TopicWhereInput) *TopicWhereInput {
	if where == nil {
		where = &TopicWhereInput{}
	}
	if where.State == nil {
		where.State = &StringFilter{Equals: stringPtr("published")}
	} else if where.State.Equals == nil {
		where.State.Equals = stringPtr("published")
	}
	return where
}

// ensureVideoPublished 確保查詢只返回 published 的 videos
func ensureVideoPublished(where *VideoWhereInput) *VideoWhereInput {
	if where == nil {
		where = &VideoWhereInput{}
	}
	if where.State == nil {
		where.State = &StringFilter{Equals: stringPtr("published")}
	} else if where.State.Equals == nil {
		where.State.Equals = stringPtr("published")
	}
	return where
}

func stringPtr(s string) *string {
	return &s
}

// QueryTopics 查詢 topics
func (r *Repo) QueryTopics(ctx context.Context, where *TopicWhereInput, orders []OrderRule, take, skip int) ([]Topic, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensureTopicPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, name, slug, "sortOrder", state, "publishedDate", brief, "apiDataBrief", "leading", "heroImage", "heroUrl", "heroVideo", COALESCE(og_title, '') as og_title, COALESCE(og_description, '') as og_description, "og_image", COALESCE(type, 'list') as type, COALESCE(style, '') as style, "isFeatured", COALESCE("title_style", 'feature') as title_style, COALESCE(javascript, '') as javascript, COALESCE(dfp, '') as dfp, COALESCE("mobile_dfp", '') as mobile_dfp, "createdAt" FROM "Topic" t`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("t.state", where.State)
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		orderParts := []string{}
		for _, o := range orders {
			dir := "ASC"
			if o.Direction == "desc" {
				dir = "DESC"
			}
			switch o.Field {
			case "sortOrder":
				orderParts = append(orderParts, fmt.Sprintf(`t."sortOrder" %s NULLS LAST`, dir))
			case "id":
				orderParts = append(orderParts, fmt.Sprintf(`t.id %s`, dir))
			case "createdAt":
				orderParts = append(orderParts, fmt.Sprintf(`t."createdAt" %s`, dir))
			case "publishedDate":
				orderParts = append(orderParts, fmt.Sprintf(`t."publishedDate" %s`, dir))
			}
		}
		if len(orderParts) > 0 {
			sb.WriteString(strings.Join(orderParts, ", "))
		}
	} else {
		sb.WriteString(` ORDER BY t."sortOrder" ASC NULLS LAST, t.id DESC`)
	}

	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []Topic{}
	topicIDs := []int{}
	heroImageIDs := []int{}
	heroVideoIDs := []int{}
	ogImageIDs := []int{}
	for rows.Next() {
		var t Topic
		var dbID int
		var sortOrder sql.NullInt64
		var pubAt, createdAt sql.NullTime
		var brief, apiDataBrief sql.NullString
		var heroImageID, heroVideoID, ogImageID sql.NullInt64
		var heroUrl sql.NullString
		var leading sql.NullString
		if err := rows.Scan(&dbID, &t.Name, &t.Slug, &sortOrder, &t.State, &pubAt, &brief, &apiDataBrief, &leading, &heroImageID, &heroUrl, &heroVideoID, &t.OgTitle, &t.OgDescription, &ogImageID, &t.Type, &t.Style, &t.IsFeatured, &t.TitleStyle, &t.Javascript, &t.Dfp, &t.MobileDfp, &createdAt); err != nil {
			return nil, err
		}
		if leading.Valid {
			t.Leading = leading.String
		} else {
			t.Leading = ""
		}
		if heroUrl.Valid {
			t.HeroUrl = heroUrl.String
		} else {
			t.HeroUrl = ""
		}
		t.ID = strconv.Itoa(dbID)
		if sortOrder.Valid {
			val := int(sortOrder.Int64)
			t.SortOrder = &val
		}
		if pubAt.Valid {
			t.PublishedDate = pubAt.Time.Format(timeLayoutMilli)
		}
		if createdAt.Valid {
			t.CreatedAt = createdAt.Time.Format(timeLayoutMilli)
		}
		if brief.Valid && brief.String != "" {
			if err := json.Unmarshal([]byte(brief.String), &t.Brief); err != nil {
				t.Brief = map[string]any{}
			}
		}
		if apiDataBrief.Valid && apiDataBrief.String != "" {
			t.ApiDataBrief = decodeJSONBytesAny([]byte(apiDataBrief.String))
		}
		t.Metadata = map[string]any{}
		if heroImageID.Valid {
			heroImageIDs = append(heroImageIDs, int(heroImageID.Int64))
			t.Metadata["heroImageID"] = int(heroImageID.Int64)
		}
		if heroVideoID.Valid {
			heroVideoIDs = append(heroVideoIDs, int(heroVideoID.Int64))
			t.Metadata["heroVideoID"] = int(heroVideoID.Int64)
		}
		if ogImageID.Valid {
			ogImageIDs = append(ogImageIDs, int(ogImageID.Int64))
			t.Metadata["ogImageID"] = int(ogImageID.Int64)
		}
		result = append(result, t)
		topicIDs = append(topicIDs, dbID)
	}

	// Enrich topics
	if err := r.enrichTopics(ctx, &result, topicIDs, heroImageIDs, heroVideoIDs, ogImageIDs); err != nil {
		return nil, err
	}

	return result, rows.Err()
}

// QueryTopicsCount 查詢 topics 數量
func (r *Repo) QueryTopicsCount(ctx context.Context, where *TopicWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensureTopicPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "Topic" t`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("t.state", where.State)
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	var count int
	err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(&count)
	return count, err
}

// QueryTopicByUnique 根據 unique input 查詢單一 topic
func (r *Repo) QueryTopicByUnique(ctx context.Context, where *TopicWhereUniqueInput) (*Topic, error) {
	if where == nil {
		return nil, nil
	}
	if where.Slug != nil {
		return r.QueryTopicBySlug(ctx, *where.Slug)
	}
	if where.ID != nil {
		return r.QueryTopicByID(ctx, *where.ID)
	}
	if where.Name != nil {
		// 根據 name 查詢（如果需要的話）
		return nil, fmt.Errorf("query by name not implemented")
	}
	return nil, nil
}

// QueryTopicBySlug 根據 slug 查詢單一 topic
func (r *Repo) QueryTopicBySlug(ctx context.Context, slug string) (*Topic, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT id, name, slug, "sortOrder", state, "publishedDate", brief, "apiDataBrief", "leading", "heroImage", "heroUrl", "heroVideo", COALESCE(og_title, '') as og_title, COALESCE(og_description, '') as og_description, "og_image", COALESCE(type, 'list') as type, COALESCE(style, '') as style, "isFeatured", COALESCE("title_style", 'feature') as title_style, COALESCE(javascript, '') as javascript, COALESCE(dfp, '') as dfp, COALESCE("mobile_dfp", '') as mobile_dfp, "createdAt" FROM "Topic" WHERE slug = $1 AND state = 'published'`

	var t Topic
	var dbID int
	var sortOrder sql.NullInt64
	var pubAt, createdAt sql.NullTime
	var brief, apiDataBrief sql.NullString
	var heroImageID, heroVideoID, ogImageID sql.NullInt64
	var heroUrl sql.NullString
	var leading sql.NullString
	err := r.db.QueryRowContext(ctx, query, slug).Scan(&dbID, &t.Name, &t.Slug, &sortOrder, &t.State, &pubAt, &brief, &apiDataBrief, &leading, &heroImageID, &heroUrl, &heroVideoID, &t.OgTitle, &t.OgDescription, &ogImageID, &t.Type, &t.Style, &t.IsFeatured, &t.TitleStyle, &t.Javascript, &t.Dfp, &t.MobileDfp, &createdAt)
	if leading.Valid {
		t.Leading = leading.String
	} else {
		t.Leading = ""
	}
	if heroUrl.Valid {
		t.HeroUrl = heroUrl.String
	} else {
		t.HeroUrl = ""
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.ID = strconv.Itoa(dbID)
	if sortOrder.Valid {
		val := int(sortOrder.Int64)
		t.SortOrder = &val
	}
	if pubAt.Valid {
		t.PublishedDate = pubAt.Time.Format(timeLayoutMilli)
	}
	if createdAt.Valid {
		t.CreatedAt = createdAt.Time.Format(timeLayoutMilli)
	}
	if brief.Valid && brief.String != "" {
		if err := json.Unmarshal([]byte(brief.String), &t.Brief); err != nil {
			t.Brief = map[string]any{}
		}
	}
	if apiDataBrief.Valid && apiDataBrief.String != "" {
		t.ApiDataBrief = decodeJSONBytesAny([]byte(apiDataBrief.String))
	}

	t.Metadata = map[string]any{}
	heroImageIDs := []int{}
	heroVideoIDs := []int{}
	ogImageIDs := []int{}
	if heroImageID.Valid {
		heroImageIDs = append(heroImageIDs, int(heroImageID.Int64))
		t.Metadata["heroImageID"] = int(heroImageID.Int64)
	}
	if heroVideoID.Valid {
		heroVideoIDs = append(heroVideoIDs, int(heroVideoID.Int64))
		t.Metadata["heroVideoID"] = int(heroVideoID.Int64)
	}
	if ogImageID.Valid {
		ogImageIDs = append(ogImageIDs, int(ogImageID.Int64))
		t.Metadata["ogImageID"] = int(ogImageID.Int64)
	}

	topics := []Topic{t}
	if err := r.enrichTopics(ctx, &topics, []int{dbID}, heroImageIDs, heroVideoIDs, ogImageIDs); err != nil {
		return nil, err
	}

	return &topics[0], nil
}

// QueryTopicByID 根據 ID 查詢單一 topic
func (r *Repo) QueryTopicByID(ctx context.Context, id string) (*Topic, error) {
	idInt, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT id, name, slug, "sortOrder", state, "publishedDate", brief, "apiDataBrief", "leading", "heroImage", "heroUrl", "heroVideo", COALESCE(og_title, '') as og_title, COALESCE(og_description, '') as og_description, "og_image", COALESCE(type, 'list') as type, COALESCE(style, '') as style, "isFeatured", COALESCE("title_style", 'feature') as title_style, COALESCE(javascript, '') as javascript, COALESCE(dfp, '') as dfp, COALESCE("mobile_dfp", '') as mobile_dfp, "createdAt" FROM "Topic" WHERE id = $1 AND state = 'published'`

	var t Topic
	var dbID int
	var sortOrder sql.NullInt64
	var pubAt, createdAt sql.NullTime
	var brief, apiDataBrief sql.NullString
	var heroImageID, heroVideoID, ogImageID sql.NullInt64
	var heroUrl sql.NullString
	var leading sql.NullString
	err = r.db.QueryRowContext(ctx, query, idInt).Scan(&dbID, &t.Name, &t.Slug, &sortOrder, &t.State, &pubAt, &brief, &apiDataBrief, &leading, &heroImageID, &heroUrl, &heroVideoID, &t.OgTitle, &t.OgDescription, &ogImageID, &t.Type, &t.Style, &t.IsFeatured, &t.TitleStyle, &t.Javascript, &t.Dfp, &t.MobileDfp, &createdAt)
	if leading.Valid {
		t.Leading = leading.String
	} else {
		t.Leading = ""
	}
	if heroUrl.Valid {
		t.HeroUrl = heroUrl.String
	} else {
		t.HeroUrl = ""
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.ID = strconv.Itoa(dbID)
	if sortOrder.Valid {
		val := int(sortOrder.Int64)
		t.SortOrder = &val
	}
	if pubAt.Valid {
		t.PublishedDate = pubAt.Time.Format(timeLayoutMilli)
	}
	if createdAt.Valid {
		t.CreatedAt = createdAt.Time.Format(timeLayoutMilli)
	}
	if brief.Valid && brief.String != "" {
		if err := json.Unmarshal([]byte(brief.String), &t.Brief); err != nil {
			t.Brief = map[string]any{}
		}
	}
	if apiDataBrief.Valid && apiDataBrief.String != "" {
		t.ApiDataBrief = decodeJSONBytesAny([]byte(apiDataBrief.String))
	}

	t.Metadata = map[string]any{}
	heroImageIDs := []int{}
	heroVideoIDs := []int{}
	ogImageIDs := []int{}
	if heroImageID.Valid {
		heroImageIDs = append(heroImageIDs, int(heroImageID.Int64))
		t.Metadata["heroImageID"] = int(heroImageID.Int64)
	}
	if heroVideoID.Valid {
		heroVideoIDs = append(heroVideoIDs, int(heroVideoID.Int64))
		t.Metadata["heroVideoID"] = int(heroVideoID.Int64)
	}
	if ogImageID.Valid {
		ogImageIDs = append(ogImageIDs, int(ogImageID.Int64))
		t.Metadata["ogImageID"] = int(ogImageID.Int64)
	}

	topics := []Topic{t}
	if err := r.enrichTopics(ctx, &topics, []int{dbID}, heroImageIDs, heroVideoIDs, ogImageIDs); err != nil {
		return nil, err
	}

	return &topics[0], nil
}

// enrichTopics 豐富 topics 資料（heroImage, heroVideo, ogImage, slideshow_images, tags, posts, sections）
func (r *Repo) enrichTopics(ctx context.Context, topics *[]Topic, topicIDs []int, heroImageIDs []int, heroVideoIDs []int, ogImageIDs []int) error {
	if len(*topics) == 0 {
		return nil
	}

	// Fetch images
	allImageIDs := append(heroImageIDs, ogImageIDs...)
	imagesMap, err := r.fetchImages(ctx, allImageIDs)
	if err != nil {
		return err
	}

	// Fetch videos
	videosMap, _, err := r.fetchVideos(ctx, heroVideoIDs)
	if err != nil {
		return err
	}

	// Fetch slideshow images
	slideshowMap, err := r.fetchTopicSlideshowImages(ctx, topicIDs)
	if err != nil {
		return err
	}

	// Fetch tags
	tagsMap, err := r.fetchTopicTags(ctx, topicIDs)
	if err != nil {
		return err
	}

	// Fetch posts
	postsMap, err := r.fetchTopicPosts(ctx, topicIDs)
	if err != nil {
		return err
	}

	// Fetch sections
	sectionsMap, err := r.fetchTopicSections(ctx, topicIDs)
	if err != nil {
		return err
	}

	// Assign to topics
	for i := range *topics {
		t := &(*topics)[i]
		id, _ := strconv.Atoi(t.ID)

		// Hero image
		if heroImageID, ok := t.Metadata["heroImageID"].(int); ok {
			if img, ok := imagesMap[heroImageID]; ok {
				t.HeroImage = img
			}
		}

		// Hero video
		if heroVideoID, ok := t.Metadata["heroVideoID"].(int); ok {
			if vid, ok := videosMap[heroVideoID]; ok {
				t.HeroVideo = vid
			}
		}

		// OG image
		if ogImageID, ok := t.Metadata["ogImageID"].(int); ok {
			if img, ok := imagesMap[ogImageID]; ok {
				t.OgImage = img
			}
		}

		// Slideshow images
		t.SlideshowImages = slideshowMap[id]

		// Tags
		t.Tags = tagsMap[id]

		// Posts
		t.Posts = postsMap[id]

		// Sections
		t.Sections = sectionsMap[id]
	}

	return nil
}

// fetchTopicSlideshowImages 查詢 topic 的 slideshow images
func (r *Repo) fetchTopicSlideshowImages(ctx context.Context, topicIDs []int) (map[int][]Photo, error) {
	result := map[int][]Photo{}
	if len(topicIDs) == 0 {
		return result, nil
	}
	// 根據 schema.prisma，Topic.slideshow_images 是透過 _Topic_slideshow_images 表關聯
	query := `
		SELECT tsi."A" as topic_id, i.id, COALESCE(i.name, '') as name, COALESCE(i."topicKeywords", '') as topicKeywords, COALESCE(i."imageFile_id", ''), COALESCE(i."imageFile_extension", ''), i."imageFile_width", i."imageFile_height"
		FROM "_Topic_slideshow_images" tsi
		JOIN "Image" i ON i.id = tsi."B"
		WHERE tsi."A" = ANY($1)
		ORDER BY tsi."A", tsi."B"
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(topicIDs))
	if err != nil {
		return result, nil
	}
	defer rows.Close()
	imageIDs := []int{}
	imageMap := map[int]int{} // imageID -> topicID
	for rows.Next() {
		var topicID, imageID int
		var name, topicKeywords, fileID, ext string
		var width, height sql.NullInt64
		if err := rows.Scan(&topicID, &imageID, &name, &topicKeywords, &fileID, &ext, &width, &height); err != nil {
			return result, err
		}
		imageIDs = append(imageIDs, imageID)
		imageMap[imageID] = topicID
		// 直接建立 Photo 物件，因為我們已經有所有需要的資料
		photo := Photo{
			ID:            strconv.Itoa(imageID),
			Name:          name,
			TopicKeywords: topicKeywords,
			ImageFile: ImageFile{
				Width:  int(width.Int64),
				Height: int(height.Int64),
			},
		}
		photo.Resized = r.buildResizedURLs(fileID, ext)
		photo.ResizedWebp = r.buildResizedURLs(fileID, "webP")
		result[topicID] = append(result[topicID], photo)
	}
	return result, rows.Err()
}

// fetchTopicTags 查詢 topic 的 tags
func (r *Repo) fetchTopicTags(ctx context.Context, topicIDs []int) (map[int][]Tag, error) {
	result := map[int][]Tag{}
	if len(topicIDs) == 0 {
		return result, nil
	}
	// 根據 schema.prisma，Topic.tags 是透過 Tag_topics 表關聯（Tag 是 A，Topic 是 B）
	query := `
		SELECT tt."B" as topic_id, t.id, t.name, t.slug
		FROM "_Tag_topics" tt
		JOIN "Tag" t ON t.id = tt."A"
		WHERE tt."B" = ANY($1)
		ORDER BY tt."B", t.id
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(topicIDs))
	if err != nil {
		return result, nil
	}
	defer rows.Close()
	for rows.Next() {
		var topicID int
		var tag Tag
		var tagID int
		if err := rows.Scan(&topicID, &tagID, &tag.Name, &tag.Slug); err != nil {
			return result, err
		}
		tag.ID = strconv.Itoa(tagID)
		result[topicID] = append(result[topicID], tag)
	}
	return result, rows.Err()
}

// fetchTopicPosts 查詢 topic 的 posts
func (r *Repo) fetchTopicPosts(ctx context.Context, topicIDs []int) (map[int][]Post, error) {
	result := map[int][]Post{}
	if len(topicIDs) == 0 {
		return result, nil
	}
	// 根據 schema.prisma，Topic.posts 是透過 Post.topics 欄位關聯（Post.topics 是 Topic 的 foreign key）
	query := `
		SELECT p.topics as topic_id, p.id, p.slug, p.title, p."heroImage"
		FROM "Post" p
		WHERE p.topics = ANY($1) AND p.state = 'published'
		ORDER BY p.topics, p."publishedDate" DESC, p.id DESC
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(topicIDs))
	if err != nil {
		return result, nil
	}
	defer rows.Close()
	postIDs := []int{}
	postMap := map[int]int{} // postID -> topicID
	imageIDs := []int{}
	for rows.Next() {
		var topicID, postID int
		var slug, title string
		var heroID sql.NullInt64
		if err := rows.Scan(&topicID, &postID, &slug, &title, &heroID); err != nil {
			return result, err
		}
		postIDs = append(postIDs, postID)
		postMap[postID] = topicID
		if heroID.Valid {
			imageIDs = append(imageIDs, int(heroID.Int64))
		}
	}
	if len(postIDs) > 0 {
		// 這裡簡化處理，只返回基本的 post 資訊
		// 如果需要完整的 post 資料，可以調用 enrichPosts
		for _, postID := range postIDs {
			post := Post{
				ID: strconv.Itoa(postID),
			}
			if topicID, ok := postMap[postID]; ok {
				result[topicID] = append(result[topicID], post)
			}
		}
	}
	return result, rows.Err()
}

// fetchTopicSections 查詢 topic 的 sections
func (r *Repo) fetchTopicSections(ctx context.Context, topicIDs []int) (map[int][]Section, error) {
	result := map[int][]Section{}
	if len(topicIDs) == 0 {
		return result, nil
	}
	// 根據 schema.prisma，Topic.sections 是透過 _Section_topics 表關聯（Section 是 A，Topic 是 B）
	query := `
		SELECT st."B" as topic_id, s.id, s.name, s.slug, s.state, COALESCE(s.color, '') as color
		FROM "_Section_topics" st
		JOIN "Section" s ON s.id = st."A"
		WHERE st."B" = ANY($1)
		ORDER BY st."B", s.id
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(topicIDs))
	if err != nil {
		return result, nil
	}
	defer rows.Close()
	for rows.Next() {
		var topicID int
		var s Section
		if err := rows.Scan(&topicID, &s.ID, &s.Name, &s.Slug, &s.State, &s.Color); err != nil {
			return result, err
		}
		result[topicID] = append(result[topicID], s)
	}
	return result, rows.Err()
}

// QueryVideos 查詢 videos
func (r *Repo) QueryVideos(ctx context.Context, where *VideoWhereInput, orders []OrderRule, take, skip int) ([]Video, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensureVideoPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, COALESCE(name, '') as name, "isShorts", COALESCE("youtubeUrl", '') as youtubeUrl, COALESCE("fileDuration", '') as fileDuration, COALESCE("youtubeDuration", '') as youtubeDuration, COALESCE(content, '') as content, "heroImage", COALESCE(uploader, '') as uploader, COALESCE("uploaderEmail", '') as uploaderEmail, "isFeed", COALESCE("videoSection", 'news') as videoSection, state, "publishedDate", COALESCE("publishedDateString", '') as publishedDateString, "updateTimeStamp", "createdAt", "file_filename" FROM "Video" v`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
		if f.Not != nil && f.Not.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s <> $%d`, field, argIdx))
			args = append(args, *f.Not.Equals)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("v.state", where.State)
		buildStringFilter("v.videoSection", where.VideoSection)
		buildStringFilter("v.youtubeUrl", where.YoutubeUrl)
		if where.IsShorts != nil && where.IsShorts.Equals != nil {
			conds = append(conds, fmt.Sprintf(`v."isShorts" = $%d`, argIdx))
			args = append(args, *where.IsShorts.Equals)
			argIdx++
		}
		if where.Tags != nil && where.Tags.Some != nil && where.Tags.Some.ID != nil && where.Tags.Some.ID.Equals != nil {
			// 透過 _Video_tags 表查詢（Tag 是 A，Video 是 B）
			sb.WriteString(` JOIN "_Video_tags" vt ON vt."B" = v.id`)
			tagID, err := strconv.Atoi(*where.Tags.Some.ID.Equals)
			if err == nil {
				conds = append(conds, fmt.Sprintf(`vt."A" = $%d`, argIdx))
				args = append(args, tagID)
				argIdx++
			}
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		orderParts := []string{}
		for _, o := range orders {
			dir := "ASC"
			if o.Direction == "desc" {
				dir = "DESC"
			}
			switch o.Field {
			case "publishedDate":
				orderParts = append(orderParts, fmt.Sprintf(`v."publishedDate" %s`, dir))
			case "id":
				orderParts = append(orderParts, fmt.Sprintf(`v.id %s`, dir))
			}
		}
		if len(orderParts) > 0 {
			sb.WriteString(strings.Join(orderParts, ", "))
			// 不添加次級排序，讓資料庫使用自然順序（與 target 一致）
		}
	} else {
		sb.WriteString(` ORDER BY v."publishedDate" DESC`)
	}

	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []Video{}
	videoIDs := []int{}
	heroImageIDs := []int{}
	for rows.Next() {
		var v Video
		var dbID int
		var pubAt, createdAt sql.NullTime
		var heroImageID sql.NullInt64
		var fileFilename sql.NullString
		if err := rows.Scan(&dbID, &v.Name, &v.IsShorts, &v.YoutubeUrl, &v.FileDuration, &v.YoutubeDuration, &v.Content, &heroImageID, &v.Uploader, &v.UploaderEmail, &v.IsFeed, &v.VideoSection, &v.State, &pubAt, &v.PublishedDateString, &v.UpdateTimeStamp, &createdAt, &fileFilename); err != nil {
			return nil, err
		}
		v.ID = strconv.Itoa(dbID)
		if pubAt.Valid {
			v.PublishedDate = pubAt.Time.Format(timeLayoutMilli)
		} else {
			// 當 publishedDate 為 null 時，設為空字串以匹配 target 的 nil 行為
			v.PublishedDate = ""
		}
		if createdAt.Valid {
			v.CreatedAt = createdAt.Time.Format(timeLayoutMilli)
		}
		// fileDuration 和 youtubeDuration 如果為空字串或 "0"，轉換為 ISO 8601 duration 格式
		if v.FileDuration == "" || v.FileDuration == "0" {
			v.FileDuration = "PT0S"
		}
		if v.YoutubeDuration == "" || v.YoutubeDuration == "0" {
			v.YoutubeDuration = "PT0S"
		}
		// 根據 schema.prisma，Video.videoSrc 是 virtual field，需要從 file_filename 生成
		// 格式：https://statics-dev.mirrordaily.news/video-files/{filename}
		if fileFilename.Valid && fileFilename.String != "" {
			v.VideoSrc = fmt.Sprintf("https://statics-dev.mirrordaily.news/video-files/%s", fileFilename.String)
		} else {
			v.VideoSrc = ""
		}
		if heroImageID.Valid {
			heroImageIDs = append(heroImageIDs, int(heroImageID.Int64))
			v.Metadata = map[string]any{"heroImageID": int(heroImageID.Int64)}
		}
		result = append(result, v)
		videoIDs = append(videoIDs, dbID)
	}

	// Enrich videos
	if err := r.enrichVideos(ctx, &result, videoIDs, heroImageIDs); err != nil {
		return nil, err
	}

	return result, rows.Err()
}

// QueryVideosCount 查詢 videos 數量
func (r *Repo) QueryVideosCount(ctx context.Context, where *VideoWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensureVideoPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "Video" v`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
		if f.Not != nil && f.Not.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s <> $%d`, field, argIdx))
			args = append(args, *f.Not.Equals)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("v.state", where.State)
		buildStringFilter("v.videoSection", where.VideoSection)
		buildStringFilter("v.youtubeUrl", where.YoutubeUrl)
		if where.IsShorts != nil && where.IsShorts.Equals != nil {
			conds = append(conds, fmt.Sprintf(`v."isShorts" = $%d`, argIdx))
			args = append(args, *where.IsShorts.Equals)
			argIdx++
		}
		if where.Tags != nil && where.Tags.Some != nil && where.Tags.Some.ID != nil && where.Tags.Some.ID.Equals != nil {
			sb.WriteString(` JOIN "_Video_tags" vt ON vt."A" = v.id`)
			tagID, err := strconv.Atoi(*where.Tags.Some.ID.Equals)
			if err == nil {
				conds = append(conds, fmt.Sprintf(`vt."B" = $%d`, argIdx))
				args = append(args, tagID)
				argIdx++
			}
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	var count int
	err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(&count)
	return count, err
}

// QueryVideoByUnique 根據 unique input 查詢單一 video
func (r *Repo) QueryVideoByUnique(ctx context.Context, where *VideoWhereUniqueInput) (*Video, error) {
	if where == nil {
		return nil, nil
	}
	if where.ID != nil {
		return r.QueryVideoByID(ctx, *where.ID)
	}
	return nil, nil
}

// QueryVideoByID 根據 ID 查詢單一 video
func (r *Repo) QueryVideoByID(ctx context.Context, id string) (*Video, error) {
	if id == "" {
		return nil, nil
	}
	idInt, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT id, COALESCE(name, '') as name, "isShorts", COALESCE("youtubeUrl", '') as youtubeUrl, COALESCE("fileDuration", '') as fileDuration, COALESCE("youtubeDuration", '') as youtubeDuration, COALESCE(content, '') as content, "heroImage", COALESCE(uploader, '') as uploader, COALESCE("uploaderEmail", '') as uploaderEmail, "isFeed", COALESCE("videoSection", 'news') as videoSection, state, "publishedDate", COALESCE("publishedDateString", '') as publishedDateString, "updateTimeStamp", "createdAt", "file_filename" FROM "Video" WHERE id = $1 AND state = 'published'`

	var v Video
	var dbID int
	var pubAt, createdAt sql.NullTime
	var heroImageID sql.NullInt64
	var fileFilename sql.NullString
	err = r.db.QueryRowContext(ctx, query, idInt).Scan(&dbID, &v.Name, &v.IsShorts, &v.YoutubeUrl, &v.FileDuration, &v.YoutubeDuration, &v.Content, &heroImageID, &v.Uploader, &v.UploaderEmail, &v.IsFeed, &v.VideoSection, &v.State, &pubAt, &v.PublishedDateString, &v.UpdateTimeStamp, &createdAt, &fileFilename)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v.ID = strconv.Itoa(dbID)
	if pubAt.Valid {
		v.PublishedDate = pubAt.Time.Format(timeLayoutMilli)
	} else {
		// 當 publishedDate 為 null 時，設為空字串以匹配 target 的 nil 行為
		v.PublishedDate = ""
	}
	if createdAt.Valid {
		v.CreatedAt = createdAt.Time.Format(timeLayoutMilli)
	}
	// fileDuration 和 youtubeDuration 如果為空字串或 "0"，轉換為 ISO 8601 duration 格式
	if v.FileDuration == "" || v.FileDuration == "0" {
		v.FileDuration = "PT0S"
	}
	if v.YoutubeDuration == "" || v.YoutubeDuration == "0" {
		v.YoutubeDuration = "PT0S"
	}
	// 根據 schema.prisma，Video.videoSrc 是 virtual field，需要從 file_filename 生成
	// 格式：https://statics-dev.mirrordaily.news/video-files/{filename}
	if fileFilename.Valid && fileFilename.String != "" {
		v.VideoSrc = fmt.Sprintf("https://statics-dev.mirrordaily.news/video-files/%s", fileFilename.String)
	} else {
		v.VideoSrc = ""
	}
	if heroImageID.Valid {
		heroImageIDs := []int{int(heroImageID.Int64)}
		v.Metadata = map[string]any{"heroImageID": int(heroImageID.Int64)}
		videos := []Video{v}
		if err := r.enrichVideos(ctx, &videos, []int{dbID}, heroImageIDs); err != nil {
			return nil, err
		}
		return &videos[0], nil
	}
	return &v, nil
}

// enrichVideos 豐富 videos 資料（heroImage, tags, related_posts）
func (r *Repo) enrichVideos(ctx context.Context, videos *[]Video, videoIDs []int, heroImageIDs []int) error {
	if len(*videos) == 0 {
		return nil
	}

	// Fetch images
	imagesMap, err := r.fetchImages(ctx, heroImageIDs)
	if err != nil {
		return err
	}

	// Fetch tags
	tagsMap, err := r.fetchVideoTags(ctx, videoIDs)
	if err != nil {
		return err
	}

	// Fetch related posts
	postsMap, err := r.fetchVideoRelatedPosts(ctx, videoIDs)
	if err != nil {
		return err
	}

	// Assign to videos
	for i := range *videos {
		v := &(*videos)[i]
		id, _ := strconv.Atoi(v.ID)

		// Hero image
		if heroImageID, ok := v.Metadata["heroImageID"].(int); ok {
			if img, ok := imagesMap[heroImageID]; ok {
				v.HeroImage = img
			}
		}

		// Tags
		if tags, ok := tagsMap[id]; ok {
			v.Tags = tags
		} else {
			v.Tags = []Tag{}
		}

		// Related posts
		v.RelatedPosts = postsMap[id]
	}

	return nil
}

// fetchVideoTags 查詢 video 的 tags
func (r *Repo) fetchVideoTags(ctx context.Context, videoIDs []int) (map[int][]Tag, error) {
	result := map[int][]Tag{}
	if len(videoIDs) == 0 {
		return result, nil
	}
	// 根據 Video.ts，Video.tags 是透過 _Video_tags 表關聯（Tag 是 A，Video 是 B）
	query := `
		SELECT vt."B" as video_id, t.id, t.name, t.slug
		FROM "_Video_tags" vt
		JOIN "Tag" t ON t.id = vt."A"
		WHERE vt."B" = ANY($1)
		ORDER BY vt."B", t.id
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(videoIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var videoID int
		var tag Tag
		var tagID int
		if err := rows.Scan(&videoID, &tagID, &tag.Name, &tag.Slug); err != nil {
			return result, err
		}
		tag.ID = strconv.Itoa(tagID)
		result[videoID] = append(result[videoID], tag)
	}
	return result, rows.Err()
}

// fetchVideoRelatedPosts 查詢 video 的 related posts
func (r *Repo) fetchVideoRelatedPosts(ctx context.Context, videoIDs []int) (map[int][]Post, error) {
	result := map[int][]Post{}
	if len(videoIDs) == 0 {
		return result, nil
	}
	// 根據 schema.prisma，Video.related_posts 是透過 Post_related_videos 表關聯（Post 是 A，Video 是 B）
	query := `
		SELECT prv."B" as video_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_related_videos" prv
		JOIN "Post" p ON p.id = prv."A"
		WHERE prv."B" = ANY($1) AND p.state = 'published'
		ORDER BY prv."B", p."publishedDate" DESC, p.id DESC
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(videoIDs))
	if err != nil {
		return result, nil
	}
	defer rows.Close()
	for rows.Next() {
		var videoID int
		var post Post
		var dbID int
		var heroID sql.NullInt64
		if err := rows.Scan(&videoID, &dbID, &post.Slug, &post.Title, &heroID); err != nil {
			return result, err
		}
		post.ID = strconv.Itoa(dbID)
		if heroID.Valid {
			post.Metadata = map[string]any{"heroImageID": int(heroID.Int64)}
		}
		result[videoID] = append(result[videoID], post)
	}
	return result, rows.Err()
}
