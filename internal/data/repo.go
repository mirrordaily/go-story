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
	ID          string         `json:"id"`
	ImageFile   ImageFile      `json:"imageFile"`
	Resized     Resized        `json:"resized"`
	ResizedWebp Resized        `json:"resizedWebp"`
	Metadata    map[string]any `json:"-"`
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
	ID        string `json:"id"`
	VideoSrc  string `json:"videoSrc"`
	HeroImage *Photo `json:"heroImage"`
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
	Slug string `json:"slug"`
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
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, "apiDataBrief", "apiData", content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo" FROM "Post" p`)

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
			p             Post
			dbID          int
			publishedAt   sql.NullTime
			updatedAt     sql.NullTime
			heroImageID   sql.NullInt64
			heroVideoID   sql.NullInt64
			ogImageID     sql.NullInt64
			topicsID      sql.NullInt64
			relatedsOneID sql.NullInt64
			relatedsTwoID sql.NullInt64
			briefRaw      []byte
			apiDataBrief  []byte
			apiData       []byte
			contentRaw    []byte
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
			"heroImageID":   nullableInt(heroImageID),
			"ogImageID":     nullableInt(ogImageID),
			"heroVideoID":   nullableInt(heroVideoID),
			"topicsID":      nullableInt(topicsID),
			"relatedsOneID": nullableInt(relatedsOneID),
			"relatedsTwoID": nullableInt(relatedsTwoID),
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
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, "apiDataBrief", "apiData", content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo" FROM "Post" p WHERE `)
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
		p             Post
		dbID          int
		publishedAt   sql.NullTime
		updatedAt     sql.NullTime
		heroImageID   sql.NullInt64
		heroVideoID   sql.NullInt64
		ogImageID     sql.NullInt64
		topicsID      sql.NullInt64
		relatedsOneID sql.NullInt64
		relatedsTwoID sql.NullInt64
		briefRaw      []byte
		apiDataBrief  []byte
		apiData       []byte
		contentRaw    []byte
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
		"heroImageID":   nullableInt(heroImageID),
		"ogImageID":     nullableInt(ogImageID),
		"heroVideoID":   nullableInt(heroVideoID),
		"topicsID":      nullableInt(topicsID),
		"relatedsOneID": nullableInt(relatedsOneID),
		"relatedsTwoID": nullableInt(relatedsTwoID),
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
	for _, p := range posts {
		if id := getMetaInt(p.Metadata, "relatedsOneID"); id > 0 {
			relatedOneIDs = append(relatedOneIDs, id)
		}
		if id := getMetaInt(p.Metadata, "relatedsTwoID"); id > 0 {
			relatedTwoIDs = append(relatedTwoIDs, id)
		}
	}
	relatedSinglesIDs := append(relatedOneIDs, relatedTwoIDs...)
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
		if len(p.Warnings) > 0 {
			p.Warning = &p.Warnings[0]
		}
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
	// Warnings 是從 relateds 來的，需要 join Warning 表與 relateds
	// 根據 schema.prisma，表名是 _Post_Warnings（注意大小寫），A 是 Post ID，B 是 Warning ID
	// 先嘗試從 relateds 取得 warnings
	query := `
		SELECT DISTINCT r."A" as post_id, w.id, w.content
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."B"
		JOIN "_Post_Warnings" pw ON pw."A" = p.id
		JOIN "Warning" w ON w.id = pw."B"
		WHERE r."A" = ANY($1)
		UNION
		SELECT DISTINCT r."B" as post_id, w.id, w.content
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."A"
		JOIN "_Post_Warnings" pw ON pw."A" = p.id
		JOIN "Warning" w ON w.id = pw."B"
		WHERE r."B" = ANY($1)
		ORDER BY post_id, w.id
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		// 如果查詢失敗（可能是表名不對），嘗試使用小寫表名
		query = `
			SELECT DISTINCT r."A" as post_id, w.id, w.content
			FROM "_Post_relateds" r
			JOIN "Post" p ON p.id = r."B"
			JOIN "_Post_warnings" pw ON pw."A" = p.id
			JOIN "Warning" w ON w.id = pw."B"
			WHERE r."A" = ANY($1)
			UNION
			SELECT DISTINCT r."B" as post_id, w.id, w.content
			FROM "_Post_relateds" r
			JOIN "Post" p ON p.id = r."A"
			JOIN "_Post_warnings" pw ON pw."A" = p.id
			JOIN "Warning" w ON w.id = pw."B"
			WHERE r."B" = ANY($1)
			ORDER BY post_id, w.id
		`
		rows, err = r.db.QueryContext(ctx, query, pqIntArray(postIDs))
		if err != nil {
			return result, err
		}
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
	rows, err := r.db.QueryContext(ctx, `SELECT id, COALESCE("imageFile_id", ''), COALESCE("imageFile_extension", ''), "imageFile_width", "imageFile_height" FROM "Image" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var im struct {
			id     int
			fileID string
			ext    string
			width  sql.NullInt64
			height sql.NullInt64
		}
		if err := rows.Scan(&im.id, &im.fileID, &im.ext, &im.width, &im.height); err != nil {
			return result, err
		}
		photo := Photo{
			ID: strconv.Itoa(im.id),
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
	// categories 是從 relateds 來的，需要 join Category 表與 relateds
	// 根據 schema.prisma，External 的 relateds 是 Post[]，所以從 related posts 的 categories 取得
	// 先嘗試從 relateds 取得 categories
	query := `
		SELECT DISTINCT er."A" as external_id, c.id, c.name, c.slug, c.state
		FROM "_External_relateds" er
		JOIN "Post" p ON p.id = er."B"
		JOIN "_Category_posts" cp ON cp."B" = p.id
		JOIN "Category" c ON c.id = cp."A"
		WHERE er."A" = ANY($1)
		ORDER BY er."A", c.id
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		return result, err
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
