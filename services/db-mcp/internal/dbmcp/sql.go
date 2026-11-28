package dbmcp

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// The grammar is the entire SQL interface. Driver statements are generated from
// this tree. No fragment of the caller's SQL is appended to a driver query.
var sqlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "Directive", Pattern: `/\*[!+]`}, // Vendor-executable comments and hints are outside the grammar.
	{Name: "comment", Pattern: `--[^\r\n]*|(?s:/\*.*?\*/)`},
	{Name: "Keyword", Pattern: `(?i:\b(?:SELECT|AS|FROM|WHERE|AND|OR|GROUP|BY|ORDER|ASC|DESC|NULLS|FIRST|LAST|LIMIT|TRUE|FALSE)\b)`},
	{Name: "Ident", Pattern: `[A-Za-z_][A-Za-z0-9_]*`},
	{Name: "Quoted", Pattern: "\"(?:[^\"]|\"\")*\"|\x60(?:[^\x60]|\x60\x60)*\x60"},
	{Name: "String", Pattern: `'(?:''|[^'])*'`},
	{Name: "Number", Pattern: `[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?`},
	{Name: "Op", Pattern: `<>|!=|<=|>=|[=<>]`},
	{Name: "Punct", Pattern: `[?*.,();]`},
})
var selectGrammar = participle.MustBuild[selectQuery](participle.Lexer(sqlLexer), participle.CaseInsensitive("Keyword", "Ident"), participle.UseLookahead(2))

type selectQuery struct {
	Columns   []selectItem `parser:"'SELECT' @@ (',' @@)*"`
	From      string       `parser:"'FROM' @(Ident|Quoted)"`
	Qualified string       `parser:"('.' @(Ident|Quoted))?"`
	Where     *disjunction `parser:"('WHERE' @@)?"`
	Group     []string     `parser:"('GROUP' 'BY' @(Ident|Quoted) (',' @(Ident|Quoted))*)?"`
	Order     []orderTerm  `parser:"('ORDER' 'BY' @@ (',' @@)*)?"`
	Limit     string       `parser:"('LIMIT' @Number)? ';'?"`
}
type selectItem struct {
	Star   bool   `parser:"( @'*'"`
	Count  bool   `parser:"| @'COUNT' '(' '*' ')'"`
	Column string `parser:"| @(Ident|Quoted) )"`
	Alias  string `parser:"('AS'? @(Ident|Quoted))?"`
}
type orderTerm struct {
	Field     string `parser:"@(Ident|Quoted)"`
	Direction string `parser:"@('ASC'|'DESC')?"`
	Nulls     string `parser:"('NULLS' @('FIRST'|'LAST'))?"`
}
type disjunction struct {
	Terms []*conjunction `parser:"@@ ('OR' @@)*"`
}
type conjunction struct {
	Terms []*predicate `parser:"@@ ('AND' @@)*"`
}
type predicate struct {
	Nested *disjunction `parser:"( '(' @@ ')' "`
	Field  string       `parser:"| @(Ident|Quoted)"`
	Op     string       `parser:"@Op"`
	Value  string       `parser:"@(String|Quoted|Number|'?'|'TRUE'|'FALSE') )"`
}

type statement struct {
	text string
	args []any
}

const badSQL = refusal("Use a single catalog view, approved columns, comparisons, COUNT(*), grouping, ordering or LIMIT.")

type compiler struct {
	target        *Target
	fields        []string
	params, bound []any
}

func (c *compiler) name(raw string) (string, error) {
	if len(raw) >= 2 {
		delim := raw[0]
		if delim == '"' || delim == '\x60' {
			if (delim == '"') != (c.target.Engine == "postgres") {
				return "", badSQL
			}
			raw = raw[1 : len(raw)-1]
		}
	}
	if !identifier.MatchString(raw) {
		return "", badSQL
	}
	return raw, nil
}
func (c *compiler) quote(name string) string {
	if c.target.Engine == "postgres" {
		return "\"" + name + "\""
	}
	return "\x60" + name + "\x60"
}
func (c *compiler) field(raw string, allowed []string) (string, error) {
	name, err := c.name(raw)
	if err != nil || !slices.Contains(allowed, name) {
		return "", badSQL
	}
	return c.quote(name), nil
}
func compileSQL(t *Target, source string, params []any, cap int) (statement, error) {
	if len(source) == 0 || len(source) > 8000 || len(params) > 100 {
		return statement{}, badSQL
	}
	// Bound recursion before the grammar descends through nested predicates.
	tokens, err := selectGrammar.Lex("", strings.NewReader(source))
	if err != nil {
		return statement{}, badSQL
	}
	depth := 0
	for _, tok := range tokens {
		switch tok.Value {
		case "(":
			depth++
		case ")":
			depth--
		}
		if depth < 0 || depth > 20 {
			return statement{}, badSQL
		}
	}
	q, err := selectGrammar.ParseString("", source)
	if err != nil {
		return statement{}, badSQL
	}
	c := compiler{target: t, params: params}
	object, err := c.name(q.From)
	if err != nil {
		return statement{}, err
	}
	if q.Qualified != "" {
		if object != t.Schema {
			return statement{}, badSQL
		}
		object, err = c.name(q.Qualified)
		if err != nil {
			return statement{}, err
		}
	}
	c.fields, err = t.fields(object)
	if err != nil {
		return statement{}, err
	}
	orderable := slices.Clone(c.fields)
	var columns []string
	for _, item := range q.Columns {
		if item.Star {
			if item.Alias != "" {
				return statement{}, badSQL
			}
			for _, f := range c.fields {
				columns = append(columns, c.quote(f))
			}
			continue
		}
		expr := "COUNT(*)"
		if !item.Count {
			expr, err = c.field(item.Column, c.fields)
			if err != nil {
				return statement{}, err
			}
		}
		if item.Alias != "" {
			alias, err := c.name(item.Alias)
			if err != nil {
				return statement{}, err
			}
			orderable = append(orderable, alias)
			expr += " AS " + c.quote(alias)
		}
		columns = append(columns, expr)
	}
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + c.quote(t.Schema) + "." + c.quote(object)
	if q.Where != nil {
		where, err := c.where(q.Where)
		if err != nil {
			return statement{}, err
		}
		query += " WHERE " + where
	}
	if len(c.params) != 0 {
		return statement{}, badSQL
	}
	if len(q.Group) > 0 {
		var group []string
		for _, raw := range q.Group {
			f, err := c.field(raw, c.fields)
			if err != nil {
				return statement{}, err
			}
			group = append(group, f)
		}
		query += " GROUP BY " + strings.Join(group, ", ")
	}
	if len(q.Order) > 0 {
		var order []string
		for _, term := range q.Order {
			f, err := c.field(term.Field, orderable)
			if err != nil {
				return statement{}, err
			}
			direction := "ASC"
			if strings.EqualFold(term.Direction, "DESC") {
				direction = "DESC"
			}
			if term.Nulls != "" {
				nulls := strings.ToUpper(term.Nulls)
				if t.Engine == "postgres" {
					direction += " NULLS " + nulls
				} else {
					nullDirection := "ASC"
					if nulls == "FIRST" {
						nullDirection = "DESC"
					}
					order = append(order, "("+f+" IS NULL) "+nullDirection)
				}
			}
			order = append(order, f+" "+direction)
		}
		query += " ORDER BY " + strings.Join(order, ", ")
	}
	limit := cap + 1
	if q.Limit != "" {
		n, err := strconv.ParseUint(q.Limit, 10, 64)
		if err != nil {
			return statement{}, badSQL
		}
		if n < uint64(limit) {
			limit = int(n)
		}
	}
	return statement{query + " LIMIT " + strconv.Itoa(limit), c.bound}, nil
}
func (c *compiler) where(expr *disjunction) (string, error) {
	var groups []string
	for _, group := range expr.Terms {
		var terms []string
		for _, p := range group.Terms {
			if p.Nested != nil {
				nested, err := c.where(p.Nested)
				if err != nil {
					return "", err
				}
				terms = append(terms, "("+nested+")")
				continue
			}
			f, err := c.field(p.Field, c.fields)
			if err != nil {
				return "", err
			}
			value, err := c.value(p.Value)
			if err != nil {
				return "", err
			}
			c.bound = append(c.bound, value)
			bind := "?"
			if c.target.Engine == "postgres" {
				bind = "$" + strconv.Itoa(len(c.bound))
			}
			op := p.Op
			if op == "!=" {
				op = "<>"
			}
			terms = append(terms, f+" "+op+" "+bind)
		}
		groups = append(groups, strings.Join(terms, " AND "))
	}
	return strings.Join(groups, " OR "), nil
}
func (c *compiler) value(token string) (any, error) {
	switch {
	case token == "?":
		if len(c.params) == 0 {
			return nil, badSQL
		}
		v := c.params[0]
		c.params = c.params[1:]
		switch n := v.(type) {
		case nil, string, bool, int64, float64:
			return n, nil
		case json.Number:
			return n.String(), nil // Preserve decimal precision through type inference.
		default:
			return nil, badSQL
		}
	case strings.EqualFold(token, "true"), strings.EqualFold(token, "false"):
		return strings.EqualFold(token, "true"), nil
	case token[0] == '\'' || (token[0] == '"' && c.target.Engine == "mysql"):
		delim := token[:1]
		value := strings.ReplaceAll(token[1:len(token)-1], delim+delim, delim)
		// MySQL backslash escapes depend on sql_mode. Bound values are unambiguous.
		if c.target.Engine == "mysql" && strings.Contains(value, `\`) {
			return nil, refusal("Use a bound parameter for a MySQL string containing a backslash.")
		}
		return value, nil
	case token[0] >= '0' && token[0] <= '9':
		return token, nil
	default:
		return nil, badSQL
	}
}
