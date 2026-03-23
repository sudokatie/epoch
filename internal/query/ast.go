package query

import (
	"fmt"
	"strings"
	"time"
)

// Statement represents a parsed SQL statement
type Statement interface {
	stmt()
	String() string
}

// SelectStatement represents a SELECT query
type SelectStatement struct {
	Fields      []*Field
	Measurement string
	Condition   Expr
	GroupBy     *GroupBy
	OrderBy     *OrderBy
	Limit       int
	Offset      int
}

func (*SelectStatement) stmt() {}

func (s *SelectStatement) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")

	for i, f := range s.Fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(f.String())
	}

	sb.WriteString(" FROM ")
	sb.WriteString(s.Measurement)

	if s.Condition != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(s.Condition.String())
	}

	if s.GroupBy != nil {
		sb.WriteString(" ")
		sb.WriteString(s.GroupBy.String())
	}

	if s.OrderBy != nil {
		sb.WriteString(" ")
		sb.WriteString(s.OrderBy.String())
	}

	if s.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", s.Limit))
	}

	if s.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", s.Offset))
	}

	return sb.String()
}

// Field represents a field in the SELECT clause
type Field struct {
	Expr  Expr
	Alias string
}

func (f *Field) String() string {
	s := f.Expr.String()
	if f.Alias != "" {
		s += " AS " + f.Alias
	}
	return s
}

// GroupBy represents a GROUP BY clause
type GroupBy struct {
	Tags     []string
	Interval time.Duration
}

func (g *GroupBy) String() string {
	var parts []string
	if g.Interval > 0 {
		parts = append(parts, fmt.Sprintf("time(%s)", g.Interval))
	}
	parts = append(parts, g.Tags...)
	return "GROUP BY " + strings.Join(parts, ", ")
}

// OrderBy represents an ORDER BY clause
type OrderBy struct {
	Field string
	Desc  bool
}

func (o *OrderBy) String() string {
	s := "ORDER BY " + o.Field
	if o.Desc {
		s += " DESC"
	} else {
		s += " ASC"
	}
	return s
}

// Expr represents an expression in the query
type Expr interface {
	expr()
	String() string
}

// Wildcard represents a * in SELECT
type Wildcard struct{}

func (*Wildcard) expr()         {}
func (*Wildcard) String() string { return "*" }

// Identifier represents a field or tag name
type Identifier struct {
	Name string
}

func (*Identifier) expr()           {}
func (i *Identifier) String() string { return i.Name }

// StringLiteral represents a quoted string
type StringLiteral struct {
	Value string
}

func (*StringLiteral) expr()           {}
func (s *StringLiteral) String() string { return fmt.Sprintf("'%s'", s.Value) }

// NumberLiteral represents a numeric value
type NumberLiteral struct {
	Value float64
	IsInt bool
}

func (*NumberLiteral) expr() {}
func (n *NumberLiteral) String() string {
	if n.IsInt {
		return fmt.Sprintf("%d", int64(n.Value))
	}
	return fmt.Sprintf("%g", n.Value)
}

// BooleanLiteral represents true or false
type BooleanLiteral struct {
	Value bool
}

func (*BooleanLiteral) expr() {}
func (b *BooleanLiteral) String() string {
	if b.Value {
		return "true"
	}
	return "false"
}

// TimeLiteral represents a time value
type TimeLiteral struct {
	Value time.Time
}

func (*TimeLiteral) expr() {}
func (t *TimeLiteral) String() string {
	return fmt.Sprintf("'%s'", t.Value.Format(time.RFC3339Nano))
}

// DurationLiteral represents a duration like 1h, 5m, etc.
type DurationLiteral struct {
	Value time.Duration
}

func (*DurationLiteral) expr() {}
func (d *DurationLiteral) String() string {
	return d.Value.String()
}

// Call represents a function call like mean(field)
type Call struct {
	Name string
	Args []Expr
}

func (*Call) expr() {}
func (c *Call) String() string {
	var args []string
	for _, arg := range c.Args {
		args = append(args, arg.String())
	}
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(args, ", "))
}

// BinaryExpr represents a binary expression like a = b or a > 5
type BinaryExpr struct {
	Left  Expr
	Op    Token
	Right Expr
}

func (*BinaryExpr) expr() {}
func (b *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.Left.String(), b.Op.String(), b.Right.String())
}

// ParenExpr represents a parenthesized expression
type ParenExpr struct {
	Expr Expr
}

func (*ParenExpr) expr() {}
func (p *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", p.Expr.String())
}

// NowExpr represents the now() function
type NowExpr struct{}

func (*NowExpr) expr()         {}
func (*NowExpr) String() string { return "now()" }

// Token types
type Token int

const (
	ILLEGAL Token = iota
	EOF
	WS

	// Literals
	IDENT
	STRING
	NUMBER
	DURATION

	// Operators
	EQ        // =
	NEQ       // != or <>
	LT        // <
	LTE       // <=
	GT        // >
	GTE       // >=
	PLUS      // +
	MINUS     // -
	MUL       // *
	DIV       // /
	LPAREN    // (
	RPAREN    // )
	COMMA     // ,
	SEMICOLON // ;

	// Keywords
	SELECT
	FROM
	WHERE
	AND
	OR
	NOT
	GROUP
	BY
	ORDER
	ASC
	DESC
	LIMIT
	OFFSET
	AS
	TRUE
	FALSE
	NOW
	TIME
)

var tokenStrings = map[Token]string{
	ILLEGAL:   "ILLEGAL",
	EOF:       "EOF",
	WS:        "WS",
	IDENT:     "IDENT",
	STRING:    "STRING",
	NUMBER:    "NUMBER",
	DURATION:  "DURATION",
	EQ:        "=",
	NEQ:       "!=",
	LT:        "<",
	LTE:       "<=",
	GT:        ">",
	GTE:       ">=",
	PLUS:      "+",
	MINUS:     "-",
	MUL:       "*",
	DIV:       "/",
	LPAREN:    "(",
	RPAREN:    ")",
	COMMA:     ",",
	SEMICOLON: ";",
	SELECT:    "SELECT",
	FROM:      "FROM",
	WHERE:     "WHERE",
	AND:       "AND",
	OR:        "OR",
	NOT:       "NOT",
	GROUP:     "GROUP",
	BY:        "BY",
	ORDER:     "ORDER",
	ASC:       "ASC",
	DESC:      "DESC",
	LIMIT:     "LIMIT",
	OFFSET:    "OFFSET",
	AS:        "AS",
	TRUE:      "TRUE",
	FALSE:     "FALSE",
	NOW:       "NOW",
	TIME:      "TIME",
}

func (t Token) String() string {
	if s, ok := tokenStrings[t]; ok {
		return s
	}
	return fmt.Sprintf("Token(%d)", t)
}

var keywords = map[string]Token{
	"SELECT": SELECT,
	"FROM":   FROM,
	"WHERE":  WHERE,
	"AND":    AND,
	"OR":     OR,
	"NOT":    NOT,
	"GROUP":  GROUP,
	"BY":     BY,
	"ORDER":  ORDER,
	"ASC":    ASC,
	"DESC":   DESC,
	"LIMIT":  LIMIT,
	"OFFSET": OFFSET,
	"AS":     AS,
	"TRUE":   TRUE,
	"FALSE":  FALSE,
	"NOW":    NOW,
	"TIME":   TIME,
}

// LookupKeyword returns the keyword token for s, or IDENT if not a keyword
func LookupKeyword(s string) Token {
	if tok, ok := keywords[strings.ToUpper(s)]; ok {
		return tok
	}
	return IDENT
}
