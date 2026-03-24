package query

import (
	"testing"
)

func TestExprStrings(t *testing.T) {
	tests := []struct {
		name string
		expr Expr
		want string
	}{
		{"Wildcard", &Wildcard{}, "*"},
		{"Identifier", &Identifier{Name: "value"}, "value"},
		{"StringLiteral", &StringLiteral{Value: "hello"}, "'hello'"},
		{"NumberLiteral", &NumberLiteral{Value: 42, IsInt: true}, "42"},
		{"NumberLiteral Float", &NumberLiteral{Value: 3.14, IsInt: false}, "3.14"},
		{"BooleanLiteral True", &BooleanLiteral{Value: true}, "true"},
		{"BooleanLiteral False", &BooleanLiteral{Value: false}, "false"},
		{"NowExpr", &NowExpr{}, "now()"},
		{"ParenExpr", &ParenExpr{Expr: &Identifier{Name: "x"}}, "(x)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCallString(t *testing.T) {
	call := &Call{
		Name: "mean",
		Args: []Expr{&Identifier{Name: "value"}},
	}
	got := call.String()
	if got != "mean(value)" {
		t.Errorf("Call.String() = %q, want %q", got, "mean(value)")
	}

	call2 := &Call{
		Name: "percentile",
		Args: []Expr{&Identifier{Name: "value"}, &NumberLiteral{Value: 95, IsInt: true}},
	}
	got2 := call2.String()
	if got2 != "percentile(value, 95)" {
		t.Errorf("Call.String() = %q, want %q", got2, "percentile(value, 95)")
	}
}

func TestDurationLiteralString(t *testing.T) {
	dur := &DurationLiteral{Value: 3600000000000} // 1 hour in nanoseconds
	got := dur.String()
	if got != "1h0m0s" {
		t.Errorf("DurationLiteral.String() = %q", got)
	}
}

func TestBinaryExprString(t *testing.T) {
	expr := &BinaryExpr{
		Left:  &Identifier{Name: "a"},
		Op:    PLUS,
		Right: &Identifier{Name: "b"},
	}
	got := expr.String()
	if got != "a + b" {
		t.Errorf("BinaryExpr.String() = %q, want %q", got, "a + b")
	}
}

func TestTokenString(t *testing.T) {
	tests := []struct {
		tok  Token
		want string
	}{
		{SELECT, "SELECT"},
		{FROM, "FROM"},
		{WHERE, "WHERE"},
		{AND, "AND"},
		{OR, "OR"},
		{EQ, "="},
		{NEQ, "!="},
		{LT, "<"},
		{GT, ">"},
		{PLUS, "+"},
		{MINUS, "-"},
		{MUL, "*"},
		{DIV, "/"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.tok.String()
			if got != tt.want {
				t.Errorf("Token(%d).String() = %q, want %q", tt.tok, got, tt.want)
			}
		})
	}
}
