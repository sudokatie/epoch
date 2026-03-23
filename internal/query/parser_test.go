package query

import (
	"testing"
	"time"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input  string
		tokens []Token
	}{
		{"SELECT", []Token{SELECT, EOF}},
		{"SELECT * FROM cpu", []Token{SELECT, MUL, FROM, IDENT, EOF}},
		{"WHERE x = 5", []Token{WHERE, IDENT, EQ, NUMBER, EOF}},
		{"x > 10 AND y < 20", []Token{IDENT, GT, NUMBER, AND, IDENT, LT, NUMBER, EOF}},
		{"x != 'hello'", []Token{IDENT, NEQ, STRING, EOF}},
		{"x <> 5", []Token{IDENT, NEQ, NUMBER, EOF}},
		{"x <= 5", []Token{IDENT, LTE, NUMBER, EOF}},
		{"x >= 5", []Token{IDENT, GTE, NUMBER, EOF}},
		{"mean(value)", []Token{IDENT, LPAREN, IDENT, RPAREN, EOF}},
		{"GROUP BY time(1h)", []Token{GROUP, BY, TIME, LPAREN, DURATION, RPAREN, EOF}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := NewLexer(tt.input)
			for i, expected := range tt.tokens {
				tok, _ := l.NextToken()
				if tok != expected {
					t.Errorf("token %d: got %v, want %v", i, tok, expected)
				}
			}
		})
	}
}

func TestParseSimpleSelect(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if sel.Measurement != "cpu" {
		t.Errorf("measurement = %q, want cpu", sel.Measurement)
	}

	if len(sel.Fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(sel.Fields))
	}

	if _, ok := sel.Fields[0].Expr.(*Wildcard); !ok {
		t.Errorf("expected Wildcard, got %T", sel.Fields[0].Expr)
	}
}

func TestParseSelectWithFields(t *testing.T) {
	stmt, err := ParseQuery("SELECT usage, temp FROM cpu")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if len(sel.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(sel.Fields))
	}

	if id, ok := sel.Fields[0].Expr.(*Identifier); !ok || id.Name != "usage" {
		t.Errorf("field 0: expected usage, got %v", sel.Fields[0].Expr)
	}

	if id, ok := sel.Fields[1].Expr.(*Identifier); !ok || id.Name != "temp" {
		t.Errorf("field 1: expected temp, got %v", sel.Fields[1].Expr)
	}
}

func TestParseSelectWithAlias(t *testing.T) {
	stmt, err := ParseQuery("SELECT usage AS cpu_usage FROM cpu")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.Fields[0].Alias != "cpu_usage" {
		t.Errorf("alias = %q, want cpu_usage", sel.Fields[0].Alias)
	}
}

func TestParseSelectWithWhere(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu WHERE host = 'server1'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.Condition == nil {
		t.Fatal("expected condition")
	}

	bin, ok := sel.Condition.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", sel.Condition)
	}

	if bin.Op != EQ {
		t.Errorf("op = %v, want =", bin.Op)
	}
}

func TestParseSelectWithTimeRange(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu WHERE time > now() - 1h")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.Condition == nil {
		t.Fatal("expected condition")
	}
}

func TestParseSelectWithAnd(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu WHERE host = 'server1' AND region = 'us-west'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	bin, ok := sel.Condition.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", sel.Condition)
	}

	if bin.Op != AND {
		t.Errorf("top-level op = %v, want AND", bin.Op)
	}
}

func TestParseSelectWithOr(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu WHERE host = 'server1' OR host = 'server2'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	bin, ok := sel.Condition.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", sel.Condition)
	}

	if bin.Op != OR {
		t.Errorf("top-level op = %v, want OR", bin.Op)
	}
}

func TestParseSelectWithFunction(t *testing.T) {
	stmt, err := ParseQuery("SELECT mean(usage) FROM cpu")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	call, ok := sel.Fields[0].Expr.(*Call)
	if !ok {
		t.Fatalf("expected Call, got %T", sel.Fields[0].Expr)
	}

	if call.Name != "mean" {
		t.Errorf("function name = %q, want mean", call.Name)
	}

	if len(call.Args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(call.Args))
	}
}

func TestParseSelectWithGroupBy(t *testing.T) {
	stmt, err := ParseQuery("SELECT mean(usage) FROM cpu GROUP BY time(5m), host")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.GroupBy == nil {
		t.Fatal("expected GROUP BY")
	}

	if sel.GroupBy.Interval != 5*time.Minute {
		t.Errorf("interval = %v, want 5m", sel.GroupBy.Interval)
	}

	if len(sel.GroupBy.Tags) != 1 || sel.GroupBy.Tags[0] != "host" {
		t.Errorf("tags = %v, want [host]", sel.GroupBy.Tags)
	}
}

func TestParseSelectWithOrderBy(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu ORDER BY time DESC")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.OrderBy == nil {
		t.Fatal("expected ORDER BY")
	}

	if sel.OrderBy.Field != "time" {
		t.Errorf("field = %q, want time", sel.OrderBy.Field)
	}

	if !sel.OrderBy.Desc {
		t.Error("expected DESC")
	}
}

func TestParseSelectWithLimit(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu LIMIT 100")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.Limit != 100 {
		t.Errorf("limit = %d, want 100", sel.Limit)
	}
}

func TestParseSelectWithOffset(t *testing.T) {
	stmt, err := ParseQuery("SELECT * FROM cpu LIMIT 100 OFFSET 50")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.Offset != 50 {
		t.Errorf("offset = %d, want 50", sel.Offset)
	}
}

func TestParseSelectFull(t *testing.T) {
	query := `SELECT mean(usage), max(temp) FROM cpu 
		WHERE host = 'server1' AND time > now() - 1h 
		GROUP BY time(5m), region 
		ORDER BY time DESC 
		LIMIT 100 OFFSET 10`

	stmt, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if len(sel.Fields) != 2 {
		t.Errorf("fields = %d, want 2", len(sel.Fields))
	}
	if sel.Measurement != "cpu" {
		t.Errorf("measurement = %q", sel.Measurement)
	}
	if sel.Condition == nil {
		t.Error("expected condition")
	}
	if sel.GroupBy == nil {
		t.Error("expected group by")
	}
	if sel.OrderBy == nil {
		t.Error("expected order by")
	}
	if sel.Limit != 100 {
		t.Errorf("limit = %d", sel.Limit)
	}
	if sel.Offset != 10 {
		t.Errorf("offset = %d", sel.Offset)
	}
}

func TestParseNumberLiterals(t *testing.T) {
	tests := []struct {
		input string
		value float64
		isInt bool
	}{
		{"SELECT 42 FROM x", 42, true},
		{"SELECT 3.14 FROM x", 3.14, false},
		{"SELECT -5 FROM x", -5, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			sel := stmt.(*SelectStatement)
			num, ok := sel.Fields[0].Expr.(*NumberLiteral)
			if !ok {
				// Check for binary expr (negative numbers)
				if bin, ok := sel.Fields[0].Expr.(*BinaryExpr); ok {
					num, _ = bin.Right.(*NumberLiteral)
				}
			}
			if num == nil {
				t.Fatalf("expected NumberLiteral")
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"1s", 1000000000},
		{"5m", 5 * 60 * 1000000000},
		{"2h", 2 * 3600 * 1000000000},
		{"1d", 86400 * 1000000000},
		{"1w", 7 * 86400 * 1000000000},
		{"500ms", 500000000},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseDuration(tt.input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []string{
		"SELEC * FROM cpu",          // typo
		"SELECT FROM cpu",            // missing fields
		"SELECT * cpu",               // missing FROM
		"SELECT * FROM",              // missing measurement
		"SELECT mean( FROM cpu",      // unclosed paren
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := ParseQuery(input)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestStatementString(t *testing.T) {
	query := "SELECT mean(usage) FROM cpu WHERE host = 'server1' GROUP BY time(5m) ORDER BY time DESC LIMIT 100"
	stmt, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	// Should be able to convert back to string
	s := stmt.String()
	if s == "" {
		t.Error("String() returned empty")
	}
	t.Logf("String(): %s", s)
}

func BenchmarkParse(b *testing.B) {
	query := "SELECT mean(usage), max(temp) FROM cpu WHERE host = 'server1' AND time > now() - 1h GROUP BY time(5m) ORDER BY time DESC LIMIT 100"

	for i := 0; i < b.N; i++ {
		ParseQuery(query)
	}
}
