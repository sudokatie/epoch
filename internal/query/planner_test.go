package query

import (
	"strings"
	"testing"
	"time"
)

func TestPlannerSimpleScan(t *testing.T) {
	stmt := &SelectStatement{
		Fields:      []*Field{{Expr: &Wildcard{}}},
		Measurement: "cpu",
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	scan, ok := plan.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", plan)
	}

	if scan.Measurement != "cpu" {
		t.Errorf("expected measurement 'cpu', got '%s'", scan.Measurement)
	}
}

func TestPlannerWithTimeRange(t *testing.T) {
	// SELECT * FROM cpu WHERE time > now() - 1h
	stmt := &SelectStatement{
		Fields:      []*Field{{Expr: &Wildcard{}}},
		Measurement: "cpu",
		Condition: &BinaryExpr{
			Left: &Identifier{Name: "time"},
			Op:   GT,
			Right: &BinaryExpr{
				Left:  &NowExpr{},
				Op:    MINUS,
				Right: &DurationLiteral{Value: time.Hour},
			},
		},
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	scan, ok := plan.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", plan)
	}

	if scan.TimeRange.Start.IsZero() {
		t.Error("expected time range start to be set")
	}

	// Should be approximately 1 hour ago
	expectedStart := time.Now().Add(-time.Hour)
	diff := scan.TimeRange.Start.Sub(expectedStart)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("time range start off by %v", diff)
	}
}

func TestPlannerWithTagFilter(t *testing.T) {
	// SELECT * FROM cpu WHERE host = 'server1'
	stmt := &SelectStatement{
		Fields:      []*Field{{Expr: &Wildcard{}}},
		Measurement: "cpu",
		Condition: &BinaryExpr{
			Left:  &Identifier{Name: "host"},
			Op:    EQ,
			Right: &StringLiteral{Value: "server1"},
		},
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	scan, ok := plan.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", plan)
	}

	if scan.TagFilters["host"] != "server1" {
		t.Errorf("expected tag filter host=server1, got %v", scan.TagFilters)
	}
}

func TestPlannerWithFieldFilter(t *testing.T) {
	// SELECT * FROM cpu WHERE usage > 80
	stmt := &SelectStatement{
		Fields:      []*Field{{Expr: &Wildcard{}}},
		Measurement: "cpu",
		Condition: &BinaryExpr{
			Left:  &Identifier{Name: "usage"},
			Op:    GT,
			Right: &NumberLiteral{Value: 80, IsInt: true},
		},
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Field filter should remain as FilterNode
	filter, ok := plan.(*FilterNode)
	if !ok {
		t.Fatalf("expected FilterNode, got %T", plan)
	}

	if filter.Predicate.String() != "usage > 80" {
		t.Errorf("unexpected predicate: %s", filter.Predicate.String())
	}
}

func TestPlannerWithAggregation(t *testing.T) {
	// SELECT mean(usage) FROM cpu
	stmt := &SelectStatement{
		Fields: []*Field{{
			Expr: &Call{
				Name: "mean",
				Args: []Expr{&Identifier{Name: "usage"}},
			},
		}},
		Measurement: "cpu",
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	group, ok := plan.(*GroupNode)
	if !ok {
		t.Fatalf("expected GroupNode, got %T", plan)
	}

	if len(group.Aggregates) != 1 {
		t.Fatalf("expected 1 aggregate, got %d", len(group.Aggregates))
	}

	if group.Aggregates[0].Func != "mean" {
		t.Errorf("expected function 'mean', got '%s'", group.Aggregates[0].Func)
	}

	if group.Aggregates[0].Field != "usage" {
		t.Errorf("expected field 'usage', got '%s'", group.Aggregates[0].Field)
	}
}

func TestPlannerWithGroupBy(t *testing.T) {
	// SELECT mean(usage) FROM cpu GROUP BY host, time(5m)
	stmt := &SelectStatement{
		Fields: []*Field{{
			Expr: &Call{
				Name: "mean",
				Args: []Expr{&Identifier{Name: "usage"}},
			},
		}},
		Measurement: "cpu",
		GroupBy: &GroupBy{
			Tags:     []string{"host"},
			Interval: 5 * time.Minute,
		},
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	group, ok := plan.(*GroupNode)
	if !ok {
		t.Fatalf("expected GroupNode, got %T", plan)
	}

	if len(group.Tags) != 1 || group.Tags[0] != "host" {
		t.Errorf("expected tags [host], got %v", group.Tags)
	}

	if group.Interval != 5*time.Minute {
		t.Errorf("expected interval 5m, got %v", group.Interval)
	}
}

func TestPlannerWithOrderBy(t *testing.T) {
	// SELECT * FROM cpu ORDER BY time DESC
	stmt := &SelectStatement{
		Fields:      []*Field{{Expr: &Wildcard{}}},
		Measurement: "cpu",
		OrderBy: &OrderBy{
			Field: "time",
			Desc:  true,
		},
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sort, ok := plan.(*SortNode)
	if !ok {
		t.Fatalf("expected SortNode, got %T", plan)
	}

	if sort.Field != "time" {
		t.Errorf("expected field 'time', got '%s'", sort.Field)
	}

	if !sort.Desc {
		t.Error("expected DESC order")
	}
}

func TestPlannerWithLimit(t *testing.T) {
	// SELECT * FROM cpu LIMIT 100 OFFSET 50
	stmt := &SelectStatement{
		Fields:      []*Field{{Expr: &Wildcard{}}},
		Measurement: "cpu",
		Limit:       100,
		Offset:      50,
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	limit, ok := plan.(*LimitNode)
	if !ok {
		t.Fatalf("expected LimitNode, got %T", plan)
	}

	if limit.Limit != 100 {
		t.Errorf("expected limit 100, got %d", limit.Limit)
	}

	if limit.Offset != 50 {
		t.Errorf("expected offset 50, got %d", limit.Offset)
	}
}

func TestPlannerComplexQuery(t *testing.T) {
	// SELECT mean(usage), max(usage) FROM cpu 
	// WHERE time > now() - 1h AND host = 'server1'
	// GROUP BY host, time(5m)
	// ORDER BY time DESC
	// LIMIT 100
	stmt := &SelectStatement{
		Fields: []*Field{
			{Expr: &Call{Name: "mean", Args: []Expr{&Identifier{Name: "usage"}}}},
			{Expr: &Call{Name: "max", Args: []Expr{&Identifier{Name: "usage"}}}},
		},
		Measurement: "cpu",
		Condition: &BinaryExpr{
			Left: &BinaryExpr{
				Left: &Identifier{Name: "time"},
				Op:   GT,
				Right: &BinaryExpr{
					Left:  &NowExpr{},
					Op:    MINUS,
					Right: &DurationLiteral{Value: time.Hour},
				},
			},
			Op: AND,
			Right: &BinaryExpr{
				Left:  &Identifier{Name: "host"},
				Op:    EQ,
				Right: &StringLiteral{Value: "server1"},
			},
		},
		GroupBy: &GroupBy{
			Tags:     []string{"host"},
			Interval: 5 * time.Minute,
		},
		OrderBy: &OrderBy{
			Field: "time",
			Desc:  true,
		},
		Limit: 100,
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be: Limit -> Sort -> Group -> Scan
	limit, ok := plan.(*LimitNode)
	if !ok {
		t.Fatalf("expected LimitNode at top, got %T", plan)
	}

	sort, ok := limit.Input.(*SortNode)
	if !ok {
		t.Fatalf("expected SortNode under Limit, got %T", limit.Input)
	}

	group, ok := sort.Input.(*GroupNode)
	if !ok {
		t.Fatalf("expected GroupNode under Sort, got %T", sort.Input)
	}

	scan, ok := group.Input.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode under Group, got %T", group.Input)
	}

	// Verify scan has time range and tag filter pushed down
	if scan.TimeRange.Start.IsZero() {
		t.Error("expected time range to be set on scan")
	}

	if scan.TagFilters["host"] != "server1" {
		t.Error("expected tag filter to be pushed to scan")
	}

	// Verify aggregates
	if len(group.Aggregates) != 2 {
		t.Errorf("expected 2 aggregates, got %d", len(group.Aggregates))
	}
}

func TestPlannerProjection(t *testing.T) {
	// SELECT usage, temp FROM cpu
	stmt := &SelectStatement{
		Fields: []*Field{
			{Expr: &Identifier{Name: "usage"}},
			{Expr: &Identifier{Name: "temp"}},
		},
		Measurement: "cpu",
	}

	planner := NewPlanner()
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	proj, ok := plan.(*ProjectNode)
	if !ok {
		t.Fatalf("expected ProjectNode, got %T", plan)
	}

	if len(proj.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(proj.Fields))
	}

	if proj.Fields[0] != "usage" || proj.Fields[1] != "temp" {
		t.Errorf("unexpected fields: %v", proj.Fields)
	}
}

func TestFormatPlan(t *testing.T) {
	plan := &LimitNode{
		Limit: 100,
		Input: &SortNode{
			Field: "time",
			Desc:  true,
			Input: &ScanNode{
				Measurement: "cpu",
				TagFilters:  map[string]string{"host": "server1"},
			},
		},
	}

	formatted := FormatPlan(plan, 0)

	// Verify structure is present
	if !strings.Contains(formatted, "Limit(100)") {
		t.Error("missing Limit in formatted plan")
	}
	if !strings.Contains(formatted, "Sort(time DESC)") {
		t.Error("missing Sort in formatted plan")
	}
	if !strings.Contains(formatted, "Scan(cpu") {
		t.Error("missing Scan in formatted plan")
	}
}

func TestTimeRangeContains(t *testing.T) {
	now := time.Now()
	tr := TimeRange{
		Start: now.Add(-time.Hour),
		End:   now,
	}

	// Within range
	if !tr.Contains(now.Add(-30 * time.Minute)) {
		t.Error("expected time within range")
	}

	// Before range
	if tr.Contains(now.Add(-2 * time.Hour)) {
		t.Error("expected time before range to be excluded")
	}

	// After range
	if tr.Contains(now.Add(time.Hour)) {
		t.Error("expected time after range to be excluded")
	}

	// Empty range contains everything
	empty := TimeRange{}
	if !empty.Contains(now) {
		t.Error("empty range should contain any time")
	}
}

func TestPlannerOptimize(t *testing.T) {
	// Create a plan with nested filters
	plan := &FilterNode{
		Predicate: &BinaryExpr{
			Left:  &Identifier{Name: "a"},
			Op:    GT,
			Right: &NumberLiteral{Value: 1},
		},
		Input: &FilterNode{
			Predicate: &BinaryExpr{
				Left:  &Identifier{Name: "b"},
				Op:    LT,
				Right: &NumberLiteral{Value: 10},
			},
			Input: &ScanNode{
				Measurement: "test",
				TagFilters:  map[string]string{},
			},
		},
	}

	planner := NewPlanner()
	optimized := planner.Optimize(plan)

	// Should merge into single filter
	filter, ok := optimized.(*FilterNode)
	if !ok {
		t.Fatalf("expected FilterNode, got %T", optimized)
	}

	// Predicate should be combined with AND
	binary, ok := filter.Predicate.(*BinaryExpr)
	if !ok || binary.Op != AND {
		t.Error("expected merged predicate with AND")
	}
}

func TestIsAggregateFunc(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		{"count", true},
		{"COUNT", true},
		{"sum", true},
		{"mean", true},
		{"min", true},
		{"max", true},
		{"first", true},
		{"last", true},
		{"median", true},
		{"percentile", true},
		{"stddev", true},
		{"upper", false},
		{"lower", false},
		{"custom", false},
	}

	for _, tt := range tests {
		if got := isAggregateFunc(tt.name); got != tt.expected {
			t.Errorf("isAggregateFunc(%q) = %v, want %v", tt.name, got, tt.expected)
		}
	}
}

func TestPlanNodeStrings(t *testing.T) {
	// Test ScanNode String
	scan := &ScanNode{
		Measurement: "cpu",
		TimeRange:   TimeRange{},
		TagFilters:  map[string]string{"host": "server1"},
	}
	s := scan.String()
	if !strings.Contains(s, "Scan") {
		t.Errorf("ScanNode.String() = %q, should contain 'Scan'", s)
	}

	// Test FilterNode
	filter := &FilterNode{
		Input:     scan,
		Predicate: &BooleanLiteral{Value: true},
	}
	fs := filter.String()
	if !strings.Contains(fs, "Filter") {
		t.Errorf("FilterNode.String() = %q, should contain 'Filter'", fs)
	}

	// Test GroupNode
	group := &GroupNode{
		Input: scan,
		Tags:  []string{"host"},
	}
	gs := group.String()
	if !strings.Contains(gs, "Group") {
		t.Errorf("GroupNode.String() = %q, should contain 'Group'", gs)
	}

	// Test ProjectNode
	project := &ProjectNode{
		Input:  scan,
		Fields: []string{"value"},
	}
	ps := project.String()
	if !strings.Contains(ps, "Project") {
		t.Errorf("ProjectNode.String() = %q, should contain 'Project'", ps)
	}

	// Test SortNode
	sortNode := &SortNode{
		Input: scan,
		Field: "time",
		Desc:  true,
	}
	ss := sortNode.String()
	if !strings.Contains(ss, "Sort") {
		t.Errorf("SortNode.String() = %q, should contain 'Sort'", ss)
	}

	// Test LimitNode
	limit := &LimitNode{
		Input:  scan,
		Limit:  10,
		Offset: 5,
	}
	ls := limit.String()
	if !strings.Contains(ls, "Limit") {
		t.Errorf("LimitNode.String() = %q, should contain 'Limit'", ls)
	}
}

func TestExtractTimeConditionComplex(t *testing.T) {
	planner := NewPlanner()

	// Test with time > X AND time < Y
	stmt, _ := ParseQuery("SELECT * FROM cpu WHERE time > now() - 1h AND time < now()")
	selectStmt := stmt.(*SelectStatement)
	
	plan, err := planner.Plan(selectStmt)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	
	_ = plan // Verify planning succeeds
}

func TestExtractAggregatesMultiple(t *testing.T) {
	planner := NewPlanner()

	stmt, _ := ParseQuery("SELECT mean(value), max(value), min(value) FROM cpu")
	selectStmt := stmt.(*SelectStatement)
	
	plan, err := planner.Plan(selectStmt)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	
	_ = plan
}

func TestPlanWithOrderBy(t *testing.T) {
	planner := NewPlanner()

	stmt, _ := ParseQuery("SELECT * FROM cpu ORDER BY time DESC")
	selectStmt := stmt.(*SelectStatement)
	
	plan, err := planner.Plan(selectStmt)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	
	_ = plan
}

func TestPlanWithLimitOffset(t *testing.T) {
	planner := NewPlanner()

	stmt, _ := ParseQuery("SELECT * FROM cpu LIMIT 10 OFFSET 5")
	selectStmt := stmt.(*SelectStatement)
	
	plan, err := planner.Plan(selectStmt)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	
	_ = plan
}

func TestPlannerOptimizations(t *testing.T) {
	planner := NewPlanner()

	// Test query that triggers pushdown
	stmt, _ := ParseQuery("SELECT value FROM cpu WHERE host = 'server1' AND value > 10")
	selectStmt := stmt.(*SelectStatement)
	
	plan, err := planner.Plan(selectStmt)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	_ = plan

	// Test query with complex filters
	stmt2, _ := ParseQuery("SELECT * FROM cpu WHERE (host = 'a' OR host = 'b') AND value > 0")
	selectStmt2 := stmt2.(*SelectStatement)
	
	plan2, err := planner.Plan(selectStmt2)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	_ = plan2
}

func TestTimeExprRecognition(t *testing.T) {
	planner := NewPlanner()

	// Query with time expression
	stmt, _ := ParseQuery("SELECT * FROM cpu WHERE time > now() - 1h")
	selectStmt := stmt.(*SelectStatement)
	
	plan, err := planner.Plan(selectStmt)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	_ = plan
}
