package query

import (
	"fmt"
	"strings"
	"time"
)

// Plan represents a query execution plan
type Plan interface {
	plan()
	String() string
	Children() []Plan
}

// ScanNode reads data from a measurement
type ScanNode struct {
	Measurement string
	TimeRange   TimeRange
	TagFilters  map[string]string
}

func (*ScanNode) plan() {}
func (s *ScanNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Scan(%s", s.Measurement))
	if !s.TimeRange.IsZero() {
		sb.WriteString(fmt.Sprintf(", time=[%v, %v]", s.TimeRange.Start, s.TimeRange.End))
	}
	if len(s.TagFilters) > 0 {
		sb.WriteString(", tags={")
		first := true
		for k, v := range s.TagFilters {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s=%s", k, v))
			first = false
		}
		sb.WriteString("}")
	}
	sb.WriteString(")")
	return sb.String()
}
func (*ScanNode) Children() []Plan { return nil }

// FilterNode applies a predicate to filter rows
type FilterNode struct {
	Input     Plan
	Predicate Expr
}

func (*FilterNode) plan() {}
func (f *FilterNode) String() string {
	return fmt.Sprintf("Filter(%s)", f.Predicate.String())
}
func (f *FilterNode) Children() []Plan { return []Plan{f.Input} }

// GroupNode performs GROUP BY aggregation
type GroupNode struct {
	Input      Plan
	Tags       []string
	Interval   time.Duration
	Aggregates []*AggregateExpr
}

func (*GroupNode) plan() {}
func (g *GroupNode) String() string {
	var parts []string
	if g.Interval > 0 {
		parts = append(parts, fmt.Sprintf("time(%s)", g.Interval))
	}
	parts = append(parts, g.Tags...)
	aggs := make([]string, len(g.Aggregates))
	for i, a := range g.Aggregates {
		aggs[i] = a.String()
	}
	return fmt.Sprintf("Group(by=[%s], aggs=[%s])", strings.Join(parts, ", "), strings.Join(aggs, ", "))
}
func (g *GroupNode) Children() []Plan { return []Plan{g.Input} }

// AggregateExpr represents an aggregate function in a plan
type AggregateExpr struct {
	Func  string
	Field string
	Alias string
}

func (a *AggregateExpr) String() string {
	s := fmt.Sprintf("%s(%s)", a.Func, a.Field)
	if a.Alias != "" {
		s += " AS " + a.Alias
	}
	return s
}

// ProjectNode selects specific fields from the result
type ProjectNode struct {
	Input  Plan
	Fields []string
}

func (*ProjectNode) plan() {}
func (p *ProjectNode) String() string {
	return fmt.Sprintf("Project(%s)", strings.Join(p.Fields, ", "))
}
func (p *ProjectNode) Children() []Plan { return []Plan{p.Input} }

// SortNode orders results by a field
type SortNode struct {
	Input Plan
	Field string
	Desc  bool
}

func (*SortNode) plan() {}
func (s *SortNode) String() string {
	dir := "ASC"
	if s.Desc {
		dir = "DESC"
	}
	return fmt.Sprintf("Sort(%s %s)", s.Field, dir)
}
func (s *SortNode) Children() []Plan { return []Plan{s.Input} }

// LimitNode restricts the number of results
type LimitNode struct {
	Input  Plan
	Limit  int
	Offset int
}

func (*LimitNode) plan() {}
func (l *LimitNode) String() string {
	if l.Offset > 0 {
		return fmt.Sprintf("Limit(%d, offset=%d)", l.Limit, l.Offset)
	}
	return fmt.Sprintf("Limit(%d)", l.Limit)
}
func (l *LimitNode) Children() []Plan { return []Plan{l.Input} }

// TimeRange represents a time bounds for a query
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// IsZero returns true if the time range is unset
func (tr TimeRange) IsZero() bool {
	return tr.Start.IsZero() && tr.End.IsZero()
}

// Contains checks if a timestamp is within the range
func (tr TimeRange) Contains(t time.Time) bool {
	if !tr.Start.IsZero() && t.Before(tr.Start) {
		return false
	}
	if !tr.End.IsZero() && t.After(tr.End) {
		return false
	}
	return true
}

// FormatPlan returns a formatted string representation of the plan tree
func FormatPlan(p Plan, indent int) string {
	var sb strings.Builder
	prefix := strings.Repeat("  ", indent)
	sb.WriteString(prefix)
	sb.WriteString(p.String())
	sb.WriteString("\n")
	for _, child := range p.Children() {
		sb.WriteString(FormatPlan(child, indent+1))
	}
	return sb.String()
}
