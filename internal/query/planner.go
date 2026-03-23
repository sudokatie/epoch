package query

import (
	"time"
)

// Planner converts AST to execution plan
type Planner struct{}

// NewPlanner creates a new query planner
func NewPlanner() *Planner {
	return &Planner{}
}

// Plan converts a SELECT statement to an execution plan
func (p *Planner) Plan(stmt *SelectStatement) (Plan, error) {
	// Step 1: Create base scan node
	scan := &ScanNode{
		Measurement: stmt.Measurement,
		TagFilters:  make(map[string]string),
	}

	// Step 2: Extract time range and tag filters from WHERE clause
	var remainingCondition Expr
	if stmt.Condition != nil {
		timeRange, tagFilters, remaining := p.extractFilters(stmt.Condition)
		scan.TimeRange = timeRange
		scan.TagFilters = tagFilters
		remainingCondition = remaining
	}

	// Build the plan tree
	var plan Plan = scan

	// Step 3: Add filter node for remaining predicates
	if remainingCondition != nil {
		plan = &FilterNode{
			Input:     plan,
			Predicate: remainingCondition,
		}
	}

	// Step 4: Handle GROUP BY with aggregations
	aggregates := p.extractAggregates(stmt.Fields)
	if len(aggregates) > 0 || stmt.GroupBy != nil {
		groupNode := &GroupNode{
			Input:      plan,
			Aggregates: aggregates,
		}
		if stmt.GroupBy != nil {
			groupNode.Tags = stmt.GroupBy.Tags
			groupNode.Interval = stmt.GroupBy.Interval
		}
		plan = groupNode
	}

	// Step 5: Add projection if not SELECT *
	if !p.isSelectAll(stmt.Fields) && len(aggregates) == 0 {
		fields := p.extractFieldNames(stmt.Fields)
		plan = &ProjectNode{
			Input:  plan,
			Fields: fields,
		}
	}

	// Step 6: Add ORDER BY
	if stmt.OrderBy != nil {
		plan = &SortNode{
			Input: plan,
			Field: stmt.OrderBy.Field,
			Desc:  stmt.OrderBy.Desc,
		}
	}

	// Step 7: Add LIMIT/OFFSET
	if stmt.Limit > 0 {
		plan = &LimitNode{
			Input:  plan,
			Limit:  stmt.Limit,
			Offset: stmt.Offset,
		}
	}

	return plan, nil
}

// extractFilters extracts time ranges and tag filters from a WHERE clause
// Returns: time range, tag filters, remaining condition
func (p *Planner) extractFilters(expr Expr) (TimeRange, map[string]string, Expr) {
	var timeRange TimeRange
	tagFilters := make(map[string]string)
	remaining := p.extractFiltersRecursive(expr, &timeRange, tagFilters)
	return timeRange, tagFilters, remaining
}

func (p *Planner) extractFiltersRecursive(expr Expr, timeRange *TimeRange, tagFilters map[string]string) Expr {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *BinaryExpr:
		// Handle AND: recursively process both sides
		if e.Op == AND {
			left := p.extractFiltersRecursive(e.Left, timeRange, tagFilters)
			right := p.extractFiltersRecursive(e.Right, timeRange, tagFilters)
			if left == nil && right == nil {
				return nil
			}
			if left == nil {
				return right
			}
			if right == nil {
				return left
			}
			return &BinaryExpr{Left: left, Op: AND, Right: right}
		}

		// Check for time conditions
		if p.isTimeExpr(e.Left) {
			if tr := p.extractTimeCondition(e); tr != nil {
				if e.Op == GT || e.Op == GTE {
					timeRange.Start = *tr
				} else if e.Op == LT || e.Op == LTE {
					timeRange.End = *tr
				}
				return nil // Consumed by time range
			}
		}

		// Check for tag equality: tag = 'value'
		if e.Op == EQ {
			if ident, ok := e.Left.(*Identifier); ok {
				if str, ok := e.Right.(*StringLiteral); ok {
					// Assume any equality with string literal is a tag filter
					// In a real implementation, we'd check against schema
					tagFilters[ident.Name] = str.Value
					return nil // Consumed by tag filter
				}
			}
		}

		return expr

	case *ParenExpr:
		inner := p.extractFiltersRecursive(e.Expr, timeRange, tagFilters)
		if inner == nil {
			return nil
		}
		return &ParenExpr{Expr: inner}

	default:
		return expr
	}
}

// isTimeExpr checks if an expression refers to the time column
func (p *Planner) isTimeExpr(expr Expr) bool {
	if ident, ok := expr.(*Identifier); ok {
		return ident.Name == "time"
	}
	return false
}

// extractTimeCondition extracts a time value from a comparison
func (p *Planner) extractTimeCondition(e *BinaryExpr) *time.Time {
	// Handle: time > now() - 1h
	switch right := e.Right.(type) {
	case *BinaryExpr:
		// time > now() - 1h
		if right.Op == MINUS {
			if _, ok := right.Left.(*NowExpr); ok {
				if dur, ok := right.Right.(*DurationLiteral); ok {
					t := time.Now().Add(-dur.Value)
					return &t
				}
			}
		} else if right.Op == PLUS {
			if _, ok := right.Left.(*NowExpr); ok {
				if dur, ok := right.Right.(*DurationLiteral); ok {
					t := time.Now().Add(dur.Value)
					return &t
				}
			}
		}
	case *NowExpr:
		t := time.Now()
		return &t
	case *TimeLiteral:
		return &right.Value
	case *NumberLiteral:
		// Assume nanosecond timestamp
		t := time.Unix(0, int64(right.Value))
		return &t
	}
	return nil
}

// extractAggregates extracts aggregate functions from SELECT fields
func (p *Planner) extractAggregates(fields []*Field) []*AggregateExpr {
	var aggregates []*AggregateExpr
	for _, f := range fields {
		if call, ok := f.Expr.(*Call); ok {
			if isAggregateFunc(call.Name) {
				agg := &AggregateExpr{
					Func:  call.Name,
					Alias: f.Alias,
				}
				if len(call.Args) > 0 {
					if ident, ok := call.Args[0].(*Identifier); ok {
						agg.Field = ident.Name
					} else if _, ok := call.Args[0].(*Wildcard); ok {
						agg.Field = "*"
					}
				}
				if agg.Alias == "" {
					agg.Alias = call.String()
				}
				aggregates = append(aggregates, agg)
			}
		}
	}
	return aggregates
}

// isAggregateFunc checks if a function name is an aggregate
func isAggregateFunc(name string) bool {
	switch name {
	case "count", "COUNT", "sum", "SUM", "mean", "MEAN",
		"min", "MIN", "max", "MAX", "first", "FIRST",
		"last", "LAST", "median", "MEDIAN", "percentile", "PERCENTILE",
		"stddev", "STDDEV":
		return true
	}
	return false
}

// isSelectAll checks if the query is SELECT *
func (p *Planner) isSelectAll(fields []*Field) bool {
	if len(fields) == 1 {
		if _, ok := fields[0].Expr.(*Wildcard); ok {
			return true
		}
	}
	return false
}

// extractFieldNames extracts field names from SELECT fields
func (p *Planner) extractFieldNames(fields []*Field) []string {
	var names []string
	for _, f := range fields {
		switch e := f.Expr.(type) {
		case *Identifier:
			names = append(names, e.Name)
		case *Wildcard:
			names = append(names, "*")
		}
	}
	return names
}

// Optimize applies optimizations to a plan
func (p *Planner) Optimize(plan Plan) Plan {
	// Optimization 1: Push filters down to scan
	plan = p.pushDownFilters(plan)

	// Optimization 2: Eliminate redundant projections
	plan = p.eliminateRedundantProjections(plan)

	return plan
}

// pushDownFilters pushes filter predicates closer to the scan
func (p *Planner) pushDownFilters(plan Plan) Plan {
	switch n := plan.(type) {
	case *FilterNode:
		// If child is also a filter, merge them
		if child, ok := n.Input.(*FilterNode); ok {
			merged := &BinaryExpr{
				Left:  child.Predicate,
				Op:    AND,
				Right: n.Predicate,
			}
			return p.pushDownFilters(&FilterNode{
				Input:     child.Input,
				Predicate: merged,
			})
		}
		// Recurse
		n.Input = p.pushDownFilters(n.Input)
		return n

	case *ProjectNode:
		n.Input = p.pushDownFilters(n.Input)
		return n

	case *GroupNode:
		n.Input = p.pushDownFilters(n.Input)
		return n

	case *SortNode:
		n.Input = p.pushDownFilters(n.Input)
		return n

	case *LimitNode:
		n.Input = p.pushDownFilters(n.Input)
		return n

	default:
		return plan
	}
}

// eliminateRedundantProjections removes unnecessary projection nodes
func (p *Planner) eliminateRedundantProjections(plan Plan) Plan {
	switch n := plan.(type) {
	case *ProjectNode:
		// If projecting *, it's redundant
		if len(n.Fields) == 1 && n.Fields[0] == "*" {
			return p.eliminateRedundantProjections(n.Input)
		}
		n.Input = p.eliminateRedundantProjections(n.Input)
		return n

	case *FilterNode:
		n.Input = p.eliminateRedundantProjections(n.Input)
		return n

	case *GroupNode:
		n.Input = p.eliminateRedundantProjections(n.Input)
		return n

	case *SortNode:
		n.Input = p.eliminateRedundantProjections(n.Input)
		return n

	case *LimitNode:
		n.Input = p.eliminateRedundantProjections(n.Input)
		return n

	default:
		return plan
	}
}
