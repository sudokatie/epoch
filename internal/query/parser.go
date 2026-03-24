package query

import (
	"fmt"
	"strconv"
	"time"
)

// Parser parses InfluxQL-style queries
type Parser struct {
	lexer *Lexer
	tok   Token
	lit   string
}

// NewParser creates a new parser
func NewParser(input string) *Parser {
	p := &Parser{lexer: NewLexer(input)}
	p.next()
	return p
}

func (p *Parser) next() {
	p.tok, p.lit = p.lexer.NextToken()
}

func (p *Parser) expect(tok Token) error {
	if p.tok != tok {
		return fmt.Errorf("expected %v, got %v (%q)", tok, p.tok, p.lit)
	}
	p.next()
	return nil
}

// Parse parses the input and returns a statement
func (p *Parser) Parse() (Statement, error) {
	switch p.tok {
	case SELECT:
		return p.parseSelect()
	case CREATE:
		return p.parseCreate()
	case DROP:
		return p.parseDrop()
	case SHOW:
		return p.parseShow()
	default:
		return nil, fmt.Errorf("unexpected token: %v", p.tok)
	}
}

func (p *Parser) parseCreate() (Statement, error) {
	p.next() // skip CREATE

	switch p.tok {
	case DATABASE:
		return p.parseCreateDatabase()
	case RETENTION:
		return p.parseCreateRetentionPolicy()
	case CONTINUOUS:
		return p.parseCreateContinuousQuery()
	default:
		return nil, fmt.Errorf("expected DATABASE, RETENTION, or CONTINUOUS after CREATE, got %v", p.tok)
	}
}

func (p *Parser) parseCreateDatabase() (Statement, error) {
	p.next() // skip DATABASE

	stmt := &CreateDatabaseStatement{}

	// Optional IF NOT EXISTS
	if p.tok == IF {
		p.next()
		if p.tok != NOT {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		p.next()
		if err := p.expect(EXISTS); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected database name")
	}
	stmt.Name = p.lit
	p.next()

	return stmt, nil
}

func (p *Parser) parseCreateRetentionPolicy() (Statement, error) {
	p.next() // skip RETENTION
	if err := p.expect(POLICY); err != nil {
		return nil, err
	}

	stmt := &CreateRetentionPolicyStatement{
		ReplicationFactor: 1, // default
	}

	// Optional IF NOT EXISTS
	if p.tok == IF {
		p.next()
		if p.tok != NOT {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		p.next()
		if err := p.expect(EXISTS); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	// Policy name
	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected policy name")
	}
	stmt.Name = p.lit
	p.next()

	// ON database
	if err := p.expect(ON); err != nil {
		return nil, err
	}
	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected database name")
	}
	stmt.Database = p.lit
	p.next()

	// Parse clauses in any order
	for p.tok != EOF && p.tok != SEMICOLON {
		switch p.tok {
		case DURATIONKW:
			p.next()
			if p.tok != DURATION && p.tok != NUMBER {
				return nil, fmt.Errorf("expected duration value")
			}
			dur, err := ParseDuration(p.lit)
			if err != nil {
				return nil, err
			}
			stmt.Duration = time.Duration(dur)
			p.next()

		case REPLICATION:
			p.next()
			if p.tok != NUMBER {
				return nil, fmt.Errorf("expected replication factor")
			}
			rf, err := strconv.Atoi(p.lit)
			if err != nil {
				return nil, err
			}
			stmt.ReplicationFactor = rf
			p.next()

		case SHARD:
			p.next()
			if err := p.expect(DURATIONKW); err != nil {
				return nil, err
			}
			if p.tok != DURATION && p.tok != NUMBER {
				return nil, fmt.Errorf("expected shard duration")
			}
			dur, err := ParseDuration(p.lit)
			if err != nil {
				return nil, err
			}
			stmt.ShardDuration = time.Duration(dur)
			p.next()

		case DEFAULT:
			stmt.Default = true
			p.next()

		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

func (p *Parser) parseCreateContinuousQuery() (Statement, error) {
	p.next() // skip CONTINUOUS
	if err := p.expect(QUERY); err != nil {
		return nil, err
	}

	stmt := &CreateContinuousQueryStatement{}

	// Query name
	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected continuous query name")
	}
	stmt.Name = p.lit
	p.next()

	// ON database
	if err := p.expect(ON); err != nil {
		return nil, err
	}
	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected database name")
	}
	stmt.Database = p.lit
	p.next()

	// BEGIN
	if err := p.expect(BEGIN); err != nil {
		return nil, err
	}

	// Parse the SELECT statement
	selectStmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	stmt.Query = selectStmt

	// END
	if err := p.expect(END); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *Parser) parseDrop() (Statement, error) {
	p.next() // skip DROP

	switch p.tok {
	case DATABASE:
		return p.parseDropDatabase()
	case RETENTION:
		return p.parseDropRetentionPolicy()
	case CONTINUOUS:
		return p.parseDropContinuousQuery()
	case MEASUREMENT:
		return p.parseDropMeasurement()
	default:
		return nil, fmt.Errorf("expected DATABASE, RETENTION, CONTINUOUS, or MEASUREMENT after DROP, got %v", p.tok)
	}
}

func (p *Parser) parseDropDatabase() (Statement, error) {
	p.next() // skip DATABASE

	stmt := &DropDatabaseStatement{}

	// Optional IF EXISTS
	if p.tok == IF {
		p.next()
		if err := p.expect(EXISTS); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected database name")
	}
	stmt.Name = p.lit
	p.next()

	return stmt, nil
}

func (p *Parser) parseDropRetentionPolicy() (Statement, error) {
	p.next() // skip RETENTION
	if err := p.expect(POLICY); err != nil {
		return nil, err
	}

	stmt := &DropRetentionPolicyStatement{}

	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected policy name")
	}
	stmt.Name = p.lit
	p.next()

	if err := p.expect(ON); err != nil {
		return nil, err
	}

	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected database name")
	}
	stmt.Database = p.lit
	p.next()

	return stmt, nil
}

func (p *Parser) parseDropContinuousQuery() (Statement, error) {
	p.next() // skip CONTINUOUS
	if err := p.expect(QUERY); err != nil {
		return nil, err
	}

	stmt := &DropContinuousQueryStatement{}

	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected continuous query name")
	}
	stmt.Name = p.lit
	p.next()

	if err := p.expect(ON); err != nil {
		return nil, err
	}

	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected database name")
	}
	stmt.Database = p.lit
	p.next()

	return stmt, nil
}

func (p *Parser) parseDropMeasurement() (Statement, error) {
	p.next() // skip MEASUREMENT

	stmt := &DropMeasurementStatement{}

	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected measurement name")
	}
	stmt.Name = p.lit
	p.next()

	return stmt, nil
}

func (p *Parser) parseShow() (Statement, error) {
	p.next() // skip SHOW

	switch p.tok {
	case DATABASES:
		p.next()
		return &ShowDatabasesStatement{}, nil

	case MEASUREMENTS:
		return p.parseShowMeasurements()

	case TAG:
		return p.parseShowTagKeys()

	case FIELD:
		return p.parseShowFieldKeys()

	case RETENTION:
		return p.parseShowRetentionPolicies()

	case CONTINUOUS:
		p.next()
		if err := p.expect(QUERY); err != nil {
			// Try QUERIES
			if p.tok == IDENT && p.lit == "QUERIES" {
				p.next()
			}
		}
		return &ShowContinuousQueriesStatement{}, nil

	default:
		return nil, fmt.Errorf("expected DATABASES, MEASUREMENTS, TAG, FIELD, RETENTION, or CONTINUOUS after SHOW, got %v", p.tok)
	}
}

func (p *Parser) parseShowMeasurements() (Statement, error) {
	p.next() // skip MEASUREMENTS

	stmt := &ShowMeasurementsStatement{}

	if p.tok == ON {
		p.next()
		if p.tok != IDENT && p.tok != STRING {
			return nil, fmt.Errorf("expected database name")
		}
		stmt.Database = p.lit
		p.next()
	}

	return stmt, nil
}

func (p *Parser) parseShowTagKeys() (Statement, error) {
	p.next() // skip TAG
	if err := p.expect(KEYS); err != nil {
		return nil, err
	}

	stmt := &ShowTagKeysStatement{}

	if p.tok == ON {
		p.next()
		if p.tok != IDENT && p.tok != STRING {
			return nil, fmt.Errorf("expected database name")
		}
		stmt.Database = p.lit
		p.next()
	}

	if p.tok == FROM {
		p.next()
		if p.tok != IDENT && p.tok != STRING {
			return nil, fmt.Errorf("expected measurement name")
		}
		stmt.Measurement = p.lit
		p.next()
	}

	return stmt, nil
}

func (p *Parser) parseShowFieldKeys() (Statement, error) {
	p.next() // skip FIELD
	if err := p.expect(KEYS); err != nil {
		return nil, err
	}

	stmt := &ShowFieldKeysStatement{}

	if p.tok == ON {
		p.next()
		if p.tok != IDENT && p.tok != STRING {
			return nil, fmt.Errorf("expected database name")
		}
		stmt.Database = p.lit
		p.next()
	}

	if p.tok == FROM {
		p.next()
		if p.tok != IDENT && p.tok != STRING {
			return nil, fmt.Errorf("expected measurement name")
		}
		stmt.Measurement = p.lit
		p.next()
	}

	return stmt, nil
}

func (p *Parser) parseShowRetentionPolicies() (Statement, error) {
	p.next() // skip RETENTION
	if err := p.expect(POLICIES); err != nil {
		return nil, err
	}

	stmt := &ShowRetentionPoliciesStatement{}

	if p.tok == ON {
		p.next()
		if p.tok != IDENT && p.tok != STRING {
			return nil, fmt.Errorf("expected database name")
		}
		stmt.Database = p.lit
		p.next()
	}

	return stmt, nil
}

func (p *Parser) parseSelect() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// SELECT
	if err := p.expect(SELECT); err != nil {
		return nil, err
	}

	// Fields
	fields, err := p.parseFields()
	if err != nil {
		return nil, err
	}
	stmt.Fields = fields

	// FROM
	if err := p.expect(FROM); err != nil {
		return nil, err
	}

	// Measurement
	if p.tok != IDENT && p.tok != STRING {
		return nil, fmt.Errorf("expected measurement name, got %v", p.tok)
	}
	stmt.Measurement = p.lit
	p.next()

	// Optional WHERE
	if p.tok == WHERE {
		p.next()
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Condition = cond
	}

	// Optional GROUP BY
	if p.tok == GROUP {
		p.next()
		if err := p.expect(BY); err != nil {
			return nil, err
		}
		groupBy, err := p.parseGroupBy()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
	}

	// Optional ORDER BY
	if p.tok == ORDER {
		p.next()
		if err := p.expect(BY); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// Optional LIMIT
	if p.tok == LIMIT {
		p.next()
		if p.tok != NUMBER {
			return nil, fmt.Errorf("expected number after LIMIT")
		}
		limit, err := strconv.Atoi(p.lit)
		if err != nil {
			return nil, err
		}
		stmt.Limit = limit
		p.next()
	}

	// Optional OFFSET
	if p.tok == OFFSET {
		p.next()
		if p.tok != NUMBER {
			return nil, fmt.Errorf("expected number after OFFSET")
		}
		offset, err := strconv.Atoi(p.lit)
		if err != nil {
			return nil, err
		}
		stmt.Offset = offset
		p.next()
	}

	return stmt, nil
}

func (p *Parser) parseFields() ([]*Field, error) {
	var fields []*Field

	for {
		field, err := p.parseField()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)

		if p.tok != COMMA {
			break
		}
		p.next() // skip comma
	}

	return fields, nil
}

func (p *Parser) parseField() (*Field, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	field := &Field{Expr: expr}

	// Optional AS alias
	if p.tok == AS {
		p.next()
		if p.tok != IDENT {
			return nil, fmt.Errorf("expected alias name")
		}
		field.Alias = p.lit
		p.next()
	}

	return field, nil
}

func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() (Expr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.tok == OR {
		op := p.tok
		p.next()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAndExpr() (Expr, error) {
	left, err := p.parseComparisonExpr()
	if err != nil {
		return nil, err
	}

	for p.tok == AND {
		op := p.tok
		p.next()
		right, err := p.parseComparisonExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseComparisonExpr() (Expr, error) {
	left, err := p.parseAddExpr()
	if err != nil {
		return nil, err
	}

	switch p.tok {
	case EQ, NEQ, LT, LTE, GT, GTE:
		op := p.tok
		p.next()
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil
	}

	return left, nil
}

func (p *Parser) parseAddExpr() (Expr, error) {
	left, err := p.parseMulExpr()
	if err != nil {
		return nil, err
	}

	for p.tok == PLUS || p.tok == MINUS {
		op := p.tok
		p.next()
		right, err := p.parseMulExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseMulExpr() (Expr, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	for p.tok == MUL || p.tok == DIV {
		op := p.tok
		p.next()
		right, err := p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseUnaryExpr() (Expr, error) {
	if p.tok == MINUS {
		p.next()
		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Left:  &NumberLiteral{Value: 0},
			Op:    MINUS,
			Right: expr,
		}, nil
	}

	return p.parsePrimaryExpr()
}

func (p *Parser) parsePrimaryExpr() (Expr, error) {
	switch p.tok {
	case MUL:
		p.next()
		return &Wildcard{}, nil

	case IDENT, TIME:
		name := p.lit
		p.next()

		// Check for function call
		if p.tok == LPAREN {
			return p.parseCall(name)
		}

		return &Identifier{Name: name}, nil

	case STRING:
		value := p.lit
		p.next()
		return &StringLiteral{Value: value}, nil

	case NUMBER:
		value, err := strconv.ParseFloat(p.lit, 64)
		if err != nil {
			return nil, err
		}
		isInt := !contains(p.lit, '.')
		p.next()
		return &NumberLiteral{Value: value, IsInt: isInt}, nil

	case DURATION:
		dur, err := ParseDuration(p.lit)
		if err != nil {
			return nil, err
		}
		p.next()
		return &DurationLiteral{Value: time.Duration(dur)}, nil

	case TRUE:
		p.next()
		return &BooleanLiteral{Value: true}, nil

	case FALSE:
		p.next()
		return &BooleanLiteral{Value: false}, nil

	case NOW:
		p.next()
		if p.tok == LPAREN {
			p.next()
			if err := p.expect(RPAREN); err != nil {
				return nil, err
			}
		}
		return &NowExpr{}, nil

	case LPAREN:
		p.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(RPAREN); err != nil {
			return nil, err
		}
		return &ParenExpr{Expr: expr}, nil

	default:
		return nil, fmt.Errorf("unexpected token in expression: %v (%q)", p.tok, p.lit)
	}
}

func (p *Parser) parseCall(name string) (Expr, error) {
	p.next() // skip LPAREN

	var args []Expr
	if p.tok != RPAREN {
		for {
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)

			if p.tok != COMMA {
				break
			}
			p.next()
		}
	}

	if err := p.expect(RPAREN); err != nil {
		return nil, err
	}

	return &Call{Name: name, Args: args}, nil
}

func (p *Parser) parseGroupBy() (*GroupBy, error) {
	groupBy := &GroupBy{}

	for {
		if p.tok == TIME {
			p.next()
			if err := p.expect(LPAREN); err != nil {
				return nil, err
			}
			if p.tok != DURATION && p.tok != NUMBER {
				return nil, fmt.Errorf("expected duration in time()")
			}
			dur, err := ParseDuration(p.lit)
			if err != nil {
				return nil, err
			}
			groupBy.Interval = time.Duration(dur)
			p.next()
			if err := p.expect(RPAREN); err != nil {
				return nil, err
			}
		} else if p.tok == IDENT {
			groupBy.Tags = append(groupBy.Tags, p.lit)
			p.next()
		} else {
			break
		}

		if p.tok != COMMA {
			break
		}
		p.next()
	}

	return groupBy, nil
}

func (p *Parser) parseOrderBy() (*OrderBy, error) {
	orderBy := &OrderBy{}

	if p.tok == IDENT || p.tok == TIME {
		orderBy.Field = p.lit
		p.next()
	} else {
		return nil, fmt.Errorf("expected field name in ORDER BY")
	}

	if p.tok == ASC {
		orderBy.Desc = false
		p.next()
	} else if p.tok == DESC {
		orderBy.Desc = true
		p.next()
	}

	return orderBy, nil
}

func contains(s string, c byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return true
		}
	}
	return false
}

// ParseQuery is a convenience function to parse a query string
func ParseQuery(input string) (Statement, error) {
	return NewParser(input).Parse()
}
