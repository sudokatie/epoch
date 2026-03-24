package query

import (
	"fmt"
	"strings"
	"time"
)

// DDL Statement Types

// CreateDatabaseStatement represents CREATE DATABASE
type CreateDatabaseStatement struct {
	Name        string
	IfNotExists bool
}

func (*CreateDatabaseStatement) stmt() {}
func (s *CreateDatabaseStatement) String() string {
	if s.IfNotExists {
		return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", s.Name)
	}
	return fmt.Sprintf("CREATE DATABASE %s", s.Name)
}

// DropDatabaseStatement represents DROP DATABASE
type DropDatabaseStatement struct {
	Name     string
	IfExists bool
}

func (*DropDatabaseStatement) stmt() {}
func (s *DropDatabaseStatement) String() string {
	if s.IfExists {
		return fmt.Sprintf("DROP DATABASE IF EXISTS %s", s.Name)
	}
	return fmt.Sprintf("DROP DATABASE %s", s.Name)
}

// ShowDatabasesStatement represents SHOW DATABASES
type ShowDatabasesStatement struct{}

func (*ShowDatabasesStatement) stmt()          {}
func (*ShowDatabasesStatement) String() string { return "SHOW DATABASES" }

// ShowMeasurementsStatement represents SHOW MEASUREMENTS
type ShowMeasurementsStatement struct {
	Database string
}

func (*ShowMeasurementsStatement) stmt() {}
func (s *ShowMeasurementsStatement) String() string {
	if s.Database != "" {
		return fmt.Sprintf("SHOW MEASUREMENTS ON %s", s.Database)
	}
	return "SHOW MEASUREMENTS"
}

// ShowTagKeysStatement represents SHOW TAG KEYS
type ShowTagKeysStatement struct {
	Database    string
	Measurement string
}

func (*ShowTagKeysStatement) stmt() {}
func (s *ShowTagKeysStatement) String() string {
	var sb strings.Builder
	sb.WriteString("SHOW TAG KEYS")
	if s.Database != "" {
		sb.WriteString(" ON ")
		sb.WriteString(s.Database)
	}
	if s.Measurement != "" {
		sb.WriteString(" FROM ")
		sb.WriteString(s.Measurement)
	}
	return sb.String()
}

// ShowFieldKeysStatement represents SHOW FIELD KEYS
type ShowFieldKeysStatement struct {
	Database    string
	Measurement string
}

func (*ShowFieldKeysStatement) stmt() {}
func (s *ShowFieldKeysStatement) String() string {
	var sb strings.Builder
	sb.WriteString("SHOW FIELD KEYS")
	if s.Database != "" {
		sb.WriteString(" ON ")
		sb.WriteString(s.Database)
	}
	if s.Measurement != "" {
		sb.WriteString(" FROM ")
		sb.WriteString(s.Measurement)
	}
	return sb.String()
}

// CreateRetentionPolicyStatement represents CREATE RETENTION POLICY
type CreateRetentionPolicyStatement struct {
	Name              string
	Database          string
	Duration          time.Duration
	ShardDuration     time.Duration
	ReplicationFactor int
	Default           bool
	IfNotExists       bool
}

func (*CreateRetentionPolicyStatement) stmt() {}
func (s *CreateRetentionPolicyStatement) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE RETENTION POLICY ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(s.Name)
	sb.WriteString(" ON ")
	sb.WriteString(s.Database)
	sb.WriteString(fmt.Sprintf(" DURATION %s", s.Duration))
	sb.WriteString(fmt.Sprintf(" REPLICATION %d", s.ReplicationFactor))
	if s.ShardDuration > 0 {
		sb.WriteString(fmt.Sprintf(" SHARD DURATION %s", s.ShardDuration))
	}
	if s.Default {
		sb.WriteString(" DEFAULT")
	}
	return sb.String()
}

// DropRetentionPolicyStatement represents DROP RETENTION POLICY
type DropRetentionPolicyStatement struct {
	Name     string
	Database string
}

func (*DropRetentionPolicyStatement) stmt() {}
func (s *DropRetentionPolicyStatement) String() string {
	return fmt.Sprintf("DROP RETENTION POLICY %s ON %s", s.Name, s.Database)
}

// ShowRetentionPoliciesStatement represents SHOW RETENTION POLICIES
type ShowRetentionPoliciesStatement struct {
	Database string
}

func (*ShowRetentionPoliciesStatement) stmt() {}
func (s *ShowRetentionPoliciesStatement) String() string {
	return fmt.Sprintf("SHOW RETENTION POLICIES ON %s", s.Database)
}

// CreateContinuousQueryStatement represents CREATE CONTINUOUS QUERY
type CreateContinuousQueryStatement struct {
	Name     string
	Database string
	Query    *SelectStatement
	Into     string
	Interval time.Duration
}

func (*CreateContinuousQueryStatement) stmt() {}
func (s *CreateContinuousQueryStatement) String() string {
	return fmt.Sprintf("CREATE CONTINUOUS QUERY %s ON %s BEGIN %s END",
		s.Name, s.Database, s.Query.String())
}

// DropContinuousQueryStatement represents DROP CONTINUOUS QUERY
type DropContinuousQueryStatement struct {
	Name     string
	Database string
}

func (*DropContinuousQueryStatement) stmt() {}
func (s *DropContinuousQueryStatement) String() string {
	return fmt.Sprintf("DROP CONTINUOUS QUERY %s ON %s", s.Name, s.Database)
}

// ShowContinuousQueriesStatement represents SHOW CONTINUOUS QUERIES
type ShowContinuousQueriesStatement struct{}

func (*ShowContinuousQueriesStatement) stmt()          {}
func (*ShowContinuousQueriesStatement) String() string { return "SHOW CONTINUOUS QUERIES" }

// DropMeasurementStatement represents DROP MEASUREMENT
type DropMeasurementStatement struct {
	Name string
}

func (*DropMeasurementStatement) stmt() {}
func (s *DropMeasurementStatement) String() string {
	return fmt.Sprintf("DROP MEASUREMENT %s", s.Name)
}

// Add new keywords for DDL
func init() {
	// Add DDL keywords to the keywords map
	ddlKeywords := map[string]Token{
		"CREATE":      CREATE,
		"DROP":        DROP,
		"DATABASE":    DATABASE,
		"DATABASES":   DATABASES,
		"MEASUREMENT": MEASUREMENT,
		"MEASUREMENTS": MEASUREMENTS,
		"RETENTION":   RETENTION,
		"POLICY":      POLICY,
		"POLICIES":    POLICIES,
		"CONTINUOUS":  CONTINUOUS,
		"QUERY":       QUERY,
		"DURATION":    DURATIONKW,
		"REPLICATION": REPLICATION,
		"SHARD":       SHARD,
		"DEFAULT":     DEFAULT,
		"IF":          IF,
		"EXISTS":      EXISTS,
		"SHOW":        SHOW,
		"ON":          ON,
		"INTO":        INTO,
		"BEGIN":       BEGIN,
		"END":         END,
		"TAG":         TAG,
		"FIELD":       FIELD,
		"KEYS":        KEYS,
	}

	for k, v := range ddlKeywords {
		keywords[k] = v
	}
}

// Additional Token constants for DDL
const (
	CREATE Token = iota + 100
	DROP
	DATABASE
	DATABASES
	MEASUREMENT
	MEASUREMENTS
	RETENTION
	POLICY
	POLICIES
	CONTINUOUS
	QUERY
	DURATIONKW
	REPLICATION
	SHARD
	DEFAULT
	IF
	EXISTS
	SHOW
	ON
	INTO
	BEGIN
	END
	TAG
	FIELD
	KEYS
)

func init() {
	// Add string representations for DDL tokens
	ddlTokenStrings := map[Token]string{
		CREATE:       "CREATE",
		DROP:         "DROP",
		DATABASE:     "DATABASE",
		DATABASES:    "DATABASES",
		MEASUREMENT:  "MEASUREMENT",
		MEASUREMENTS: "MEASUREMENTS",
		RETENTION:    "RETENTION",
		POLICY:       "POLICY",
		POLICIES:     "POLICIES",
		CONTINUOUS:   "CONTINUOUS",
		QUERY:        "QUERY",
		DURATIONKW:   "DURATION",
		REPLICATION:  "REPLICATION",
		SHARD:        "SHARD",
		DEFAULT:      "DEFAULT",
		IF:           "IF",
		EXISTS:       "EXISTS",
		SHOW:         "SHOW",
		ON:           "ON",
		INTO:         "INTO",
		BEGIN:        "BEGIN",
		END:          "END",
		TAG:          "TAG",
		FIELD:        "FIELD",
		KEYS:         "KEYS",
	}

	for k, v := range ddlTokenStrings {
		tokenStrings[k] = v
	}
}
