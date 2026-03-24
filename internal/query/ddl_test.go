package query

import (
	"strings"
	"testing"
	"time"
)

func TestParseCreateDatabase(t *testing.T) {
	tests := []struct {
		input   string
		name    string
		ifNotEx bool
		wantErr bool
	}{
		{"CREATE DATABASE mydb", "mydb", false, false},
		{"CREATE DATABASE IF NOT EXISTS mydb", "mydb", true, false},
		{"CREATE DATABASE \"my-db\"", "my-db", false, false},
		{"CREATE DATABASE", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := ParseQuery(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			create, ok := stmt.(*CreateDatabaseStatement)
			if !ok {
				t.Fatalf("expected CreateDatabaseStatement, got %T", stmt)
			}

			if create.Name != tt.name {
				t.Errorf("name = %q, want %q", create.Name, tt.name)
			}
			if create.IfNotExists != tt.ifNotEx {
				t.Errorf("ifNotExists = %v, want %v", create.IfNotExists, tt.ifNotEx)
			}
		})
	}
}

func TestParseDropDatabase(t *testing.T) {
	tests := []struct {
		input    string
		name     string
		ifExists bool
		wantErr  bool
	}{
		{"DROP DATABASE mydb", "mydb", false, false},
		{"DROP DATABASE IF EXISTS mydb", "mydb", true, false},
		{"DROP DATABASE", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := ParseQuery(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			drop, ok := stmt.(*DropDatabaseStatement)
			if !ok {
				t.Fatalf("expected DropDatabaseStatement, got %T", stmt)
			}

			if drop.Name != tt.name {
				t.Errorf("name = %q, want %q", drop.Name, tt.name)
			}
			if drop.IfExists != tt.ifExists {
				t.Errorf("ifExists = %v, want %v", drop.IfExists, tt.ifExists)
			}
		})
	}
}

func TestParseShowDatabases(t *testing.T) {
	stmt, err := ParseQuery("SHOW DATABASES")
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}

	_, ok := stmt.(*ShowDatabasesStatement)
	if !ok {
		t.Fatalf("expected ShowDatabasesStatement, got %T", stmt)
	}
}

func TestParseShowMeasurements(t *testing.T) {
	tests := []struct {
		input    string
		database string
	}{
		{"SHOW MEASUREMENTS", ""},
		{"SHOW MEASUREMENTS ON mydb", "mydb"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			show, ok := stmt.(*ShowMeasurementsStatement)
			if !ok {
				t.Fatalf("expected ShowMeasurementsStatement, got %T", stmt)
			}

			if show.Database != tt.database {
				t.Errorf("database = %q, want %q", show.Database, tt.database)
			}
		})
	}
}

func TestParseShowTagKeys(t *testing.T) {
	tests := []struct {
		input       string
		database    string
		measurement string
	}{
		{"SHOW TAG KEYS", "", ""},
		{"SHOW TAG KEYS ON mydb", "mydb", ""},
		{"SHOW TAG KEYS FROM cpu", "", "cpu"},
		{"SHOW TAG KEYS ON mydb FROM cpu", "mydb", "cpu"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			show, ok := stmt.(*ShowTagKeysStatement)
			if !ok {
				t.Fatalf("expected ShowTagKeysStatement, got %T", stmt)
			}

			if show.Database != tt.database {
				t.Errorf("database = %q, want %q", show.Database, tt.database)
			}
			if show.Measurement != tt.measurement {
				t.Errorf("measurement = %q, want %q", show.Measurement, tt.measurement)
			}
		})
	}
}

func TestParseShowFieldKeys(t *testing.T) {
	tests := []struct {
		input       string
		database    string
		measurement string
	}{
		{"SHOW FIELD KEYS", "", ""},
		{"SHOW FIELD KEYS ON mydb", "mydb", ""},
		{"SHOW FIELD KEYS FROM cpu", "", "cpu"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			show, ok := stmt.(*ShowFieldKeysStatement)
			if !ok {
				t.Fatalf("expected ShowFieldKeysStatement, got %T", stmt)
			}

			if show.Database != tt.database {
				t.Errorf("database = %q, want %q", show.Database, tt.database)
			}
		})
	}
}

func TestParseCreateRetentionPolicy(t *testing.T) {
	tests := []struct {
		input     string
		name      string
		database  string
		duration  time.Duration
		replFactor int
		isDefault bool
	}{
		{
			"CREATE RETENTION POLICY rp1 ON mydb DURATION 7d REPLICATION 1",
			"rp1", "mydb", 7 * 24 * time.Hour, 1, false,
		},
		{
			"CREATE RETENTION POLICY rp2 ON mydb DURATION 30d REPLICATION 2 DEFAULT",
			"rp2", "mydb", 30 * 24 * time.Hour, 2, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			create, ok := stmt.(*CreateRetentionPolicyStatement)
			if !ok {
				t.Fatalf("expected CreateRetentionPolicyStatement, got %T", stmt)
			}

			if create.Name != tt.name {
				t.Errorf("name = %q, want %q", create.Name, tt.name)
			}
			if create.Database != tt.database {
				t.Errorf("database = %q, want %q", create.Database, tt.database)
			}
			if create.Duration != tt.duration {
				t.Errorf("duration = %v, want %v", create.Duration, tt.duration)
			}
			if create.ReplicationFactor != tt.replFactor {
				t.Errorf("replication = %d, want %d", create.ReplicationFactor, tt.replFactor)
			}
			if create.Default != tt.isDefault {
				t.Errorf("default = %v, want %v", create.Default, tt.isDefault)
			}
		})
	}
}

func TestParseDropRetentionPolicy(t *testing.T) {
	stmt, err := ParseQuery("DROP RETENTION POLICY rp1 ON mydb")
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}

	drop, ok := stmt.(*DropRetentionPolicyStatement)
	if !ok {
		t.Fatalf("expected DropRetentionPolicyStatement, got %T", stmt)
	}

	if drop.Name != "rp1" {
		t.Errorf("name = %q, want %q", drop.Name, "rp1")
	}
	if drop.Database != "mydb" {
		t.Errorf("database = %q, want %q", drop.Database, "mydb")
	}
}

func TestParseShowRetentionPolicies(t *testing.T) {
	stmt, err := ParseQuery("SHOW RETENTION POLICIES ON mydb")
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}

	show, ok := stmt.(*ShowRetentionPoliciesStatement)
	if !ok {
		t.Fatalf("expected ShowRetentionPoliciesStatement, got %T", stmt)
	}

	if show.Database != "mydb" {
		t.Errorf("database = %q, want %q", show.Database, "mydb")
	}
}

func TestParseDropMeasurement(t *testing.T) {
	stmt, err := ParseQuery("DROP MEASUREMENT cpu")
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}

	drop, ok := stmt.(*DropMeasurementStatement)
	if !ok {
		t.Fatalf("expected DropMeasurementStatement, got %T", stmt)
	}

	if drop.Name != "cpu" {
		t.Errorf("name = %q, want %q", drop.Name, "cpu")
	}
}

func TestDDLStatementStrings(t *testing.T) {
	tests := []struct {
		stmt Statement
		want string
	}{
		{&CreateDatabaseStatement{Name: "mydb"}, "CREATE DATABASE mydb"},
		{&CreateDatabaseStatement{Name: "mydb", IfNotExists: true}, "CREATE DATABASE IF NOT EXISTS mydb"},
		{&DropDatabaseStatement{Name: "mydb"}, "DROP DATABASE mydb"},
		{&DropDatabaseStatement{Name: "mydb", IfExists: true}, "DROP DATABASE IF EXISTS mydb"},
		{&ShowDatabasesStatement{}, "SHOW DATABASES"},
		{&ShowMeasurementsStatement{}, "SHOW MEASUREMENTS"},
		{&ShowMeasurementsStatement{Database: "mydb"}, "SHOW MEASUREMENTS ON mydb"},
		{&ShowTagKeysStatement{}, "SHOW TAG KEYS"},
		{&ShowFieldKeysStatement{}, "SHOW FIELD KEYS"},
		{&DropMeasurementStatement{Name: "cpu"}, "DROP MEASUREMENT cpu"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.stmt.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDDLStringsWithOptions(t *testing.T) {
	// ShowTagKeysStatement with options
	showTags := &ShowTagKeysStatement{
		Database:    "mydb",
		Measurement: "cpu",
	}
	s := showTags.String()
	if !strings.Contains(s, "mydb") || !strings.Contains(s, "cpu") {
		t.Errorf("ShowTagKeysStatement.String() = %q, missing db or measurement", s)
	}

	// ShowFieldKeysStatement with options
	showFields := &ShowFieldKeysStatement{
		Database:    "mydb",
		Measurement: "cpu",
	}
	sf := showFields.String()
	if !strings.Contains(sf, "mydb") || !strings.Contains(sf, "cpu") {
		t.Errorf("ShowFieldKeysStatement.String() = %q, missing db or measurement", sf)
	}

	// CreateRetentionPolicyStatement with shard duration
	crp := &CreateRetentionPolicyStatement{
		Name:              "mypolicy",
		Database:          "mydb",
		Duration:          24 * time.Hour,
		ShardDuration:     1 * time.Hour,
		ReplicationFactor: 2,
		Default:           true,
		IfNotExists:       true,
	}
	crps := crp.String()
	if !strings.Contains(crps, "SHARD") {
		t.Errorf("CreateRetentionPolicyStatement.String() = %q, missing SHARD", crps)
	}
	if !strings.Contains(crps, "DEFAULT") {
		t.Errorf("CreateRetentionPolicyStatement.String() = %q, missing DEFAULT", crps)
	}
}
