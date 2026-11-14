package schema

import (
	"fmt"
	"io/fs"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgreSQLVisibilityGeneratedColumnGrouping(t *testing.T) {
	type column struct {
		name     string
		dataType string
		operator string
	}

	deploymentColumns := []column{
		{"TemporalWorkerDeploymentVersion", "VARCHAR(255)", "->>"},
		{"TemporalWorkflowVersioningBehavior", "VARCHAR(255)", "->>"},
		{"TemporalWorkerDeployment", "VARCHAR(255)", "->>"},
	}

	tests := []struct {
		name    string
		file    string
		columns []column
		indexes []string
	}{
		{
			name: "v1.7",
			file: "v1.7/add_pause_info_search_attribute.sql",
			columns: append(
				[]column{{"TemporalPauseInfo", "JSONB", "->"}},
				deploymentColumns...,
			),
			indexes: []string{"by_temporal_pause_info"},
		},
		{
			name:    "v1.8",
			file:    "v1.8/add_deployment_search_attributes.sql",
			columns: deploymentColumns,
			indexes: []string{
				"by_temporal_worker_deployment_version",
				"by_temporal_workflow_versioning_behavior",
				"by_temporal_worker_deployment",
			},
		},
	}

	base := "postgresql/v12/visibility/versioned/"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := fs.ReadFile(Assets(), base+tc.file)
			require.NoError(t, err)
			sql := string(data)

			alter := regexp.MustCompile(
				`(?i)\bALTER\s+TABLE\s+executions_visibility\b`,
			)
			require.Len(t, alter.FindAllString(sql, -1), 1,
				"generated columns must share one ALTER TABLE")

			add := regexp.MustCompile(`(?i)\bADD\s+COLUMN\b`)
			require.Len(t, add.FindAllString(sql, -1), len(tc.columns))

			for _, col := range tc.columns {
				pattern := fmt.Sprintf(
					`(?is)\bADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+%s\s+%s\s+GENERATED\s+ALWAYS\s+AS\s*\(\s*search_attributes\s*%s\s*'%s'\s*\)\s+STORED\b`,
					regexp.QuoteMeta(col.name),
					regexp.QuoteMeta(col.dataType),
					regexp.QuoteMeta(col.operator),
					regexp.QuoteMeta(col.name),
				)
				require.Regexp(t, pattern, sql,
					"incorrect generated-column definition: %s", col.name)
			}

			createIndex := regexp.MustCompile(`(?i)\bCREATE\s+INDEX\b`)
			require.Len(t, createIndex.FindAllString(sql, -1), len(tc.indexes))

			for _, name := range tc.indexes {
				pattern := `(?i)\bCREATE\s+INDEX\s+` +
					regexp.QuoteMeta(name) + `\b`
				require.Regexp(t, pattern, sql)
			}
		})
	}
}
