package batcher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAllNamespacesQuery(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "system search attributes", query: "ExecutionStatus='Running' AND StartTime < '2026-11-20T00:00:00Z'"},
		{name: "predefined search attribute", query: "TemporalNamespaceDivision IS NULL"},
		{name: "reserved aliases", query: "TemporalScheduleId='s' AND TemporalBuildIds='build' AND TemporalWorkflowId='w'"},
		{name: "ambiguous schedule alias", query: "ScheduleId='s'", wantErr: "got: ScheduleId"},
		{name: "ambiguous predefined alias", query: "NamespaceDivision IS NULL", wantErr: "got: NamespaceDivision"},
		{name: "empty query", query: "", wantErr: "requires a visibility query"},
		{name: "whitespace-only query", query: " \t", wantErr: "requires a visibility query"},
		{name: "custom search attribute", query: "CustomerId='abc'", wantErr: "got: CustomerId"},
		{name: "raw custom field name", query: "WorkflowType='w' OR Keyword01='x'", wantErr: "got: Keyword01"},
		{name: "namespace ID", query: "NamespaceId='id'", wantErr: "got: NamespaceId"},
		{name: "unparsable query", query: "WorkflowType=", wantErr: "invalid query"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateAllNamespacesQuery(tc.query)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
