package postgresql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRebind(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		query string
		out   string
	}{
		{
			name:  "no placeholder",
			query: "SELECT * FROM executions_visibility",
			out:   "SELECT * FROM executions_visibility",
		},
		{
			name:  "single placeholder",
			query: "SELECT * FROM executions_visibility WHERE namespace_id = ?",
			out:   "SELECT * FROM executions_visibility WHERE namespace_id = $1",
		},
		{
			name:  "multiple placeholders",
			query: "SELECT * FROM executions_visibility WHERE namespace_id = ? AND run_id = ? LIMIT ?",
			out:   "SELECT * FROM executions_visibility WHERE namespace_id = $1 AND run_id = $2 LIMIT $3",
		},
		{
			name:  "question mark inside string literal is not a placeholder",
			query: "SELECT * FROM executions_visibility WHERE workflow_id = 'foo?-value' LIMIT ?",
			out:   "SELECT * FROM executions_visibility WHERE workflow_id = 'foo?-value' LIMIT $1",
		},
		{
			name:  "placeholders around a string literal are numbered consecutively",
			query: "SELECT * FROM executions_visibility WHERE namespace_id = ? AND workflow_id = '?' LIMIT ?",
			out:   "SELECT * FROM executions_visibility WHERE namespace_id = $1 AND workflow_id = '?' LIMIT $2",
		},
		{
			name:  "multiple question marks inside one string literal",
			query: "SELECT * FROM executions_visibility WHERE workflow_id = '??a?' LIMIT ?",
			out:   "SELECT * FROM executions_visibility WHERE workflow_id = '??a?' LIMIT $1",
		},
		{
			name:  "escaped quote inside string literal",
			query: "SELECT * FROM executions_visibility WHERE workflow_id = 'it''s a ? value' LIMIT ?",
			out:   "SELECT * FROM executions_visibility WHERE workflow_id = 'it''s a ? value' LIMIT $1",
		},
		{
			name:  "question mark between two adjacent string literals is a placeholder",
			query: "SELECT * FROM executions_visibility WHERE workflow_id = 'a' AND run_id = ? AND task_queue = 'b'",
			out:   "SELECT * FROM executions_visibility WHERE workflow_id = 'a' AND run_id = $1 AND task_queue = 'b'",
		},
		{
			name:  "question mark inside a json containment literal",
			query: "SELECT * FROM executions_visibility WHERE KeywordList01 @> jsonb_build_array('foo?') LIMIT ?",
			out:   "SELECT * FROM executions_visibility WHERE KeywordList01 @> jsonb_build_array('foo?') LIMIT $1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.out, rebind(tc.query))
		})
	}
}
