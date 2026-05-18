package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
)

func TestResolveNow(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, 3, 31, 17, 0, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		input   string
		output  string
		wantErr string
	}{
		{
			name:   "plain now",
			input:  "StartTime > NOW()",
			output: "StartTime > '2026-03-31T17:00:00Z'",
		},
		{
			name:   "lowercase now()",
			input:  "StartTime > now()",
			output: "StartTime > '2026-03-31T17:00:00Z'",
		},
		{
			name:   "now minus quoted duration",
			input:  "StartTime > NOW() - '5h'",
			output: "StartTime > '2026-03-31T12:00:00Z'",
		},
		{
			name:   "mixed case Now() with quoted duration",
			input:  "StartTime > Now() - '5h'",
			output: "StartTime > '2026-03-31T12:00:00Z'",
		},
		{
			name:   "now plus quoted compound duration",
			input:  "CloseTime < NOW() + '1h30m'",
			output: "CloseTime < '2026-03-31T18:30:00Z'",
		},
		{
			// sqlparser.String lowercases AND
			name:   "quoted literal untouched, AND lowercased in output",
			input:  "WorkflowType = 'NOW() - 5h' AND StartTime > NOW()",
			output: "WorkflowType = 'NOW() - 5h' and StartTime > '2026-03-31T17:00:00Z'",
		},
		{
			// weeks are not supported; use days instead (same as ExecutionDuration syntax)
			name:   "days",
			input:  "StartTime > NOW() - '9d'",
			output: "StartTime > '2026-03-22T17:00:00Z'",
		},
		{
			name:   "fractional hours",
			input:  "StartTime > NOW() - '1.5h'",
			output: "StartTime > '2026-03-31T15:30:00Z'",
		},
		{
			name:   "no NOW() passthrough",
			input:  "WorkflowType = 'myWorkflow'",
			output: "WorkflowType = 'myWorkflow'",
		},
		{
			name:    "invalid duration",
			input:   "StartTime > NOW() - 'h'",
			wantErr: "invalid NOW() duration",
		},
		{
			name:    "unquoted duration rejected by parser",
			input:   "StartTime > NOW() - 5h",
			wantErr: "malformed",
		},
		{
			name:   "microseconds with ASCII us",
			input:  "StartTime > NOW() - '500us'",
			output: "StartTime > '2026-03-31T16:59:59.9995Z'",
		},
		{
			// time.ParseDuration is case-sensitive; uppercase units are not supported
			name:    "uppercase unit rejected",
			input:   "StartTime > NOW() - '5H'",
			wantErr: "invalid NOW() duration",
		},
		{
			name:   "between with now",
			input:  "StartTime BETWEEN NOW() - '1h' AND NOW()",
			output: "StartTime between '2026-03-31T16:00:00Z' and '2026-03-31T17:00:00Z'",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := ResolveNow(tc.input, anchor)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.output, out)
		})
	}
}

func TestResolveNowInExpr(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, 3, 31, 17, 0, 0, 0, time.UTC)

	parseExpr := func(t *testing.T, whereClause string) sqlparser.Expr {
		t.Helper()
		stmt, err := sqlparser.Parse("select * from dummy where " + whereClause)
		require.NoError(t, err)
		return stmt.(*sqlparser.Select).Where.Expr
	}

	t.Run("substitutes NOW() in comparison", func(t *testing.T) {
		t.Parallel()
		expr := parseExpr(t, "StartTime > NOW()")
		require.NoError(t, ResolveNowInExpr(&expr, anchor))
		require.Equal(t, "StartTime > '2026-03-31T17:00:00Z'", sqlparser.String(expr))
	})

	t.Run("substitutes NOW() - 'duration' in comparison", func(t *testing.T) {
		t.Parallel()
		expr := parseExpr(t, "StartTime > NOW() - '5h'")
		require.NoError(t, ResolveNowInExpr(&expr, anchor))
		require.Equal(t, "StartTime > '2026-03-31T12:00:00Z'", sqlparser.String(expr))
	})

	t.Run("substitutes both sides of AND", func(t *testing.T) {
		t.Parallel()
		expr := parseExpr(t, "StartTime > NOW() - '1h' AND CloseTime < NOW() + '1h'")
		require.NoError(t, ResolveNowInExpr(&expr, anchor))
		require.Equal(t, "StartTime > '2026-03-31T16:00:00Z' and CloseTime < '2026-03-31T18:00:00Z'", sqlparser.String(expr))
	})

	t.Run("leaves non-NOW expressions untouched", func(t *testing.T) {
		t.Parallel()
		expr := parseExpr(t, "WorkflowType = 'myWorkflow'")
		require.NoError(t, ResolveNowInExpr(&expr, anchor))
		require.Equal(t, "WorkflowType = 'myWorkflow'", sqlparser.String(expr))
	})

	t.Run("quoted literal with NOW text untouched", func(t *testing.T) {
		t.Parallel()
		expr := parseExpr(t, "WorkflowType = 'NOW() - 5h'")
		require.NoError(t, ResolveNowInExpr(&expr, anchor))
		require.Equal(t, "WorkflowType = 'NOW() - 5h'", sqlparser.String(expr))
	})

	t.Run("returns error on invalid duration", func(t *testing.T) {
		t.Parallel()
		expr := parseExpr(t, "StartTime > NOW() - '5y'")
		err := ResolveNowInExpr(&expr, anchor)
		require.ErrorContains(t, err, "invalid NOW() duration")
	})
}
