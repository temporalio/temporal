package workers

import (
	"fmt"
	"strings"
	"testing"
)

func TestGetWhereCause(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
		errorMsg    string
		// For successful cases, we'll validate the expression type
		expectedExprType string
	}{
		{
			name:             "Valid simple WHERE clause",
			query:            "SELECT * FROM table1 WHERE WorkerInstanceKey = 'test'",
			expectError:      false,
			expectedExprType: "*sqlparser.ComparisonExpr",
		},
		{
			name:             "Valid WHERE clause with parentheses",
			query:            "SELECT * FROM table1 WHERE (WorkerInstanceKey = 'test' AND TaskQueue = 'queue1')",
			expectError:      false,
			expectedExprType: "*sqlparser.ParenExpr",
		},
		{
			name:             "Valid WHERE clause with string functions",
			query:            "SELECT * FROM table1 WHERE WorkerIdentity STARTS_WITH 'worker'",
			expectError:      false,
			expectedExprType: "*sqlparser.ComparisonExpr",
		},
		{
			name:        "Missing WHERE clause",
			query:       "SELECT * FROM table1",
			expectError: true,
			errorMsg:    "where' clause is missing",
		},
		{
			name:        "Non-SELECT statement - INSERT",
			query:       "INSERT INTO table1 VALUES ('test')",
			expectError: true,
			errorMsg:    "statement must be 'select'",
		},
		{
			name:        "Query with LIMIT clause",
			query:       "SELECT * FROM table1 WHERE WorkerInstanceKey = 'test' LIMIT 10",
			expectError: true,
			errorMsg:    "limit' clause",
		},
		{
			name:        "Query with LIMIT and OFFSET",
			query:       "SELECT * FROM table1 WHERE WorkerInstanceKey = 'test' LIMIT 10 OFFSET 5",
			expectError: true,
			errorMsg:    "limit' clause",
		},
		{
			name:        "Empty query",
			query:       "",
			expectError: true,
			errorMsg:    malformedSqlQueryErrMessage,
		},
		{
			name:        "Whitespace only query",
			query:       "   \n\t  ",
			expectError: true,
			errorMsg:    malformedSqlQueryErrMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := getWhereCause(tt.query)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %s", tt.errorMsg, err.Error())
				}
				if expr != nil {
					t.Errorf("Expected nil expression when error occurs, got: %T", expr)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}
				if expr == nil {
					t.Errorf("Expected non-nil expression, got nil")
					return
				}

				// Check the expression type matches expected
				actualType := fmt.Sprintf("%T", expr)
				if actualType != tt.expectedExprType {
					t.Errorf("Expected expression type %s, got %s", tt.expectedExprType, actualType)
				}
			}
		})
	}
}

// Test edge cases and specific SQL parser behaviors
func TestGetWhereCause_EdgeCases(t *testing.T) {
	t.Run("Very long query", func(t *testing.T) {
		// Test with a very long but valid query
		var conditions []string
		for i := 0; i < 100; i++ {
			conditions = append(conditions, fmt.Sprintf("WorkerInstanceKey = 'worker%d'", i))
		}
		query := fmt.Sprintf("SELECT * FROM table1 WHERE %s", strings.Join(conditions, " OR "))

		expr, err := getWhereCause(query)
		if err != nil {
			t.Errorf("Unexpected error for long query: %v", err)
		}
		if expr == nil {
			t.Errorf("Expected non-nil expression for long query")
		}
	})

	t.Run("Query with comments", func(t *testing.T) {
		query := "SELECT * FROM table1 WHERE WorkerInstanceKey = 'test' /* comment */"
		expr, err := getWhereCause(query)
		if err != nil {
			t.Errorf("Unexpected error for query with comments: %v", err)
		}
		if expr == nil {
			t.Errorf("Expected non-nil expression for query with comments")
		}
	})

	t.Run("Case sensitivity in SQL keywords", func(t *testing.T) {
		queries := []string{
			"select * from table1 where WorkerInstanceKey = 'test'",
			"Select * From table1 Where WorkerInstanceKey = 'test'",
			"SELECT * FROM table1 WHERE WorkerInstanceKey = 'test'",
		}

		for i, query := range queries {
			t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
				expr, err := getWhereCause(query)
				if err != nil {
					t.Errorf("Unexpected error for query with different case: %v", err)
				}
				if expr == nil {
					t.Errorf("Expected non-nil expression for query with different case")
				}
			})
		}
	})

	t.Run("Query with extra whitespace", func(t *testing.T) {
		query := "  SELECT   *   FROM   table1   WHERE   WorkerInstanceKey   =   'test'  "
		expr, err := getWhereCause(query)
		if err != nil {
			t.Errorf("Unexpected error for query with extra whitespace: %v", err)
		}
		if expr == nil {
			t.Errorf("Expected non-nil expression for query with extra whitespace")
		}
	})
}

// Benchmark the function with different query complexities
func BenchmarkGetWhereCause(b *testing.B) {
	queries := map[string]string{
		"Simple":  "SELECT * FROM table1 WHERE WorkerInstanceKey = 'test'",
		"Complex": "SELECT * FROM table1 WHERE (WorkerInstanceKey = 'test' OR TaskQueue = 'queue1') AND LastHeartbeatTime > '2023-10-27T10:30:00Z'",
		"Range":   "SELECT * FROM table1 WHERE StartTime BETWEEN '2023-01-01' AND '2023-12-31'",
	}

	for name, query := range queries {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := getWhereCause(query)
				if err != nil {
					b.Fatalf("Unexpected error: %v", err)
				}
			}
		})
	}
}
