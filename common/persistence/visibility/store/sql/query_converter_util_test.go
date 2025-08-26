package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_tokenizeTextQueryString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "empty",
			input: "",
			want:  []string{},
		},
		{
			name:  "single token",
			input: "foo",
			want:  []string{"foo"},
		},
		{
			name:  "two tokens",
			input: "foo bar",
			want:  []string{"foo", "bar"},
		},
		{
			name:  "multiple spaces collapsed",
			input: "a  b   c",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "leading and trailing spaces",
			input: "  foo   bar  ",
			want:  []string{"foo", "bar"},
		},
		{
			name:  "spaces only",
			input: "    ",
			want:  []string{},
		},
		{
			name:  "tabs are not separators",
			input: "foo\tbar baz",
			want:  []string{"foo\tbar", "baz"},
		},
		{
			name:  "newlines are not separators",
			input: "foo\nbar baz",
			want:  []string{"foo\nbar", "baz"},
		},
		{
			name:  "simple comparison with quotes",
			input: "status = \"RUNNING\"",
			want:  []string{"status", "=", "\"RUNNING\""},
		},
		{
			name:  "comparison without spaces around operator",
			input: "status='RUNNING' AND execution_time > 2025-01-01",
			want:  []string{"status='RUNNING'", "AND", "execution_time", ">", "2025-01-01"},
		},
		{
			name:  "quoted string with internal space",
			input: "WorkflowId = 'abc 123' AND RunId = 'xyz'",
			want:  []string{"WorkflowId", "=", "'abc", "123'", "AND", "RunId", "=", "'xyz'"},
		},
		{
			name:  "IS NULL and datetime literal",
			input: "CloseTime IS NULL OR CloseTime > '2024-01-01T00:00:00Z'",
			want:  []string{"CloseTime", "IS", "NULL", "OR", "CloseTime", ">", "'2024-01-01T00:00:00Z'"},
		},
		{
			name:  "LIKE pattern",
			input: "name LIKE '%abc%'",
			want:  []string{"name", "LIKE", "'%abc%'"},
		},
		{
			name:  "parentheses and operators",
			input: "(status = 'FAILED') AND (attempts >= 3)",
			want:  []string{"(status", "=", "'FAILED')", "AND", "(attempts", ">=", "3)"},
		},
		{
			name:  "IN with list including spaced item",
			input: "NamespaceId in ('ns1','ns 2','ns3')",
			want:  []string{"NamespaceId", "in", "('ns1','ns", "2','ns3')"},
		},
		{
			name:  "json containment operator",
			input: "attr_json @> '{\"key\":\"value\"}'",
			want:  []string{"attr_json", "@>", "'{\"key\":\"value\"}'"},
		},
		{
			name:  "combined IN and comparison",
			input: "execution_status IN ('RUNNING','COMPLETED') AND startTime >= 2025-01-01T00:00:00Z",
			want:  []string{"execution_status", "IN", "('RUNNING','COMPLETED')", "AND", "startTime", ">=", "2025-01-01T00:00:00Z"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := tokenizeTextQueryString(test.input)
			require.Equal(t, test.want, got)
		})
	}
}
