package query

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestUnsafeSQLString(t *testing.T) {
	t.Parallel()
	val := NewUnsafeSQLString("foo")
	require.Equal(t, `'foo'`, sqlparser.String(val))

	// chars are not escaped
	val = NewUnsafeSQLString("fo'o")
	require.Equal(t, `'fo'o'`, sqlparser.String(val))
}

func TestColName(t *testing.T) {
	t.Parallel()
	col := NewColName("foo")
	require.Equal(t, `foo`, sqlparser.String(col))

	col = NewColName("foo123")
	require.Equal(t, `foo123`, sqlparser.String(col))
}

func TestSAColName(t *testing.T) {
	t.Parallel()
	col := NewSAColumn("Alias", "Field", enumspb.INDEXED_VALUE_TYPE_BOOL)
	require.Equal(t, "Field", sqlparser.String(col))

	// system search attribute have special mapping
	col = NewSAColumn("NamespaceId", "NamespaceId", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	require.Equal(t, "namespace_id", sqlparser.String(col))
}

func TestNewFuncExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		fnName string
		exprs  []sqlparser.Expr
		want   string
	}{
		{
			name:   "no args",
			fnName: "FN_NAME",
			want:   "FN_NAME()",
		},
		{
			name:   "one literal arg",
			fnName: "FN_NAME",
			exprs:  []sqlparser.Expr{sqlparser.NewStrVal([]byte("foo"))},
			want:   "FN_NAME('foo')",
		},
		{
			name:   "one col arg",
			fnName: "FN_NAME",
			exprs:  []sqlparser.Expr{NamespaceIDSAColumn},
			want:   "FN_NAME(namespace_id)",
		},
		{
			name:   "multiple args",
			fnName: "FN_NAME",
			exprs:  []sqlparser.Expr{NamespaceIDSAColumn, sqlparser.NewIntVal([]byte("123"))},
			want:   "FN_NAME(namespace_id, 123)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := NewFuncExpr(tc.fnName, tc.exprs...)
			require.Equal(t, tc.want, sqlparser.String(got))
		})
	}
}

func TestTokenizeTextQueryString(t *testing.T) {
	t.Parallel()
	testCases := []struct {
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := TokenizeTextQueryString(tc.input)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestGetUnsafeStringTupleValues(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name  string
		input sqlparser.ValTuple
		want  []string
		err   string
	}{
		{
			name: "success",
			input: sqlparser.ValTuple{
				NewUnsafeSQLString("foo"),
				NewUnsafeSQLString("bar"),
			},
			want: []string{"foo", "bar"},
		},
		{
			name: "fail",
			input: sqlparser.ValTuple{
				NewUnsafeSQLString("foo"),
				sqlparser.NewIntVal([]byte("123")),
			},
			err: fmt.Sprintf(
				"%s: unexpected value type in tuple (expected string, got 123)",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotErr := GetUnsafeStringTupleValues(tc.input)
			if tc.err != "" {
				require.Error(t, gotErr)
				require.ErrorContains(t, gotErr, tc.err)
			} else {
				require.NoError(t, gotErr)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestParseExecutionDurationStr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		input         string
		expectedValue time.Duration
		expectedErr   error
	}{
		{
			input:         "123",
			expectedValue: time.Duration(123),
		},
		{
			input:         "123s",
			expectedValue: 123 * time.Second,
		},
		{
			input:         "123m",
			expectedValue: 123 * time.Minute,
		},
		{
			input:         "123h",
			expectedValue: 123 * time.Hour,
		},
		{
			input:         "123d",
			expectedValue: 123 * 24 * time.Hour,
		},
		{
			input:         "01:02:03",
			expectedValue: 1*time.Hour + 2*time.Minute + 3*time.Second,
		},
		{
			input:       "01:60:03",
			expectedErr: errors.New("invalid duration"),
		},
		{
			input:       "123q",
			expectedErr: errors.New("invalid duration"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseExecutionDurationStr(tc.input)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, tc.expectedValue, got)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			}
		})
	}
}

func TestReduceExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name       string
		reduceFunc func(left, right sqlparser.Expr) sqlparser.Expr
		exprs      []sqlparser.Expr
		want       sqlparser.Expr
	}{
		{
			name:  "nil",
			exprs: nil,
		},
		{
			name:  "empty",
			exprs: []sqlparser.Expr{},
		},
		{
			name:  "one item",
			exprs: []sqlparser.Expr{sqlparser.NewStrVal([]byte("foo"))},
			want:  sqlparser.NewStrVal([]byte("foo")),
		},
		{
			name: "two items",
			reduceFunc: func(left, right sqlparser.Expr) sqlparser.Expr {
				return &sqlparser.OrExpr{Left: left, Right: right}
			},
			exprs: []sqlparser.Expr{
				sqlparser.NewStrVal([]byte("foo")),
				sqlparser.NewStrVal([]byte("bar")),
			},
			want: &sqlparser.OrExpr{
				Left:  sqlparser.NewStrVal([]byte("foo")),
				Right: sqlparser.NewStrVal([]byte("bar")),
			},
		},
		{
			name: "multiple items",
			reduceFunc: func(left, right sqlparser.Expr) sqlparser.Expr {
				return &sqlparser.OrExpr{Left: left, Right: right}
			},
			exprs: []sqlparser.Expr{
				sqlparser.NewStrVal([]byte("1")),
				sqlparser.NewStrVal([]byte("2")),
				sqlparser.NewStrVal([]byte("3")),
				sqlparser.NewStrVal([]byte("4")),
				sqlparser.NewStrVal([]byte("5")),
			},
			want: &sqlparser.OrExpr{
				Left: &sqlparser.OrExpr{
					Left: &sqlparser.OrExpr{
						Left: &sqlparser.OrExpr{
							Left:  sqlparser.NewStrVal([]byte("1")),
							Right: sqlparser.NewStrVal([]byte("2")),
						},
						Right: sqlparser.NewStrVal([]byte("3")),
					},
					Right: sqlparser.NewStrVal([]byte("4")),
				},
				Right: sqlparser.NewStrVal([]byte("5")),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ReduceExprs(tc.reduceFunc, tc.exprs...)
			require.Equal(t, tc.want, got)
		})
	}
}
