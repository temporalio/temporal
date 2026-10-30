package sqlplugin

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func TestDbFields_LastField_Version(t *testing.T) {
	lastField := DbFields[len(DbFields)-1]
	require.Equal(t, VersionColumnName, lastField)
}

func TestParseCountGroupByGroupValue(t *testing.T) {
	testCases := []struct {
		name      string
		fieldName string
		value     any
		expected  any
	}{
		{
			name:      "namespace division value",
			fieldName: sadefs.TemporalNamespaceDivision,
			value:     []byte("divisionA"),
			expected:  "divisionA",
		},
		{
			// Default-division workflows have a NULL division and must be
			// normalized to the empty string to match Elasticsearch's
			// Missing("") behavior.
			name:      "namespace division null",
			fieldName: sadefs.TemporalNamespaceDivision,
			value:     nil,
			expected:  "",
		},
		{
			// NULL for other fields is left untouched (out of scope).
			name:      "other field null passes through",
			fieldName: "SomeKeyword",
			value:     nil,
			expected:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCountGroupByGroupValue(tc.fieldName, tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

// fakeRows reports a fixed number of rows and then whatever Err returns, the
// way database/sql behaves when a read ends before the rows do: Next returns
// false and only Err separates that from a clean end.
type fakeRows struct {
	remaining int
	err       error
}

func (r *fakeRows) Next() bool {
	if r.remaining == 0 {
		return false
	}
	r.remaining--
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	for i := range dest {
		p, ok := dest[i].(*any)
		if !ok {
			return errors.New("unexpected scan destination")
		}
		*p = int64(1)
	}
	return nil
}

func (r *fakeRows) Close() error { return nil }

func (r *fakeRows) Err() error { return r.err }

func TestParseCountGroupByRows_TruncatedRead(t *testing.T) {
	errTruncated := errors.New("connection lost mid-iteration")
	rows := &fakeRows{remaining: 2, err: errTruncated}

	got, err := ParseCountGroupByRows(rows, []string{sadefs.ExecutionStatus})
	require.ErrorIs(t, err, errTruncated)
	require.Nil(t, got)
}

func TestParseCountGroupByRows_CompleteRead(t *testing.T) {
	rows := &fakeRows{remaining: 2}

	got, err := ParseCountGroupByRows(rows, []string{sadefs.ExecutionStatus})
	require.NoError(t, err)
	require.Len(t, got, 2)
}
