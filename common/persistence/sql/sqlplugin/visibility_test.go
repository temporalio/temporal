package sqlplugin

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type testDBRows struct {
	rows         [][]any
	current      int
	iterationErr error
	failAfter    int
}

func (r *testDBRows) Next() bool {
	if r.iterationErr != nil && r.current == r.failAfter {
		return false
	}
	return r.current < len(r.rows)
}

func (r *testDBRows) Scan(dest ...any) error {
	for i, value := range r.rows[r.current] {
		*(dest[i].(*any)) = value
	}
	r.current++
	return nil
}

func (r *testDBRows) Err() error {
	return r.iterationErr
}

func (r *testDBRows) Close() error {
	return nil
}

func TestDbFields_LastField_Version(t *testing.T) {
	lastField := DbFields[len(DbFields)-1]
	require.Equal(t, VersionColumnName, lastField)
}

func TestParseCountGroupByRows(t *testing.T) {
	rows := &testDBRows{
		rows: [][]any{
			{[]byte("workflow-type"), int64(3)},
			{[]byte("other-workflow-type"), int64(5)},
		},
	}

	result, err := ParseCountGroupByRows(rows, []string{sadefs.WorkflowType})

	require.NoError(t, err)
	require.Equal(t, []VisibilityCountRow{
		{GroupValues: []any{"workflow-type"}, Count: 3},
		{GroupValues: []any{"other-workflow-type"}, Count: 5},
	}, result)
}

func TestParseCountGroupByRows_IterationError(t *testing.T) {
	iterationErr := errors.New("row iteration failed")
	rows := &testDBRows{
		rows: [][]any{
			{[]byte("workflow-type"), int64(3)},
			{[]byte("other-workflow-type"), int64(5)},
		},
		iterationErr: iterationErr,
		failAfter:    1,
	}

	result, err := ParseCountGroupByRows(rows, []string{sadefs.WorkflowType})

	require.ErrorIs(t, err, iterationErr)
	require.Nil(t, result)
}
