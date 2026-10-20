package circuitbreaker

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestRecentFailures(t *testing.T) {
	t.Parallel()

	t.Run("keeps the most recent failures of the run", func(t *testing.T) {
		t.Parallel()
		f := NewRecentFailures(2, 100)
		f.RecordFailure("first")
		f.RecordFailure("second")
		f.RecordFailure("third")

		messages, count := f.Take()
		require.Equal(t, []string{"second", "third"}, messages)
		require.Equal(t, 3, count)
	})

	t.Run("success ends the run", func(t *testing.T) {
		t.Parallel()
		f := NewRecentFailures(2, 100)
		f.RecordFailure("before success")
		f.RecordSuccess()
		f.RecordFailure("after success")

		messages, count := f.Take()
		require.Equal(t, []string{"after success"}, messages)
		require.Equal(t, 1, count)
	})

	t.Run("take empties the record", func(t *testing.T) {
		t.Parallel()
		f := NewRecentFailures(2, 100)
		f.RecordFailure("failure")
		f.Take()

		messages, count := f.Take()
		require.Empty(t, messages)
		require.Zero(t, count)
	})

	t.Run("truncates long messages to valid UTF-8", func(t *testing.T) {
		t.Parallel()
		f := NewRecentFailures(1, 5)
		f.RecordFailure("abcd" + strings.Repeat("é", 10))

		messages, _ := f.Take()
		require.Equal(t, []string{"abcd"}, messages)
		require.True(t, utf8.ValidString(messages[0]))
	})
}
