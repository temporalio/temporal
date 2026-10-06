package queues

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
)

type taggedTaskError struct{}

func (taggedTaskError) Error() string {
	return "tagged task error"
}

func (taggedTaskError) LogTags() []tag.Tag {
	return []tag.Tag{
		tag.String("test-task-tag", "test-task-value"),
	}
}

func TestTaskErrorLogTags(t *testing.T) {
	t.Parallel()

	tags := taskErrorLogTags(fmt.Errorf("wrapped: %w", taggedTaskError{}))
	require.Len(t, tags, 1)
	require.Equal(t, "test-task-tag", tags[0].Key())
	require.Equal(t, "test-task-value", tags[0].Value())

	require.Empty(t, taskErrorLogTags(errors.New("plain error")))
}
