package api_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc/codes"
)

func TestGetTaskCategory(t *testing.T) {
	t.Parallel()

	registry := tasks.NewDefaultTaskCategoryRegistry()
	category, err := api.GetTaskCategory(tasks.CategoryIDTransfer, registry)
	require.NoError(t, err)
	assert.Equal(t, tasks.CategoryIDTransfer, category.ID())

	_, err = api.GetTaskCategory(0, registry)
	require.Error(t, err)
	assert.ErrorContains(t, err, "0")
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}
