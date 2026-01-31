package matching

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

func TestHandlerReadinessLifecycle(t *testing.T) {
	t.Parallel()

	handler := &Handler{}

	err := handler.checkReady()
	require.Error(t, err)

	handler.ready.Store(true)
	require.NoError(t, handler.checkReady())

	handler.ready.Store(false)
	err = handler.checkReady()
	require.Error(t, err)
}

func TestAddActivityTaskReadinessCheck(t *testing.T) {
	t.Parallel()

	handler := &Handler{
		ready: atomic.Bool{},
	}

	ctx := context.Background()
	request := &matchingservice.AddActivityTaskRequest{
		NamespaceId: "test-namespace",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "test-queue",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}

	// Should fail when not ready
	_, err := handler.AddActivityTask(ctx, request)
	require.Error(t, err)
	assert.IsType(t, &serviceerror.Unavailable{}, err)
}

func TestAddWorkflowTaskReadinessCheck(t *testing.T) {
	t.Parallel()

	handler := &Handler{
		ready: atomic.Bool{},
	}

	ctx := context.Background()
	request := &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: "test-namespace",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "test-queue",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}

	// Should fail when not ready
	_, err := handler.AddWorkflowTask(ctx, request)
	require.Error(t, err)
	assert.IsType(t, &serviceerror.Unavailable{}, err)

}
