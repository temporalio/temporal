package primitives

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func TestIsInternalTaskQueueForUserNs(t *testing.T) {
	tests := []struct {
		name      string
		taskQueue string
		expected  bool
	}{
		{
			name:      "PerNSWorkerTaskQueue is internal",
			taskQueue: PerNSWorkerTaskQueue,
			expected:  true,
		},
		{
			name:      "DefaultWorkerTaskQueue is not internal for user namespace",
			taskQueue: DefaultWorkerTaskQueue,
			expected:  false,
		},
		{
			name:      "MigrationActivityTQ is not internal for user namespace",
			taskQueue: MigrationActivityTQ,
			expected:  false,
		},
		{
			name:      "AddSearchAttributesActivityTQ is not internal for user namespace",
			taskQueue: AddSearchAttributesActivityTQ,
			expected:  false,
		},
		{
			name:      "DeleteNamespaceActivityTQ is not internal for user namespace",
			taskQueue: DeleteNamespaceActivityTQ,
			expected:  false,
		},
		{
			name:      "DLQActivityTQ is not internal for user namespace",
			taskQueue: DLQActivityTQ,
			expected:  false,
		},
		{
			name:      "User defined task queue is not internal",
			taskQueue: "my-custom-task-queue",
			expected:  false,
		},
		{
			name:      "Empty string is not internal",
			taskQueue: "",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInternalPerNsTaskQueue(tt.taskQueue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckInternalTaskQueueForUserNsAllowed(t *testing.T) {
	tests := []struct {
		name            string
		parentTaskQueue string
		targetTaskQueue string
		wantErr         bool
	}{
		{
			name:            "Child is internal, parent is internal - allowed",
			parentTaskQueue: PerNSWorkerTaskQueue,
			targetTaskQueue: PerNSWorkerTaskQueue,
			wantErr:         false,
		},
		{
			name:            "Child is internal, parent is user task queue - error",
			parentTaskQueue: "my-custom-task-queue",
			targetTaskQueue: PerNSWorkerTaskQueue,
			wantErr:         true,
		},
		{
			name:            "Child is internal, parent is empty - error",
			parentTaskQueue: "",
			targetTaskQueue: PerNSWorkerTaskQueue,
			wantErr:         true,
		},
		{
			name:            "Child is not internal, parent is internal - allowed",
			parentTaskQueue: PerNSWorkerTaskQueue,
			targetTaskQueue: DefaultWorkerTaskQueue,
			wantErr:         false,
		},
		{
			name:            "Child is user task queue, parent is user task queue - allowed",
			parentTaskQueue: "my-custom-task-queue",
			targetTaskQueue: "another-custom-task-queue",
			wantErr:         false,
		},
		{
			name:            "Child is not internal, parent is empty - allowed, this method logic doesn't check for empty string",
			parentTaskQueue: "",
			targetTaskQueue: DefaultWorkerTaskQueue,
			wantErr:         false,
		},
		{
			name:            "Child is empty, parent is internal - allowed",
			parentTaskQueue: PerNSWorkerTaskQueue,
			targetTaskQueue: "",
			wantErr:         true,
		},
		{
			name:            "Both empty - error",
			parentTaskQueue: "",
			targetTaskQueue: "",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckInternalPerNsTaskQueueAllowed(tt.targetTaskQueue, tt.parentTaskQueue)
			if tt.wantErr {
				require.Error(t, err)
				var invalidArgument *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgument)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
