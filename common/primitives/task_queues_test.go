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

func TestCheckNotInternalTaskQueueForUserNs(t *testing.T) {
	tests := []struct {
		name      string
		taskQueue string
		wantErr   bool
	}{
		{
			name:      "PerNSWorkerTaskQueue returns error",
			taskQueue: PerNSWorkerTaskQueue,
			wantErr:   true,
		},
		{
			name:      "DefaultWorkerTaskQueue returns nil",
			taskQueue: DefaultWorkerTaskQueue,
			wantErr:   false,
		},
		{
			name:      "MigrationActivityTQ returns nil",
			taskQueue: MigrationActivityTQ,
			wantErr:   false,
		},
		{
			name:      "User defined task queue returns nil",
			taskQueue: "my-custom-task-queue",
			wantErr:   false,
		},
		{
			name:      "Empty string returns nil, this method logic doesn't check for empty string",
			taskQueue: "",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckNotInternalPerNsTaskQueue(tt.taskQueue)
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

func TestCheckInternalTaskQueueForUserNsAllowed(t *testing.T) {
	tests := []struct {
		name            string
		parentTaskQueue string
		childTaskQueue  string
		wantErr         bool
	}{
		{
			name:            "Child is internal, parent is internal - allowed",
			parentTaskQueue: PerNSWorkerTaskQueue,
			childTaskQueue:  PerNSWorkerTaskQueue,
			wantErr:         false,
		},
		{
			name:            "Child is internal, parent is user task queue - error",
			parentTaskQueue: "my-custom-task-queue",
			childTaskQueue:  PerNSWorkerTaskQueue,
			wantErr:         true,
		},
		{
			name:            "Child is internal, parent is empty - error",
			parentTaskQueue: "",
			childTaskQueue:  PerNSWorkerTaskQueue,
			wantErr:         true,
		},
		{
			name:            "Child is not internal, parent is internal - allowed",
			parentTaskQueue: PerNSWorkerTaskQueue,
			childTaskQueue:  DefaultWorkerTaskQueue,
			wantErr:         false,
		},
		{
			name:            "Child is user task queue, parent is user task queue - allowed",
			parentTaskQueue: "my-custom-task-queue",
			childTaskQueue:  "another-custom-task-queue",
			wantErr:         false,
		},
		{
			name:            "Child is not internal, parent is empty - allowed, this method logic doesn't check for empty string",
			parentTaskQueue: "",
			childTaskQueue:  DefaultWorkerTaskQueue,
			wantErr:         false,
		},
		{
			name:            "Child is empty, parent is internal - allowed",
			parentTaskQueue: PerNSWorkerTaskQueue,
			childTaskQueue:  "",
			wantErr:         false,
		},
		{
			name:            "Both empty - allowed, this method logic doesn't check for empty string",
			parentTaskQueue: "",
			childTaskQueue:  "",
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckInternalPerNsTaskQueueAllowed(tt.parentTaskQueue, tt.childTaskQueue)
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
