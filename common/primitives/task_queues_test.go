package primitives

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

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
			name:            "Child is empty, parent is internal - not allowed",
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
