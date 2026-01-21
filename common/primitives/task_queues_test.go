package primitives

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
			result := IsInternalTaskQueueForUserNs(tt.taskQueue)
			assert.Equal(t, tt.expected, result)
		})
	}
}
