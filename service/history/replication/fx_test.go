package replication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

func TestReplicationTaskMetricsTags_Namespace(t *testing.T) {
	const (
		namespaceID   = "test-namespace-id"
		namespaceName = "test-namespace"
	)
	historyTask := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		RawTaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
		},
	}

	testCases := []struct {
		name            string
		replicationTask *replicationspb.ReplicationTask
		setupRegistry   func(*namespace.MockRegistry)
		expectedNsTag   metrics.Tag
		expectedType    string
	}{
		{
			name:            "namespace resolved",
			replicationTask: historyTask,
			setupRegistry: func(r *namespace.MockRegistry) {
				r.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name(namespaceName), nil)
			},
			expectedNsTag: metrics.NamespaceTag(namespaceName),
			expectedType:  enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK.String(),
		},
		{
			name:            "namespace lookup fails",
			replicationTask: historyTask,
			setupRegistry: func(r *namespace.MockRegistry) {
				r.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.EmptyName, errors.New("not found"))
			},
			expectedNsTag: metrics.NamespaceUnknownTag(),
			expectedType:  enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK.String(),
		},
		{
			name: "no raw task info",
			replicationTask: &replicationspb.ReplicationTask{
				TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
			},
			setupRegistry: func(*namespace.MockRegistry) {},
			expectedNsTag: metrics.NamespaceUnknownTag(),
			expectedType:  enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK.String(),
		},
		{
			name:            "no replication task",
			replicationTask: nil,
			setupRegistry:   func(*namespace.MockRegistry) {},
			expectedNsTag:   metrics.NamespaceUnknownTag(),
			expectedType:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			registry := namespace.NewMockRegistry(ctrl)
			tc.setupRegistry(registry)
			task := NewMockTrackableExecutableTask(ctrl)
			task.EXPECT().ReplicationTask().Return(tc.replicationTask).AnyTimes()

			tags := replicationTaskMetricsTags(task, registry)

			require.Contains(t, tags, tc.expectedNsTag)
			require.Contains(t, tags, metrics.TaskTypeTag(tc.expectedType))
		})
	}
}
