package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
)

type (
	replicationMetricsSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestReplicationMetricsSuite(t *testing.T) {
	suite.Run(t, new(replicationMetricsSuite))
}

func (s *replicationMetricsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *replicationMetricsSuite) TestTaskOperationTag_Nil() {
	s.Equal("__unknown__", TaskOperationTag(nil))
}

func (s *replicationMetricsSuite) TestTaskOperationTag() {
	cases := []struct {
		taskType enumsspb.ReplicationTaskType
		expected string
	}{
		{enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK, "__unknown__"},
		{enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK, "__unknown__"},
		{enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK, metrics.SyncActivityTaskScope},
		{enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK, metrics.SyncWorkflowStateTaskScope},
		{enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK, metrics.HistoryReplicationTaskScope},
		{enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK, metrics.SyncHSMTaskScope},
		{enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK, metrics.SyncVersionedTransitionTaskScope},
		{enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK, metrics.BackfillHistoryEventsTaskScope},
		{enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK, metrics.VerifyVersionedTransitionTaskScope},
		{enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK, metrics.DeleteExecutionReplicationTaskScope},
		{enumsspb.ReplicationTaskType(-1), metrics.NoopTaskScope},
	}
	for _, c := range cases {
		s.Equal(c.expected, TaskOperationTag(&replicationspb.ReplicationTask{TaskType: c.taskType}), c.taskType.String())
	}
}

func (s *replicationMetricsSuite) TestTaskOperationTagFromTask() {
	cases := []struct {
		taskType enumsspb.TaskType
		expected string
	}{
		{enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM, metrics.SyncHSMTaskScope},
		{enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION, metrics.SyncVersionedTransitionTaskScope},
		{enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, metrics.SyncActivityTaskScope},
		{enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE, metrics.SyncWorkflowStateTaskScope},
		{enumsspb.TASK_TYPE_REPLICATION_HISTORY, metrics.HistoryReplicationTaskScope},
		{enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION, metrics.DeleteExecutionReplicationTaskScope},
		{enumsspb.TASK_TYPE_UNSPECIFIED, metrics.UnknownTaskScope},
	}
	for _, c := range cases {
		s.Equal(c.expected, TaskOperationTagFromTask(c.taskType), c.taskType.String())
	}
}
