package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/mock/gomock"
)

type (
	executableTaskConverterSuite struct {
		suite.Suite
		*require.Assertions

		convController *gomock.Controller
		converter      *executableTaskConverterImpl
		clientShardKey ClusterShardKey
		serverShardKey ClusterShardKey
	}
)

func TestExecutableTaskConverterSuite(t *testing.T) {
	suite.Run(t, new(executableTaskConverterSuite))
}

func (s *executableTaskConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.convController = gomock.NewController(s.T())

	processToolBox := ProcessToolBox{
		Config:         configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
		MetricsHandler: metrics.NoopMetricsHandler,
		Logger:         log.NewNoopLogger(),
		DLQWriter:      NoopDLQWriter{},
	}
	s.converter = NewExecutableTaskConverter(processToolBox)
	s.clientShardKey = NewClusterShardKey(1, 1)
	s.serverShardKey = NewClusterShardKey(2, 2)
}

func (s *executableTaskConverterSuite) TestNewExecutableTaskConverter() {
	s.NotNil(s.converter)
}

func (s *executableTaskConverterSuite) TestConvert_Empty() {
	tasks := s.converter.Convert("sourceCluster", s.clientShardKey, s.serverShardKey)
	s.Empty(tasks)
}

func (s *executableTaskConverterSuite) TestConvert_AllTaskTypes() {
	taskTypes := []struct {
		name     string
		taskType enumsspb.ReplicationTaskType
	}{
		{"sync_shard_status", enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK},
		{"history_metadata", enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK},
		{"sync_activity", enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK},
		{"sync_workflow_state", enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK},
		{"history_v2", enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK},
		{"sync_hsm", enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK},
		{"backfill_history", enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK},
		{"verify_versioned_transition", enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK},
		{"sync_versioned_transition", enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK},
		{"delete_execution", enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK},
		{"unknown", enumsspb.ReplicationTaskType(-1)},
	}

	replicationTasks := make([]*replicationspb.ReplicationTask, len(taskTypes))
	for i, tt := range taskTypes {
		task := &replicationspb.ReplicationTask{
			TaskType:     tt.taskType,
			SourceTaskId: int64(i + 1),
		}
		// Some constructors dereference their attributes, so provide non-nil
		// attributes for those task types.
		switch tt.taskType {
		case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
			task.Attributes = &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
				SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
					NamespaceId: "ns",
					WorkflowId:  "wf",
					RunId:       "run",
				},
			}
		case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
			task.Attributes = &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
				SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
					WorkflowState: &persistencespb.WorkflowMutableState{
						ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: "ns", WorkflowId: "wf"},
						ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run"},
					},
				},
			}
		case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
			task.Attributes = &replicationspb.ReplicationTask_HistoryTaskAttributes{
				HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
					NamespaceId: "ns",
					WorkflowId:  "wf",
					RunId:       "run",
				},
			}
		case enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK:
			task.Attributes = &replicationspb.ReplicationTask_SyncHsmAttributes{
				SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
					NamespaceId: "ns",
					WorkflowId:  "wf",
					RunId:       "run",
				},
			}
		case enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK:
			task.Attributes = &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
				BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
					NamespaceId: "ns",
					WorkflowId:  "wf",
					RunId:       "run",
				},
			}
		case enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK:
			task.Attributes = &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
				VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
					NamespaceId: "ns",
					WorkflowId:  "wf",
					RunId:       "run",
				},
			}
		case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
			task.Attributes = &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
				SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
					NamespaceId: "ns",
					WorkflowId:  "wf",
					RunId:       "run",
				},
			}
		}
		replicationTasks[i] = task
	}

	tasks := s.converter.Convert("sourceCluster", s.clientShardKey, s.serverShardKey, replicationTasks...)
	s.Len(tasks, len(taskTypes))
	for i, task := range tasks {
		s.NotNil(task, taskTypes[i].name)
		s.Equal(int64(i+1), task.TaskID(), taskTypes[i].name)
	}
}

func (s *executableTaskConverterSuite) TestConvert_VisibilityTimeNil() {
	// When VisibilityTime is nil, convertOne falls back to time.Now().
	tasks := s.converter.Convert("sourceCluster", s.clientShardKey, s.serverShardKey,
		&replicationspb.ReplicationTask{
			TaskType:       enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK,
			SourceTaskId:   100,
			VisibilityTime: nil,
		},
	)
	s.Len(tasks, 1)
	s.Equal(int64(100), tasks[0].TaskID())
}
