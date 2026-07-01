package tests

import (
	"sync/atomic"
	"testing"

	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type (
	PurgeDLQTasksSuite struct {
		parallelsuite.Suite[*PurgeDLQTasksSuite]
	}
)

func TestPurgeDLQTasksSuite(t *testing.T) {
	parallelsuite.Run(t, &PurgeDLQTasksSuite{})
}

func (s *PurgeDLQTasksSuite) TestPurgeDLQTasks() {
	var deleteTasksFaultCount atomic.Int32
	for _, tc := range []struct {
		name          string
		envOptions    []testcore.TestOption
		mutateRequest func(*adminservice.PurgeDLQTasksRequest)
		apiErr        string
		workflowErr   string
		faultCount    *atomic.Int32
	}{
		{
			name:       "HappyPath",
			envOptions: []testcore.TestOption{testcore.WithWorkerService()},
		},
		{
			name: "MissingSourceCluster",
			mutateRequest: func(request *adminservice.PurgeDLQTasksRequest) {
				request.DlqKey.SourceCluster = ""
			},
			apiErr: "SourceCluster",
		},
		{
			name: "MissingTargetCluster",
			mutateRequest: func(request *adminservice.PurgeDLQTasksRequest) {
				request.DlqKey.TargetCluster = ""
			},
			apiErr: "TargetCluster",
		},
		{
			name:       "QueueDoesNotExist",
			envOptions: []testcore.TestOption{testcore.WithWorkerService()},
			mutateRequest: func(request *adminservice.PurgeDLQTasksRequest) {
				request.DlqKey.TargetCluster = "does-not-exist"
			},
			workflowErr: "queue not found",
		},
		{
			name: "DeleteTasksUnavailableError",
			envOptions: []testcore.TestOption{
				testcore.WithWorkerService(),
				testcore.WithPersistenceFaultInjection(&config.FaultInjection{
					Injector: func(target config.FaultInjectionTarget) error {
						if target.Store == config.QueueV2Name &&
							target.Method == "RangeDeleteMessages" &&
							deleteTasksFaultCount.CompareAndSwap(0, 1) {
							return serviceerror.NewUnavailable("DLQ unavailable")
						}
						return nil
					},
				}),
			},
			faultCount: &deleteTasksFaultCount,
		},
	} {
		s.Run(tc.name, func(s *PurgeDLQTasksSuite) {
			env := testcore.NewEnv(s.T(), tc.envOptions...)
			defaultQueueKey := persistencetest.GetQueueKey(s.T())

			queueKey := persistence.QueueKey{
				QueueType:     persistence.QueueTypeHistoryDLQ,
				Category:      tasks.CategoryTransfer,
				SourceCluster: defaultQueueKey.SourceCluster,
				TargetCluster: defaultQueueKey.TargetCluster,
			}
			dlq := s.enqueueTasks(env, queueKey, &tasks.WorkflowTask{})

			request := &adminservice.PurgeDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(tasks.CategoryTransfer.ID()),
					SourceCluster: defaultQueueKey.SourceCluster,
					TargetCluster: defaultQueueKey.TargetCluster,
				},
				InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
					MessageId: persistence.FirstQueueMessageID + 1,
				},
			}
			if tc.mutateRequest != nil {
				tc.mutateRequest(request)
			}

			purgeDLQTasksResponse, err := env.AdminClient().PurgeDLQTasks(s.Context(), request)
			if tc.apiErr != "" {
				s.Error(err)
				s.ErrorContains(err, tc.apiErr)
				s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
				return
			}
			s.NoError(err)

			client, err := sdkclient.NewClientFromExisting(env.SdkClient(), sdkclient.Options{
				Namespace: primitives.SystemLocalNamespace,
			})
			s.NoError(err)

			var jobToken adminservice.DLQJobToken
			err = jobToken.Unmarshal(purgeDLQTasksResponse.JobToken)
			s.NoError(err)

			workflow := client.GetWorkflow(s.Context(), jobToken.WorkflowId, jobToken.RunId)
			err = workflow.Get(s.Context(), nil)
			if tc.workflowErr != "" {
				s.Error(err)
				s.ErrorContains(err, tc.workflowErr)
				return
			}
			s.NoError(err)

			if tc.faultCount != nil {
				s.Equal(int32(1), tc.faultCount.Load())
			}

			readRawTasksResponse, err := dlq.ReadRawTasks(s.Context(), &persistence.ReadRawTasksRequest{
				QueueKey: queueKey,
				PageSize: 10,
			})
			s.NoError(err)
			s.Len(readRawTasksResponse.Tasks, 1)
			s.Equal(int64(persistence.FirstQueueMessageID+2), readRawTasksResponse.Tasks[0].MessageMetadata.ID)
		})
	}
}

func (s *PurgeDLQTasksSuite) enqueueTasks(
	env *testcore.TestEnv,
	queueKey persistence.QueueKey,
	task *tasks.WorkflowTask,
) persistence.HistoryTaskQueueManager {
	dlq, err := env.GetTestCluster().TestBase().Factory.NewHistoryTaskQueueManager()
	s.NoError(err)

	_, err = dlq.CreateQueue(s.Context(), &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	s.NoError(err)

	for range 3 {
		_, err = dlq.EnqueueTask(s.Context(), &persistence.EnqueueTaskRequest{
			QueueType:     queueKey.QueueType,
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
			Task:          task,
			SourceShardID: tasks.GetShardIDForTask(task, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards)),
		})
		s.NoError(err)
	}

	return dlq
}
