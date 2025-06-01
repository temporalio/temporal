package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/grpc/codes"
)

type (
	PurgeDLQTasksSuite struct {
		testcore.FunctionalTestBase

		dlq              *faultyDLQ
		sdkClientFactory sdk.ClientFactory
	}
	purgeDLQTasksTestCase struct {
		name      string
		configure func(p *purgeDLQTasksTestParams)
	}
	purgeDLQTasksTestParams struct {
		category                 tasks.Category
		sourceCluster            string
		targetCluster            string
		maxMessageID             int64
		apiCallErrorExpectation  func(err error)
		workflowErrorExpectation func(err error)
		deleteTasksErr           error
	}
	faultyDLQ struct {
		persistence.HistoryTaskQueueManager
		err error
	}
)

func TestPurgeDLQTasksSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PurgeDLQTasksSuite))
}

func (q *faultyDLQ) DeleteTasks(
	ctx context.Context,
	request *persistence.DeleteTasksRequest,
) (*persistence.DeleteTasksResponse, error) {
	if q.err != nil {
		return nil, q.err
	}

	return q.HistoryTaskQueueManager.DeleteTasks(ctx, request)
}

func (s *PurgeDLQTasksSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithFxOptionsForService(primitives.HistoryService,
			fx.Decorate(func(manager persistence.HistoryTaskQueueManager) persistence.HistoryTaskQueueManager {
				s.dlq = &faultyDLQ{HistoryTaskQueueManager: manager}
				return s.dlq
			}),
		),
		testcore.WithFxOptionsForService(primitives.FrontendService,
			fx.Populate(&s.sdkClientFactory),
		),
	)
}

func (s *PurgeDLQTasksSuite) TestPurgeDLQTasks() {
	for _, tc := range []purgeDLQTasksTestCase{
		{
			name: "HappyPath",
		},
		{
			name: "MissingSourceCluster",
			configure: func(p *purgeDLQTasksTestParams) {
				p.sourceCluster = ""
				p.apiCallErrorExpectation = func(err error) {
					s.Error(err)
					s.ErrorContains(err, "SourceCluster")
					s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
				}
			},
		},
		{
			name: "MissingTargetCluster",
			configure: func(p *purgeDLQTasksTestParams) {
				p.targetCluster = ""
				p.apiCallErrorExpectation = func(err error) {
					s.Error(err)
					s.ErrorContains(err, "TargetCluster")
					s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
				}
			},
		},
		{
			name: "QueueDoesNotExist",
			configure: func(p *purgeDLQTasksTestParams) {
				p.targetCluster = "does-not-exist"
				p.workflowErrorExpectation = func(err error) {
					s.Error(err)
					s.ErrorContains(err, "queue not found")
				}
			},
		},
		{
			name: "DeleteTasksUnavailableError",
			configure: func(p *purgeDLQTasksTestParams) {
				p.deleteTasksErr = serviceerror.NewUnavailable("DLQ unavailable")
				p.workflowErrorExpectation = func(err error) {
					s.NoError(err)
				}
			},
		},
	} {
		s.Run(tc.name, func() {
			defaultParams := s.defaultDlqTestParams()

			params := defaultParams
			if tc.configure != nil {
				tc.configure(&params)
			}

			ctx := context.Background()

			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			queueKey := persistence.QueueKey{
				QueueType:     persistence.QueueTypeHistoryDLQ,
				Category:      defaultParams.category,
				SourceCluster: defaultParams.sourceCluster,
				TargetCluster: defaultParams.targetCluster,
			}
			s.enqueueTasks(ctx, queueKey, &tasks.WorkflowTask{})

			purgeDLQTasksResponse, err := s.AdminClient().PurgeDLQTasks(ctx, &adminservice.PurgeDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(params.category.ID()),
					SourceCluster: params.sourceCluster,
					TargetCluster: params.targetCluster,
				},
				InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
					MessageId: persistence.FirstQueueMessageID + 1,
				},
			})
			if params.apiCallErrorExpectation != nil {
				params.apiCallErrorExpectation(err)
				return
			}

			s.NoError(err)
			client := s.sdkClientFactory.GetSystemClient()

			var jobToken adminservice.DLQJobToken
			err = jobToken.Unmarshal(purgeDLQTasksResponse.JobToken)
			s.NoError(err)

			workflow := client.GetWorkflow(ctx, jobToken.WorkflowId, jobToken.RunId)

			err = workflow.Get(ctx, nil)
			if params.workflowErrorExpectation != nil {
				params.workflowErrorExpectation(err)
				return
			}

			s.NoError(err)
			readRawTasksResponse, err := s.dlq.ReadRawTasks(ctx, &persistence.ReadRawTasksRequest{
				QueueKey: queueKey,
				PageSize: 10,
			})
			s.NoError(err)
			s.Len(readRawTasksResponse.Tasks, 1)
			s.Assert().Equal(int64(persistence.FirstQueueMessageID+2), readRawTasksResponse.Tasks[0].MessageMetadata.ID)
		})
	}
}

func (s *PurgeDLQTasksSuite) defaultDlqTestParams() purgeDLQTasksTestParams {
	queueKey := persistencetest.GetQueueKey(s.T())

	return purgeDLQTasksTestParams{
		category:      tasks.CategoryTransfer,
		sourceCluster: queueKey.SourceCluster,
		targetCluster: queueKey.TargetCluster,
	}
}

func (s *PurgeDLQTasksSuite) enqueueTasks(ctx context.Context, queueKey persistence.QueueKey, task *tasks.WorkflowTask) {
	_, err := s.dlq.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	s.NoError(err)

	for i := 0; i < 3; i++ {
		_, err := s.dlq.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
			QueueType:     queueKey.QueueType,
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
			Task:          task,
			SourceShardID: tasks.GetShardIDForTask(task, int(s.GetTestClusterConfig().HistoryConfig.NumHistoryShards)),
		})
		s.NoError(err)
	}
}
