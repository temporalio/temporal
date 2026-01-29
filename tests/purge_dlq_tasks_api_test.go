package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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

func (q *faultyDLQ) DeleteTasks(
	ctx context.Context,
	request *persistence.DeleteTasksRequest,
) (*persistence.DeleteTasksResponse, error) {
	if q.err != nil {
		return nil, q.err
	}

	return q.HistoryTaskQueueManager.DeleteTasks(ctx, request)
}

func TestPurgeDLQTasks(t *testing.T) {
	var dlq *faultyDLQ
	var sdkClientFactory sdk.ClientFactory

	s := testcore.NewEnv(t,
		testcore.WithClusterOptions(
			testcore.WithFxOptionsForService(primitives.HistoryService,
				fx.Decorate(func(manager persistence.HistoryTaskQueueManager) persistence.HistoryTaskQueueManager {
					dlq = &faultyDLQ{HistoryTaskQueueManager: manager}
					return dlq
				}),
			),
			testcore.WithFxOptionsForService(primitives.FrontendService,
				fx.Populate(&sdkClientFactory),
			),
		),
	)

	defaultDlqTestParams := func() purgeDLQTasksTestParams {
		queueKey := persistencetest.GetQueueKey(t)

		return purgeDLQTasksTestParams{
			category:      tasks.CategoryTransfer,
			sourceCluster: queueKey.SourceCluster,
			targetCluster: queueKey.TargetCluster,
		}
	}

	enqueueTasks := func(ctx context.Context, queueKey persistence.QueueKey, task *tasks.WorkflowTask) error {
		if dlq == nil {
			return fmt.Errorf("dlq is nil")
		}
		_, err := dlq.CreateQueue(ctx, &persistence.CreateQueueRequest{
			QueueKey: queueKey,
		})
		// Ignore "already exists" errors as multiple test cases may try to create the same queue
		if err != nil && !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "AlreadyExists") {
			return fmt.Errorf("CreateQueue failed: %w", err)
		}

		for i := 0; i < 3; i++ {
			_, err := dlq.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
				QueueType:     queueKey.QueueType,
				SourceCluster: queueKey.SourceCluster,
				TargetCluster: queueKey.TargetCluster,
				Task:          task,
				SourceShardID: tasks.GetShardIDForTask(task, int(s.GetTestClusterConfig().HistoryConfig.NumHistoryShards)),
			})
			if err != nil {
				return fmt.Errorf("EnqueueTask failed: %w", err)
			}
		}
		return nil
	}

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
		t.Run(tc.name, func(st *testing.T) {
			defaultParams := defaultDlqTestParams()

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
			if err := enqueueTasks(ctx, queueKey, &tasks.WorkflowTask{}); err != nil {
				st.Fatalf("enqueueTasks failed: %v", err)
			}

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
			client := sdkClientFactory.GetSystemClient()

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
			readRawTasksResponse, err := dlq.ReadRawTasks(ctx, &persistence.ReadRawTasksRequest{
				QueueKey: queueKey,
				PageSize: 10,
			})
			s.NoError(err)
			s.Len(readRawTasksResponse.Tasks, 1)
			s.Assert().Equal(int64(persistence.FirstQueueMessageID+2), readRawTasksResponse.Tasks[0].MessageMetadata.ID)
		})
	}
}
