// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/fx"
	"google.golang.org/grpc/codes"

	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/tasks"
)

type (
	PurgeDLQTasksSuite struct {
		*require.Assertions
		FunctionalTestBase
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
	s.Assertions = require.New(s.T())
	s.setupSuite(
		"testdata/es_cluster.yaml",
		WithFxOptionsForService(primitives.HistoryService,
			fx.Decorate(func(manager persistence.HistoryTaskQueueManager) persistence.HistoryTaskQueueManager {
				s.dlq = &faultyDLQ{HistoryTaskQueueManager: manager}
				return s.dlq
			}),
		),
		WithFxOptionsForService(primitives.FrontendService,
			fx.Populate(&s.sdkClientFactory),
		),
	)
}

func (s *PurgeDLQTasksSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *PurgeDLQTasksSuite) SetupTest() {
	s.Assertions = require.New(s.T())
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

			purgeDLQTasksResponse, err := s.adminClient.PurgeDLQTasks(ctx, &adminservice.PurgeDLQTasksRequest{
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
			SourceShardID: tasks.GetShardIDForTask(task, int(s.testClusterConfig.HistoryConfig.NumHistoryShards)),
		})
		s.NoError(err)
	}
}
