package startworkflow

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
)

type (
	startWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		mockExecutionMgr *persistence.MockExecutionManager

		consistenceChecker api.WorkflowConsistencyChecker
	}
)

func TestStartWorkflowSuite(t *testing.T) {
	s := new(startWorkflowSuite)
	suite.Run(t, s)
}

func (s *startWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr

	workflowCache := workflow.NewCache(s.mockShard)
	s.consistenceChecker = api.NewWorkflowConsistencyChecker(s.mockShard, workflowCache)

	mockNamespaceCache := s.mockShard.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()

	mockEventsCache := s.mockShard.MockEventsCache
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	mockEngine := shard.NewMockEngine(s.controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockShard.SetEngineForTesting(mockEngine)
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_BrandNew() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	requestID := uuid.New()
	resp, err := Invoke(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: timestamp.DurationPtr(20 * time.Second),
				WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
				Identity:                 identity,
				RequestId:                requestID,
			},
		},
		s.mockShard,
		s.consistenceChecker,
	)
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_BrandNew_SearchAttributes() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		eventsToSave := request.NewWorkflowEvents[0].Events
		s.Len(eventsToSave, 2)
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, eventsToSave[0].GetEventType())
		startEventAttributes := eventsToSave[0].GetWorkflowExecutionStartedEventAttributes()
		// Search attribute name was mapped and saved under field name.
		s.Equal(
			payload.EncodeString("test"),
			startEventAttributes.GetSearchAttributes().GetIndexedFields()["CustomKeywordField"])
		return tests.CreateWorkflowExecutionResponse, nil
	})

	requestID := uuid.New()
	resp, err := Invoke(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: timestamp.DurationPtr(20 * time.Second),
				WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
				Identity:                 identity,
				RequestId:                requestID,
				SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
					"CustomKeywordField": payload.EncodeString("test"),
				}}},
		},
		s.mockShard,
		s.consistenceChecker,
	)
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_StillRunning_Dedup() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	requestID := "requestID"
	lastWriteVersion := common.EmptyVersion

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        requestID,
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: lastWriteVersion,
	})

	resp, err := Invoke(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
				Identity:                 identity,
				RequestId:                requestID,
			},
		},
		s.mockShard,
		s.consistenceChecker,
	)
	s.Nil(err)
	s.Equal(runID, resp.GetRunId())
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_StillRunning_NonDeDup() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        "oldRequestID",
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: lastWriteVersion,
	})

	resp, err := Invoke(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
				Identity:                 identity,
				RequestId:                "newRequestID",
			},
		},
		s.mockShard,
		s.consistenceChecker,
	)
	if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
		s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
	}
	s.Nil(resp)
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_NotRunning_PrevSuccess() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []enumspb.WorkflowIdReusePolicy{
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	expecedErrs := []bool{true, false, true}

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
			return request.Mode == persistence.CreateWorkflowModeBrandNew
		}),
	).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        "oldRequestID",
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		LastWriteVersion: lastWriteVersion,
	}).Times(len(expecedErrs))

	for index, option := range options {
		if !expecedErrs[index] {
			s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
				gomock.Any(),
				newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
					return request.Mode == persistence.CreateWorkflowModeUpdateCurrent &&
						request.PreviousRunID == runID &&
						request.PreviousLastWriteVersion == lastWriteVersion
				}),
			).Return(tests.CreateWorkflowExecutionResponse, nil)
		}

		resp, err := Invoke(
			metrics.AddMetricsContext(context.Background()),
			&historyservice.StartWorkflowExecutionRequest{
				Attempt:     1,
				NamespaceId: namespaceID.String(),
				StartRequest: &workflowservice.StartWorkflowExecutionRequest{
					Namespace:                namespaceID.String(),
					WorkflowId:               workflowID,
					WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
					WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
					WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
					Identity:                 identity,
					RequestId:                "newRequestID",
					WorkflowIdReusePolicy:    option,
				},
			},
			s.mockShard,
			s.consistenceChecker,
		)

		if expecedErrs[index] {
			if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
				s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
			}
			s.Nil(resp)
		} else {
			s.Nil(err)
			s.NotNil(resp)
		}
	}
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_NotRunning_PrevFail() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []enumspb.WorkflowIdReusePolicy{
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	expecedErrs := []bool{false, false, true}

	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}
	runIDs := []string{"1", "2", "3", "4"}

	for i, status := range statuses {

		s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
			gomock.Any(),
			newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
				return request.Mode == persistence.CreateWorkflowModeBrandNew
			}),
		).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
			Msg:              "random message",
			RequestID:        "oldRequestID",
			RunID:            runIDs[i],
			State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status:           status,
			LastWriteVersion: lastWriteVersion,
		}).Times(len(expecedErrs))

		for j, option := range options {

			if !expecedErrs[j] {
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
					gomock.Any(),
					newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
						return request.Mode == persistence.CreateWorkflowModeUpdateCurrent &&
							request.PreviousRunID == runIDs[i] &&
							request.PreviousLastWriteVersion == lastWriteVersion
					}),
				).Return(tests.CreateWorkflowExecutionResponse, nil)
			}

			resp, err := Invoke(
				metrics.AddMetricsContext(context.Background()),
				&historyservice.StartWorkflowExecutionRequest{
					Attempt:     1,
					NamespaceId: namespaceID.String(),
					StartRequest: &workflowservice.StartWorkflowExecutionRequest{
						Namespace:                namespaceID.String(),
						WorkflowId:               workflowID,
						WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
						TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
						WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
						WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
						Identity:                 identity,
						RequestId:                "newRequestID",
						WorkflowIdReusePolicy:    option,
					},
				},
				s.mockShard,
				s.consistenceChecker,
			)

			if expecedErrs[j] {
				if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
					s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
				}
				s.Nil(resp)
			} else {
				s.Nil(err)
				s.NotNil(resp)
			}
		}
	}
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_WithRunID_Dedup() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(&persistence.GetWorkflowExecutionResponse{}, nil).Times(1)

	resp, err := Invoke(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
				Identity:                 identity,
			},
			RunId: runID,
		},
		s.mockShard,
		s.consistenceChecker,
	)
	s.Nil(err)
	s.Equal(runID, resp.GetRunId())
}

func (s *startWorkflowSuite) TestStartWorkflowExecution_WithRunID_WithParentInfo_NonDedup() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, &serviceerror.NotFound{}).Times(1)

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
			s.Equal(int64(2), request.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskScheduledEventId)
			transferTasks := request.NewWorkflowSnapshot.Tasks[tasks.CategoryTransfer]
			s.Len(transferTasks, 1)
			s.Equal(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, transferTasks[0].GetType())
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	resp, err := Invoke(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: timestamp.DurationPtr(20 * time.Second),
				WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
				Identity:                 identity,
			},
			ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
				NamespaceId: tests.ParentNamespaceID.String(),
				Namespace:   tests.ParentNamespace.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "some random parent workflow ID",
					RunId:      uuid.New(),
				},
				InitiatedId:      int64(3222),
				InitiatedVersion: int64(1234),
				Clock:            vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63()),
			},
			RunId: runID,
		},
		s.mockShard,
		s.consistenceChecker,
	)
	s.Nil(err)
	s.NotNil(resp.RunId)
}

type createWorkflowExecutionRequestMatcher struct {
	f func(request *persistence.CreateWorkflowExecutionRequest) bool
}

func newCreateWorkflowExecutionRequestMatcher(f func(request *persistence.CreateWorkflowExecutionRequest) bool) gomock.Matcher {
	return &createWorkflowExecutionRequestMatcher{
		f: f,
	}
}

func (m *createWorkflowExecutionRequestMatcher) Matches(x interface{}) bool {
	request, ok := x.(*persistence.CreateWorkflowExecutionRequest)
	if !ok {
		return false
	}
	return m.f(request)
}

func (m *createWorkflowExecutionRequestMatcher) String() string {
	return "CreateWorkflowExecutionRequest match condition"
}
