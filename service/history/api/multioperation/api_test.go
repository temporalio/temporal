package multioperation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/updateworkflow"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

const (
	testUpdateID = "test-update-id"
)

type (
	updateWithStartSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		currentContext      *historyi.MockWorkflowContext
		currentMutableState *historyi.MockMutableState
		consistencyChecker  *api.MockWorkflowConsistencyChecker
	}
)

func TestUpdateWithStartSuite(t *testing.T) {
	s := new(updateWithStartSuite)
	suite.Run(t, s)
}

func (s *updateWithStartSuite) SetupSuite() {
}

func (s *updateWithStartSuite) TearDownSuite() {
}

func (s *updateWithStartSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.currentContext = historyi.NewMockWorkflowContext(s.controller)
	s.currentMutableState = historyi.NewMockMutableState(s.controller)
	s.consistencyChecker = api.NewMockWorkflowConsistencyChecker(s.controller)
}

func (s *updateWithStartSuite) TearDownTest() {
	s.controller.Finish()
}

// TestInvoke_CompletedUpdate_StatusCapturedBeforeRelease verifies that when
// Invoke finds a completed update outcome, it captures ExecutionState.Status
// before releasing the workflow lock. A concurrent writer that starts after
// the lock is released must not race with the Status read.
//
// Run with -race to verify:
//
//	go test -race -tags test_dep -count=1 \
//	  -run TestUpdateWithStartSuite/TestInvoke_CompletedUpdate_StatusCapturedBeforeRelease \
//	  ./service/history/api/multioperation/
func (s *updateWithStartSuite) TestInvoke_CompletedUpdate_StatusCapturedBeforeRelease() {
	ctx := context.Background()

	// executionState is the race target: the concurrent writer modifies Status
	// after the lock is released. This must be a mutable struct so the
	// goroutine can write to it.
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}

	// writerStarted is closed once the writer goroutine has begun writing,
	// ensuring the concurrent write is in progress before Invoke reads Status.
	lockReleased := make(chan struct{})
	writerStarted := make(chan struct{})
	var stop atomic.Bool

	// Track that release was called with nil error (clean release, not error path).
	var releaseCalledWithNilErr atomic.Bool

	releaseFn := historyi.ReleaseWorkflowContextFunc(func(err error) {
		if err == nil {
			releaseCalledWithNilErr.Store(true)
		}
		close(lockReleased)
		// Wait until the writer goroutine has started writing before returning,
		// so the subsequent Status read in Invoke overlaps with the write.
		<-writerStarted
	})

	lease := api.NewWorkflowLease(s.currentContext, releaseFn, s.currentMutableState)

	s.consistencyChecker.EXPECT().
		GetWorkflowLease(gomock.Any(), nil, gomock.Any(), gomock.Any()).
		Return(lease, nil)

	s.currentMutableState.EXPECT().
		GetUpdateOutcome(gomock.Any(), gomock.Any()).
		Return(&updatepb.Outcome{}, nil)

	s.currentMutableState.EXPECT().
		GetExecutionState().
		Return(executionState).
		AnyTimes()

	s.currentContext.EXPECT().
		GetWorkflowKey().
		Return(tests.WorkflowKey).
		AnyTimes()

	// Concurrent writer: simulates another goroutine that acquires the lock
	// after release and modifies the execution state. Signals writerStarted
	// after the first write so the release function knows it's safe to return.
	go func() {
		<-lockReleased
		executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		close(writerStarted)
		for !stop.Load() {
			executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		}
	}()
	defer stop.Store(true)

	updateReq := createUpdateRequest(testUpdateID)

	uws := &updateWithStart{
		namespaceId:        tests.NamespaceID,
		consistencyChecker: s.consistencyChecker,
		updateReq:          updateReq,
		startReq: &historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId: tests.WorkflowID,
			},
		},
		updater: updateworkflow.NewUpdater(nil, nil, nil, updateReq),
	}

	resp, err := uws.Invoke(ctx)
	s.NoError(err)
	s.NotNil(resp)

	startResp := resp.Responses[0].GetStartWorkflow()
	s.Equal(tests.RunID, startResp.RunId)
	s.False(startResp.Started)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, startResp.Status)
	s.True(releaseCalledWithNilErr.Load(), "release function should have been called with nil error")
}

func createUpdateRequest(updateID string) *historyservice.UpdateWorkflowExecutionRequest {
	return &historyservice.UpdateWorkflowExecutionRequest{
		Request: &workflowservice.UpdateWorkflowExecutionRequest{
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{
					UpdateId: updateID,
				},
			},
		},
	}
}
