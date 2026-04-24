package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
)

type LinksSuite struct {
	parallelsuite.Suite[*LinksSuite]
}

func TestLinksTestSuite(t *testing.T) {
	parallelsuite.Run(t, &LinksSuite{})
}

var links = []*commonpb.Link{
	{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  "dont-care",
				WorkflowId: "whatever",
				RunId:      uuid.NewString(),
			},
		},
	},
}

func (s *LinksSuite) TestTerminateWorkflow_LinksAttachedToEvent() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	run, err := env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: "dont-care",
		},
		"test-workflow-type",
	)
	s.NoError(err)

	// TODO(bergundy): Use SdkClient if and when it exposes links on TerminateWorkflow.
	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		Reason: "test",
		Links:  links,
	})
	s.NoError(err)

	history := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	event, err := history.Next()
	s.NoError(err)
	protorequire.ProtoSliceEqual(s.T(), links, event.Links)
}

func (s *LinksSuite) TestRequestCancelWorkflow_LinksAttachedToEvent() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	run, err := env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: "dont-care",
		},
		"test-workflow-type",
	)
	s.NoError(err)

	// TODO(bergundy): Use SdkClient if and when it exposes links on CancelWorkflow.
	_, err = env.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		Reason: "test",
		Links:  links,
	})
	s.NoError(err)

	history := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundEvent := false
	for history.HasNext() {
		event, err := history.Next()
		s.NoError(err)
		if event.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
			continue
		}
		foundEvent = true
		protorequire.ProtoSliceEqual(s.T(), links, event.Links)
	}
	s.True(foundEvent)
}

func (s *LinksSuite) TestSignalWorkflowExecution_LinksAttachedToEvent() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	run, err := env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: "dont-care",
		},
		"test-workflow-type",
	)
	s.NoError(err)

	// TODO(bergundy): Use SdkClient if and when it exposes links on SignalWorkflow.
	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		SignalName: "dont-care",
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Links:      links,
	})
	s.NoError(err)

	history := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundEvent := false
	for history.HasNext() {
		event, err := history.Next()
		s.NoError(err)
		if event.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			continue
		}
		foundEvent = true
		protorequire.ProtoSliceEqual(s.T(), links, event.Links)
	}
	s.True(foundEvent)
}

// TestUpdateWorkflowExecution_AcceptedUpdate_BacklinkInResponse verifies that
// the UpdateWorkflowExecution response contains a WorkflowEvent backlink pointing
// to the accepted event via RequestIdReference.
func (s *LinksSuite) TestUpdateWorkflowExecution_AcceptedUpdate_BacklinkInResponse() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Workflow that has an empty update handler and blocks on a completion signal.
	workflowFn := func(ctx workflow.Context) error {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context) error {
			return nil
		}); err != nil {
			return err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return nil
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	requestID := uuid.NewString()
	resp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
		Request: &updatepb.Request{
			Meta:      &updatepb.Meta{UpdateId: uuid.NewString()},
			Input:     &updatepb.Input{Name: "update"},
			RequestId: requestID,
		},
	})
	s.NoError(err)

	we := resp.GetLink().GetWorkflowEvent()
	s.NotNil(we)
	s.Equal(run.GetID(), we.GetWorkflowId())
	ref := we.GetRequestIdRef()
	s.NotNil(ref)
	s.Equal(requestID, ref.GetRequestId())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, ref.GetEventType())
}

// TestUpdateWorkflowExecution_RejectedUpdate_BacklinkInResponse verifies that
// the UpdateWorkflowExecution response contains a Workflow backlink (not a
// WorkflowEvent link) when the update is rejected by the validator, since
// rejections don't write a history event.
func (s *LinksSuite) TestUpdateWorkflowExecution_RejectedUpdate_BacklinkInResponse() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Workflow with an update handler that rejects every request to test rejection path.
	workflowFn := func(ctx workflow.Context) error {
		if err := workflow.SetUpdateHandlerWithOptions(ctx, "update",
			func(ctx workflow.Context) error { return nil },
			workflow.UpdateHandlerOptions{
				Validator: func(ctx workflow.Context) error {
					return errors.New("rejected by test")
				},
			},
		); err != nil {
			return err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return nil
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	resp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
		Request: &updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: uuid.NewString()},
			Input: &updatepb.Input{Name: "update"},
		},
	})
	s.NoError(err)
	s.NotNil(resp.GetOutcome().GetFailure(), "expected rejection failure in outcome")

	wf := resp.GetLink().GetWorkflow()
	s.Equal(run.GetID(), wf.GetWorkflowId())
	s.Equal("Update rejected", wf.GetReason())
}

// TestUpdateWorkflowExecution_AcceptedThenFailedUpdate_BacklinkInResponse verifies that
// an update which is accepted by the validator but whose handler returns a failure still
// receives a WorkflowEvent backlink (not a Workflow backlink). Accepted-then-failed updates
// write an UpdateAccepted event and must not be misclassified as rejections.
func (s *LinksSuite) TestUpdateWorkflowExecution_AcceptedThenFailedUpdate_BacklinkInResponse() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Workflow with an update handler that accepts (no validator rejection) but returns a failure.
	workflowFn := func(ctx workflow.Context) error {
		if err := workflow.SetUpdateHandler(ctx, "update",
			func(ctx workflow.Context) error {
				return errors.New("handler failure")
			},
		); err != nil {
			return err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return nil
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	requestID := uuid.NewString()
	resp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
		Request: &updatepb.Request{
			Meta:      &updatepb.Meta{UpdateId: uuid.NewString()},
			Input:     &updatepb.Input{Name: "update"},
			RequestId: requestID,
		},
	})
	s.NoError(err)
	s.NotNil(resp.GetOutcome().GetFailure(), "expected handler failure in outcome")

	// Must be a WorkflowEvent link (not a Workflow link), because the update was accepted.
	we := resp.GetLink().GetWorkflowEvent()
	s.NotNil(we, "accepted-then-failed update must produce a WorkflowEvent backlink, not a Workflow backlink")
	s.Equal(run.GetID(), we.GetWorkflowId())
	ref := we.GetRequestIdRef()
	s.NotNil(ref)
	s.Equal(requestID, ref.GetRequestId())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, ref.GetEventType())
}

func (s *LinksSuite) TestSignalWithStartWorkflowExecution_LinksAttachedToRelevantEvents() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	workflowID := testcore.RandomizeStr(s.T().Name())

	// TODO(bergundy): Use SdkClient if and when it exposes links on SignalWithStartWorkflow.
	request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		WorkflowType: &commonpb.WorkflowType{
			Name: "dont-care",
		},
		SignalName: "dont-care",
		Identity:   "test",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "dont-care",
		},
		RequestId: uuid.NewString(),
		Links:     links,
	}
	_, err := env.FrontendClient().SignalWithStartWorkflowExecution(ctx, request)
	s.NoError(err)

	// Send a second request and verify that the new signal has links attached to it too.
	request.RequestId = uuid.NewString()
	_, err = env.FrontendClient().SignalWithStartWorkflowExecution(ctx, request)
	s.NoError(err)

	history := env.SdkClient().GetWorkflowHistory(ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundStartEvent := false
	foundFirstSignal := false
	foundSecondSignal := false
	for history.HasNext() {
		event, err := history.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			if foundFirstSignal {
				foundSecondSignal = true
			} else {
				foundFirstSignal = true
			}
			protorequire.ProtoSliceEqual(s.T(), links, event.Links)
		}
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			foundStartEvent = true
			protorequire.ProtoSliceEqual(s.T(), links, event.Links)
		}
	}
	s.True(foundStartEvent)
	s.True(foundFirstSignal)
	s.True(foundSecondSignal)
}
