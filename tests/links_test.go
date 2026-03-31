package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
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
