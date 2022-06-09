package host

import (
	"context"
	"fmt"
	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
	"time"
)

func (s *integrationSuite) TestBasicVersionUpdate() {
	ctx := NewContext()
	tq := "integration-versioning-basic"
	s.prepareQueue(ctx, tq)

	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &workflow.VersionId{Version: &workflow.VersionId_WorkerBuildId{WorkerBuildId: "foo"}},
		PreviousCompatible: nil,
		BecomeDefault:      true,
	})
	s.NoError(err)
	s.NotNil(res)

	res2, err := s.engine.GetWorkerBuildIdOrdering(ctx, &workflowservice.GetWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal(len(res2.CurrentDefaults), 1)
	s.Equal(res2.CurrentDefaults[0].GetVersion().GetWorkerBuildId(), "foo")
}

func (s *integrationSuite) TestSeriesOfUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-series"
	s.prepareQueue(ctx, tq)

	for i := 0; i < 10; i++ {
		res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
			Namespace: s.namespace,
			TaskQueue: tq,
			VersionId: &workflow.VersionId{
				Version: &workflow.VersionId_WorkerBuildId{WorkerBuildId: fmt.Sprintf("foo-%d", i)}},
			PreviousCompatible: nil,
			BecomeDefault:      true,
		})
		s.NoError(err)
		s.NotNil(res)
	}
	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		VersionId: &workflow.VersionId{
			Version: &workflow.VersionId_WorkerBuildId{WorkerBuildId: "foo-2.1"}},
		PreviousCompatible: &workflow.VersionId{
			Version: &workflow.VersionId_WorkerBuildId{WorkerBuildId: "foo-2"}},
		BecomeDefault: false,
	})
	s.NoError(err)
	s.NotNil(res)

	res2, err := s.engine.GetWorkerBuildIdOrdering(ctx, &workflowservice.GetWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal(len(res2.CurrentDefaults), 1)
	s.Equal(res2.CurrentDefaults[0].GetVersion().GetWorkerBuildId(), "foo-9")
	s.Equal(len(res2.CompatibleLeaves), 1)
	s.Equal(res2.CompatibleLeaves[0].GetVersion().GetWorkerBuildId(), "foo-2.1")
}

func (s *integrationSuite) TestLinkToNonexistentCompatibleVersionReturnsNotFound() {
	ctx := NewContext()
	tq := "integration-versioning-compat-not-found"
	s.prepareQueue(ctx, tq)

	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &workflow.VersionId{Version: &workflow.VersionId_WorkerBuildId{WorkerBuildId: "foo"}},
		PreviousCompatible: &workflow.VersionId{Version: &workflow.VersionId_WorkerBuildId{WorkerBuildId: "i don't exist yo"}},
	})
	s.Error(err)
	s.Nil(res)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *integrationSuite) TestVersioningStateNotDestroyedByOtherUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-not-destroyed"
	s.prepareQueue(ctx, tq)

	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &workflow.VersionId{Version: &workflow.VersionId_WorkerBuildId{WorkerBuildId: "foo"}},
		PreviousCompatible: nil,
		BecomeDefault:      true,
	})
	s.NoError(err)
	s.NotNil(res)

	isFirst := true
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		// TODO: This timer is long to ensure the 1-minute lease-renewal on the task queue happens, to verify that
		//   doesn't blow up data. There must be a faster way to do that.
		if isFirst {
			isFirst = false
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer-id-1",
					StartToFireTimeout: timestamp.DurationPtr(70 * time.Second),
				}},
			}}, nil
		}
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq},
		Identity:            "whatever",
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}
	_, errWorkflowTask := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(errWorkflowTask)
	_, errWorkflowTask = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(errWorkflowTask)

	res2, err := s.engine.GetWorkerBuildIdOrdering(ctx, &workflowservice.GetWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal(len(res2.CurrentDefaults), 1)
	s.Equal(res2.CurrentDefaults[0].GetVersion().GetWorkerBuildId(), "foo")
}

func (s *integrationSuite) prepareQueue(ctx context.Context, tq string) {
	workflowID := "integration-versioning-queuemaker"
	wt := "integration-versioning-queuemaker"
	identity := "worker1"

	// Make sure the task queue exists by starting a workflow on it
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	// start workflow task, to ensure that the task queue exists
	// TODO: Should not be necessary - see https://github.com/temporalio/temporal/issues/2969
	we, err := s.engine.StartWorkflowExecution(ctx, request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	_, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq},
		Identity:  identity,
	})
	s.NoError(err1)
}
