package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type UserTimersTestSuite struct {
	testcore.FunctionalTestBase
}

func TestUserTimersTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(UserTimersTestSuite))
}

func (s *UserTimersTestSuite) TestUserTimers_Sequential() {
	id := "functional-user-timers-sequential-test"
	wt := "functional-user-timers-sequential-test-type"
	tl := "functional-user-timers-sequential-test-taskqueue"
	identity := "worker1"

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	workflowComplete := false
	timerCount := int32(4)
	timerCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if timerCounter < timerCount {
			timerCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, timerCounter))
			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				StartTimerCommandAttributes: commandpb.StartTimerCommandAttributes_builder{
					TimerId:            fmt.Sprintf("timer-id-%d", timerCounter),
					StartToFireTimeout: durationpb.New(1 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := 0; i < 4; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask: completed")
		s.NoError(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}
