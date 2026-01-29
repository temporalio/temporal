package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestUserTimers(t *testing.T) {
	t.Run("Sequential", func(t *testing.T) {
		s := testcore.NewEnv(t)

		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          s.Tv().WorkflowID(),
			WorkflowType:        s.Tv().WorkflowType(),
			TaskQueue:           s.Tv().TaskQueue(),
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			Identity:            s.Tv().WorkerIdentity(),
		}

		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		workflowComplete := false
		timerCount := int32(4)
		timerCounter := int32(0)
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			if timerCounter < timerCount {
				timerCounter++
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, timerCounter))
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_TIMER,
					Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId:            fmt.Sprintf("timer-id-%d", timerCounter),
						StartToFireTimeout: durationpb.New(1 * time.Second),
					}},
				}}, nil
			}

			workflowComplete = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}

		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			ActivityTaskHandler: nil,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		for i := 0; i < 4; i++ {
			_, err := poller.PollAndProcessWorkflowTask()
			s.NoError(err)
		}

		s.False(workflowComplete)
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
		s.NoError(err)
		s.True(workflowComplete)
	})
}
