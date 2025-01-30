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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowTestSuite struct {
	testcore.FunctionalTestSuite
}

func TestWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowTestSuite))
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution() {
	wt := "functional-start-workflow-test-type"
	tl := "functional-start-workflow-test-taskqueue"

	makeRequest := func() *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.New(),
			Namespace:          s.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       &commonpb.WorkflowType{Name: wt},
			TaskQueue:          &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           "worker1",
		}
	}

	s.Run("start", func() {
		request := makeRequest()
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
		s.True(we.Started)

		// Validate the default value for WorkflowTaskTimeoutSeconds
		historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: request.WorkflowId,
			RunId:      we.RunId,
		})
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled`, historyEvents)
	})

	s.Run("start twice - same request", func() {
		request := makeRequest()

		we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err0)
		s.True(we0.Started)

		we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err1)
		s.True(we1.Started)

		s.Equal(we0.RunId, we1.RunId)
	})

	s.Run("fail when already started", func() {
		request := makeRequest()
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
		s.True(we.Started)

		request.RequestId = uuid.New()

		we2, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.Error(err)
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		s.ErrorAs(err, &alreadyStarted)
		s.Nil(we2)
	})
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting() {
	id := "functional-start-workflow-use-existing-test"
	wt := &commonpb.WorkflowType{Name: "functional-start-workflow-use-existing-test-type"}
	tq := &taskqueuepb.TaskQueue{
		Name: "functional-start-workflow-use-existing-test-taskqueue",
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         id,
		WorkflowType:       wt,
		TaskQueue:          tq,
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           "worker1",
	}

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.True(we0.Started)

	request.RequestId = uuid.New()
	request.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	s.False(we1.Started)
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions() {
	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "some-secure-address", "AllowInsecure": false}},
	)
	cb := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: "https://some-secure-address",
			},
		},
	}

	testCases := []struct {
		name                     string
		WorkflowIdConflictPolicy enumspb.WorkflowIdConflictPolicy
		OnConflictOptions        *workflowpb.OnConflictOptions
		ErrMessage               string
		MaxCallbacksPerWorkflow  int
	}{
		{
			name:                     "OnConflictOptions nil",
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions:        nil,
		},
		{
			name:                     "OnConflictOptions attach request id",
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &workflowpb.OnConflictOptions{
				AttachRequestId: true,
			},
		},
		{
			name:                     "OnConflictOptions attach request id, links, callbacks",
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &workflowpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			},
		},
		{
			name:                     "OnConflictOptions failed max callbacks per workflow",
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &workflowpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			},
			ErrMessage:              "cannot attach more than 1 callbacks to a workflow (1 callbacks already attached)",
			MaxCallbacksPerWorkflow: 1,
		},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.MaxCallbacksPerWorkflow > 0 {
				s.OverrideDynamicConfig(
					dynamicconfig.MaxCallbacksPerWorkflow,
					tc.MaxCallbacksPerWorkflow,
				)
			}

			id := fmt.Sprintf("functional-start-workflow-use-existing-on-conflict-options-test-%v", i)
			wt := &commonpb.WorkflowType{Name: "functional-start-workflow-use-existing-on-conflict-options-test-type"}
			tq := &taskqueuepb.TaskQueue{
				Name: "functional-start-workflow-use-existing-on-conflict-options-test-taskqueue",
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}

			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           uuid.New(),
				Namespace:           s.Namespace().String(),
				WorkflowId:          id,
				WorkflowType:        wt,
				TaskQueue:           tq,
				Input:               nil,
				WorkflowRunTimeout:  durationpb.New(100 * time.Second),
				Identity:            "worker1",
				CompletionCallbacks: []*commonpb.Callback{cb},
			}

			we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err0)
			s.True(we0.Started)

			historyEvents := s.GetHistory(
				s.Namespace().String(),
				&commonpb.WorkflowExecution{WorkflowId: request.WorkflowId, RunId: we0.RunId},
			)
			s.EqualHistoryEvents(
				`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
				`,
				historyEvents,
			)

			request.RequestId = uuid.New()
			request.Links = []*commonpb.Link{{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "dont-care",
						WorkflowId: "whatever",
						RunId:      uuid.New(),
					},
				},
			}}
			request.CompletionCallbacks = []*commonpb.Callback{cb}
			request.WorkflowIdConflictPolicy = tc.WorkflowIdConflictPolicy
			request.OnConflictOptions = tc.OnConflictOptions
			we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			if tc.ErrMessage != "" {
				s.ErrorContains(err1, tc.ErrMessage)
				return
			}
			s.NoError(err1)
			s.Equal(we0.RunId, we1.RunId)
			s.False(we1.Started)

			if tc.OnConflictOptions == nil {
				historyEvents := s.GetHistory(
					s.Namespace().String(),
					&commonpb.WorkflowExecution{WorkflowId: request.WorkflowId, RunId: we0.RunId},
				)
				s.EqualHistoryEvents(
					`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
					`,
					historyEvents,
				)
			} else {
				historyEvents := s.GetHistory(
					s.Namespace().String(),
					&commonpb.WorkflowExecution{WorkflowId: request.WorkflowId, RunId: we0.RunId},
				)
				s.EqualHistoryEvents(
					`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated // event attributes are checked below
					`,
					historyEvents,
				)

				var wfOptionsUpdated *historypb.HistoryEvent
				for _, e := range historyEvents {
					if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
						wfOptionsUpdated = e
						break
					}
				}
				s.NotNil(wfOptionsUpdated)

				attributes := wfOptionsUpdated.GetWorkflowExecutionOptionsUpdatedEventAttributes()
				s.Nil(attributes.GetVersioningOverride())
				s.False(attributes.GetUnsetVersioningOverride())

				if tc.OnConflictOptions.AttachRequestId {
					s.Equal(request.RequestId, attributes.GetAttachedRequestId())
				} else {
					s.Empty(attributes.GetAttachedRequestId())
				}

				if tc.OnConflictOptions.AttachCompletionCallbacks {
					s.ProtoElementsMatch(request.CompletionCallbacks, attributes.GetAttachedCompletionCallbacks())
				} else {
					s.Empty(attributes.GetAttachedCompletionCallbacks())
				}

				if tc.OnConflictOptions.AttachLinks {
					s.ProtoElementsMatch(request.Links, wfOptionsUpdated.GetLinks())
				} else {
					s.Empty(wfOptionsUpdated.GetLinks())
				}
			}

			descResp, err := s.FrontendClient().DescribeWorkflowExecution(
				testcore.NewContext(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: id,
						RunId:      we0.RunId,
					},
				},
			)
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)
		})
	}
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions_Dedup() {
	id := "functional-start-workflow-use-existing-on-conflict-options-dedup-test"
	wt := &commonpb.WorkflowType{
		Name: "functional-start-workflow-use-existing-on-conflict-options-dedup-test-type",
	}
	tq := &taskqueuepb.TaskQueue{
		Name: "functional-start-workflow-use-existing-on-conflict-options-dedup-test-taskqueue",
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         id,
		WorkflowType:       wt,
		TaskQueue:          tq,
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           "worker1",
	}

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.True(we0.Started)

	request.RequestId = uuid.New()
	request.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	request.OnConflictOptions = &workflowpb.OnConflictOptions{
		AttachRequestId: true,
	}
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	s.False(we1.Started)

	we2, err2 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err2)
	s.Equal(we0.RunId, we2.RunId)
	s.False(we2.Started)

	historyEvents := s.GetHistory(
		s.Namespace().String(),
		&commonpb.WorkflowExecution{WorkflowId: request.WorkflowId, RunId: we0.RunId},
	)
	s.EqualHistoryEvents(
		fmt.Sprintf(
			`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated {"VersioningOverride": null, "UnsetVersioningOverride": false, "AttachedRequestId": "%s", "AttachedCompletionCallbacks": null}
			`,
			request.RequestId,
		),
		historyEvents,
	)
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_Terminate() {
	// setting this to 0 to be sure we are terminating old workflow
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	testCases := []struct {
		name                     string
		WorkflowIdReusePolicy    enumspb.WorkflowIdReusePolicy
		WorkflowIdConflictPolicy enumspb.WorkflowIdConflictPolicy
	}{
		{
			"TerminateIfRunning id workflow reuse policy",
			enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
			enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
		},
		{
			"TerminateExisting id workflow conflict policy",
			enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
			enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
		},
		{
			"TerminateExisting with AllowDuplicateFailedOnly",
			enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
			enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
		},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			id := fmt.Sprintf("functional-start-workflow-terminate-test-%v", i)

			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:          uuid.New(),
				Namespace:          s.Namespace().String(),
				WorkflowId:         id,
				WorkflowType:       &commonpb.WorkflowType{Name: "functional-start-workflow-terminate-test-type"},
				TaskQueue:          &taskqueuepb.TaskQueue{Name: "functional-start-workflow-terminate-test-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:              nil,
				WorkflowRunTimeout: durationpb.New(100 * time.Second),
				Identity:           "worker1",
			}

			we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err0)

			request.RequestId = uuid.New()
			request.WorkflowIdReusePolicy = tc.WorkflowIdReusePolicy
			request.WorkflowIdConflictPolicy = tc.WorkflowIdConflictPolicy
			we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err1)
			s.NotEqual(we0.RunId, we1.RunId)

			descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we0.RunId,
				},
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

			descResp, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we1.RunId,
				},
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)
		})
	}
}

func (s *WorkflowTestSuite) TestStartWorkflowExecutionWithDelay() {
	id := "functional-start-workflow-with-delay-test"
	wt := "functional-start-workflow-with-delay-test-type"
	tl := "functional-start-workflow-with-delay-test-taskqueue"
	stickyTq := "functional-start-workflow-with-delay-test-sticky-taskqueue"
	identity := "worker1"

	startDelay := 3 * time.Second

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           identity,
		WorkflowStartDelay: durationpb.New(startDelay),
	}

	reqStartTime := time.Now()
	we0, startErr := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(startErr)

	delayEndTime := time.Now()
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		delayEndTime = time.Now()
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		StickyTaskQueue:     &taskqueuepb.TaskQueue{Name: stickyTq, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, pollErr := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(pollErr)
	s.GreaterOrEqual(delayEndTime.Sub(reqStartTime), startDelay)

	descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we0.RunId,
		},
	})
	s.NoError(descErr)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
}

func (s *WorkflowTestSuite) TestTerminateWorkflow() {
	id := "functional-terminate-workflow-test"
	wt := "functional-terminate-workflow-test-type"
	tl := "functional-terminate-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:   "terminate reason",
		Details:  payloads.EncodeString("terminate details"),
		Identity: identity,
	})
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
			s.Logger.Warn("Execution not terminated yet")
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue GetHistoryLoop
		}
		break GetHistoryLoop
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowExecutionTerminated {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"terminate reason"}`, historyEvents)

	newExecutionStarted := false
StartNewExecutionLoop:
	for i := 0; i < 10; i++ {
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.New(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: wt},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			Identity:            identity,
		}

		newExecution, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		if err != nil {
			s.Logger.Warn("Start New Execution failed. Error", tag.Error(err))
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue StartNewExecutionLoop
		}

		s.Logger.Info("New Execution Started with the same ID", tag.WorkflowID(id),
			tag.WorkflowRunID(newExecution.RunId))
		newExecutionStarted = true
		break StartNewExecutionLoop
	}

	s.True(newExecutionStarted)
}

func (s *WorkflowTestSuite) TestSequentialWorkflow() {
	id := "functional-sequential-workflow-test"
	wt := "functional-sequential-workflow-test-type"
	tl := "functional-sequential-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(10)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
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

	expectedActivity := int32(1)
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.EqualValues(id, task.WorkflowExecution.WorkflowId)
		s.Equal(activityName, task.ActivityType.Name)
		id, _ := strconv.Atoi(task.ActivityId)
		s.Equal(int(expectedActivity), id)
		s.Equal(expectedActivity, s.DecodePayloadsByteSliceInt32(task.Input))
		expectedActivity++

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
		if i%2 == 0 {
			err = poller.PollAndProcessActivityTask(false)
		} else { // just for testing respondActivityTaskCompleteByID
			err = poller.PollAndProcessActivityTaskWithID(false)
		}
		s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
		s.NoError(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *WorkflowTestSuite) TestCompleteWorkflowTaskAndCreateNewOne() {
	id := "functional-complete-workflow-task-create-new-test"
	wt := "functional-complete-workflow-task-create-new-test-type"
	tl := "functional-complete-workflow-task-create-new-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	commandCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if commandCount < 2 {
			commandCount++
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "test-marker",
				}},
			}}, nil
		}

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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	res, err := poller.PollAndProcessWorkflowTask(testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)

	s.Equal(int64(3), newTask.WorkflowTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.WorkflowTask.GetStartedEventId())
	s.Equal(4, len(newTask.WorkflowTask.History.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_MARKER_RECORDED, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())
}

func (s *WorkflowTestSuite) TestWorkflowTaskAndActivityTaskTimeoutsWorkflow() {
	id := "functional-timeouts-workflow-test"
	wt := "functional-timeouts-workflow-test-type"
	tl := "functional-timeouts-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(4)
	activityCounter := int32(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
					ScheduleToStartTimeout: durationpb.New(1 * time.Second),
					StartToCloseTimeout:    durationpb.New(1 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
				}},
			}}, nil
		}

		s.Logger.Info("Completing enums")

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.EqualValues(id, task.WorkflowExecution.WorkflowId)
		s.Equal(activityName, task.ActivityType.Name)
		s.Logger.Info("Activity ID", tag.WorkflowActivityID(task.ActivityId))
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := 0; i < 8; i++ {
		dropWorkflowTask := (i%2 == 0)
		s.Logger.Info("Calling Workflow Task", tag.Counter(i))
		var err error
		if dropWorkflowTask {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithDropTask)
		} else {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithExpectedAttemptCount(2))
		}
		if err != nil {
			s.PrintHistoryEventsCompact(s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			}))
		}
		s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
		if !dropWorkflowTask {
			s.Logger.Info("Calling PollAndProcessActivityTask", tag.Counter(i))
			err = poller.PollAndProcessActivityTask(i%4 == 0)
			s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *WorkflowTestSuite) TestWorkflowRetry() {
	id := "functional-wf-retry-test"
	wt := "functional-wf-retry-type"
	tl := "functional-wf-retry-taskqueue"
	identity := "worker1"

	initialInterval := 1 * time.Second
	backoffCoefficient := 1.5
	maximumAttempts := 5
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			// Intentionally test server-initialization of Initial Interval value (which should be 1 second)
			MaximumAttempts:        int32(maximumAttempts),
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     backoffCoefficient,
		},
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution

	attemptCount := 1

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)
		attemptCount++
		if attemptCount > maximumAttempts {
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("succeed-after-retry"),
					}},
				}}, nil
		}
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: failure.NewServerFailure("retryable-error", false),
				}},
			}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	describeWorkflowExecution := func(execution *commonpb.WorkflowExecution) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: execution,
		})
	}

	for i := 1; i <= maximumAttempts; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
		events := s.GetHistory(s.Namespace().String(), executions[i-1])
		if i == maximumAttempts {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"Attempt":%d}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, i), events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"Attempt":%d}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, i), events)
		}

		dweResponse, err := describeWorkflowExecution(executions[i-1])
		s.NoError(err)
		backoff := time.Duration(0)
		if i > 1 {
			backoff = time.Duration(initialInterval.Seconds()*math.Pow(backoffCoefficient, float64(i-2))) * time.Second
			// retry backoff cannot larger than MaximumIntervalInSeconds
			if backoff > time.Second {
				backoff = time.Second
			}
		}
		expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime().AsTime().Add(backoff)
		s.Equal(expectedExecutionTime, timestamp.TimeValue(dweResponse.WorkflowExecutionInfo.GetExecutionTime()))
		s.Equal(we.RunId, dweResponse.WorkflowExecutionInfo.GetFirstRunId())
	}

	// Check run id links
	for i := 0; i < maximumAttempts; i++ {
		events := s.GetHistory(s.Namespace().String(), executions[i])
		if i == 0 {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":""}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed {"NewExecutionRunId":"%s"}`, executions[i+1].RunId), events)
		} else if i == maximumAttempts-1 {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":"%s"}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted {"NewExecutionRunId":""}`, executions[i-1].RunId), events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":"%s"}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed {"NewExecutionRunId":"%s"}`, executions[i-1].RunId, executions[i+1].RunId), events)
		}

		// Test get history from old SDKs
		// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
		// See comment in workflowHandler.go:GetWorkflowExecutionHistory
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		oldSDKCtx := headers.SetVersionsForTests(ctx, "1.3.1", headers.ClientNameJavaSDK, headers.SupportedServerVersions, "")
		resp, err := s.FrontendClient().GetWorkflowExecutionHistory(oldSDKCtx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:              s.Namespace().String(),
			Execution:              executions[i],
			MaximumPageSize:        5,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		})
		cancel()
		s.NoError(err)
		events = resp.History.Events
		if i == maximumAttempts-1 {
			s.EqualHistoryEvents(`
  5 WorkflowExecutionCompleted {"NewExecutionRunId":""}`, events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  5 WorkflowExecutionContinuedAsNew {"NewExecutionRunId":"%s"}`, executions[i+1].RunId), events)
		}
	}
}

func (s *WorkflowTestSuite) TestWorkflowRetryFailures() {
	id := "functional-wf-retry-failures-test"
	wt := "functional-wf-retry-failures-type"
	tl := "functional-wf-retry-failures-taskqueue"
	identity := "worker1"

	workflowImpl := func(attempts int, errorReason string, nonRetryable bool, executions *[]*commonpb.WorkflowExecution) testcore.WorkflowTaskHandler {
		attemptCount := 1

		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			*executions = append(*executions, task.WorkflowExecution)
			attemptCount++
			if attemptCount > attempts {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("succeed-after-retry"),
						}},
					}}, nil
			}
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
						// Reason:  "retryable-error",
						Failure: failure.NewServerFailure(errorReason, nonRetryable),
					}},
				}}, nil
		}

		return wtHandler
	}

	// Fail using attempt
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		},
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution
	wtHandler := workflowImpl(5, "retryable-error", false, &executions)
	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := s.GetHistory(s.Namespace().String(), executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.GetHistory(s.Namespace().String(), executions[1])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":2}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.GetHistory(s.Namespace().String(), executions[2])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":3}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	// Fail error reason
	request = &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		},
	}

	we, err0 = s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	executions = []*commonpb.WorkflowExecution{}
	wtHandler = workflowImpl(5, "bad-bug", true, &executions)
	poller = &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.GetHistory(s.Namespace().String(), executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)
}
