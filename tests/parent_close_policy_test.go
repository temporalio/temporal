package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ParentClosePolicySuite struct {
	testcore.FunctionalTestBase
}

func TestParentClosePolicySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ParentClosePolicySuite))
}

func (s *ParentClosePolicySuite) SetupSuite() {
	s.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableParentClosePolicyTransferTasks.Key(): true,
		}),
	)
}

// TestParentClosePolicyTerminate tests that when a parent workflow is terminated,
// its child workflow with TERMINATE parent close policy is terminated via the
// ParentClosePolicyTask transfer task.
func (s *ParentClosePolicySuite) TestParentClosePolicyTerminate() {
	parentID := "parent-close-policy-terminate-parent"
	childID := "parent-close-policy-terminate-child"
	wtParent := "parent-close-policy-terminate-parent-type"
	wtChild := "parent-close-policy-terminate-child-type"
	tv := testvars.New(s.T())

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueue := tv.TaskQueue()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	childStarted := false

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !childStarted {
			childStarted = true
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
						StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
							WorkflowId:          childID,
							WorkflowType:        childWorkflowType,
							TaskQueue:           taskQueue,
							Input:               payloads.EncodeString("child-input"),
							WorkflowRunTimeout:  durationpb.New(200 * time.Second),
							WorkflowTaskTimeout: durationpb.New(2 * time.Second),
							ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_TERMINATE,
						},
					},
				}},
			}, nil
		}
		// Wait for child started event
		for _, event := range task.History.Events {
			if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
				// Just wait - don't complete, let parent be terminated
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			}
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.NoError(err)
	s.True(childStarted)

	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.NoError(err)

	childDescribe, err := s.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
		},
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, childDescribe.WorkflowExecutionInfo.Status)

	_, err = s.FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: parentID},
			Reason:            "test termination",
		},
	)
	s.NoError(err)

	s.Eventually(func() bool {
		resp, err := s.FrontendClient().DescribeWorkflowExecution(
			testcore.NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
			},
		)
		if err != nil {
			return false
		}
		return resp.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	}, 10*time.Second, 200*time.Millisecond, "child workflow should be terminated")
}

// TestParentClosePolicyRequestCancel tests that when a parent workflow is terminated,
// its child workflow with REQUEST_CANCEL parent close policy receives a cancellation request
// via the ParentClosePolicyTask transfer task.
func (s *ParentClosePolicySuite) TestParentClosePolicyRequestCancel() {
	parentID := "parent-close-policy-cancel-parent"
	childID := "parent-close-policy-cancel-child"
	wtParent := "parent-close-policy-cancel-parent-type"
	wtChild := "parent-close-policy-cancel-child-type"
	tv := testvars.New(s.T())

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueue := tv.TaskQueue()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	childStarted := false
	childCancellationRequested := false

	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if task.WorkflowExecution.WorkflowId != parentID {
			return nil, nil
		}
		if !childStarted {
			childStarted = true
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
						StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
							WorkflowId:          childID,
							WorkflowType:        childWorkflowType,
							TaskQueue:           taskQueue,
							Input:               payloads.EncodeString("child-input"),
							WorkflowRunTimeout:  durationpb.New(200 * time.Second),
							WorkflowTaskTimeout: durationpb.New(2 * time.Second),
							ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
						},
					},
				}},
			}, nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if task.WorkflowExecution.WorkflowId != childID {
			return nil, nil
		}
		for _, event := range task.History.Events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				childCancellationRequested = true
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{
							CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{},
						},
					}},
				}, nil
			}
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	combinedHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if task.WorkflowExecution.WorkflowId == parentID {
			return parentHandler(task)
		}
		return childHandler(task)
	}

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	_, err = poller.PollAndHandleWorkflowTask(tv, combinedHandler)
	s.NoError(err)
	s.True(childStarted)

	_, err = poller.PollAndHandleWorkflowTask(tv, combinedHandler)
	s.NoError(err)

	_, err = poller.PollAndHandleWorkflowTask(tv, combinedHandler)
	s.NoError(err)

	childDescribe, err := s.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
		},
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, childDescribe.WorkflowExecutionInfo.Status)

	_, err = s.FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: parentID},
			Reason:            "test termination",
		},
	)
	s.NoError(err)

	_, err = poller.PollAndHandleWorkflowTask(tv, combinedHandler)
	s.NoError(err)
	s.True(childCancellationRequested)

	s.Eventually(func() bool {
		resp, err := s.FrontendClient().DescribeWorkflowExecution(
			testcore.NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
			},
		)
		if err != nil {
			return false
		}
		return resp.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
	}, 10*time.Second, 200*time.Millisecond, "child workflow should be canceled")
}

// TestParentClosePolicyAbandon tests that when a parent workflow is terminated,
// its child workflow with ABANDON parent close policy continues running.
func (s *ParentClosePolicySuite) TestParentClosePolicyAbandon() {
	parentID := "parent-close-policy-abandon-parent"
	childID := "parent-close-policy-abandon-child"
	wtParent := "parent-close-policy-abandon-parent-type"
	wtChild := "parent-close-policy-abandon-child-type"
	tv := testvars.New(s.T())

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueue := tv.TaskQueue()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	childStarted := false

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !childStarted {
			childStarted = true
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
						StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
							WorkflowId:          childID,
							WorkflowType:        childWorkflowType,
							TaskQueue:           taskQueue,
							Input:               payloads.EncodeString("child-input"),
							WorkflowRunTimeout:  durationpb.New(200 * time.Second),
							WorkflowTaskTimeout: durationpb.New(2 * time.Second),
							ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_ABANDON,
						},
					},
				}},
			}, nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.NoError(err)
	s.True(childStarted)

	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.NoError(err)

	_, err = s.FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: parentID},
			Reason:            "test termination",
		},
	)
	s.NoError(err)

	// Wait to verify parent close policy would have been applied
	//nolint:forbidigo // negative test: verifying ABANDON policy doesn't affect child
	time.Sleep(2 * time.Second)

	childDescribe, err := s.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
		},
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, childDescribe.WorkflowExecutionInfo.Status)

	_, _ = s.FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: childID},
			Reason:            "cleanup",
		},
	)
}

// TestParentClosePolicyMultipleChildren tests that when a parent workflow with multiple
// children is terminated, each child is handled according to its own parent close policy.
func (s *ParentClosePolicySuite) TestParentClosePolicyMultipleChildren() {
	parentID := "parent-close-policy-multi-parent"
	childTerminateID := "parent-close-policy-multi-child-terminate"
	childCancelID := "parent-close-policy-multi-child-cancel"
	childAbandonID := "parent-close-policy-multi-child-abandon"
	wtParent := "parent-close-policy-multi-parent-type"
	wtChild := "parent-close-policy-multi-child-type"
	tv := testvars.New(s.T())

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueue := tv.TaskQueue()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	childrenStarted := 0
	childCancelRequested := false

	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if task.WorkflowExecution.WorkflowId != parentID {
			return nil, nil
		}
		if childrenStarted == 0 {
			childrenStarted = 3
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
							StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
								WorkflowId:          childTerminateID,
								WorkflowType:        childWorkflowType,
								TaskQueue:           taskQueue,
								Input:               payloads.EncodeString("terminate-child"),
								WorkflowRunTimeout:  durationpb.New(200 * time.Second),
								WorkflowTaskTimeout: durationpb.New(2 * time.Second),
								ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_TERMINATE,
							},
						},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
							StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
								WorkflowId:          childCancelID,
								WorkflowType:        childWorkflowType,
								TaskQueue:           taskQueue,
								Input:               payloads.EncodeString("cancel-child"),
								WorkflowRunTimeout:  durationpb.New(200 * time.Second),
								WorkflowTaskTimeout: durationpb.New(2 * time.Second),
								ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
							},
						},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
							StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
								WorkflowId:          childAbandonID,
								WorkflowType:        childWorkflowType,
								TaskQueue:           taskQueue,
								Input:               payloads.EncodeString("abandon-child"),
								WorkflowRunTimeout:  durationpb.New(200 * time.Second),
								WorkflowTaskTimeout: durationpb.New(2 * time.Second),
								ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_ABANDON,
							},
						},
					},
				},
			}, nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		wfID := task.WorkflowExecution.WorkflowId
		if wfID == parentID {
			return parentHandler(task)
		}
		for _, event := range task.History.Events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				childCancelRequested = true
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{
							CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{},
						},
					}},
				}, nil
			}
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	// Start all children
	_, err = poller.PollAndHandleWorkflowTask(tv, childHandler)
	s.NoError(err)
	s.Equal(3, childrenStarted)

	// Process child started events (3 children = 3 events, but may be batched)
	for i := 0; i < 5; i++ {
		_, _ = poller.PollAndHandleWorkflowTask(tv, childHandler)
	}

	// Verify all children are running
	for _, childID := range []string{childTerminateID, childCancelID, childAbandonID} {
		desc, err := s.FrontendClient().DescribeWorkflowExecution(
			testcore.NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
			},
		)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, desc.WorkflowExecutionInfo.Status,
			"child %s should be running", childID)
	}

	// Terminate parent workflow
	_, err = s.FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: parentID},
			Reason:            "test termination",
		},
	)
	s.NoError(err)

	// Process cancellation for cancel child
	_, _ = poller.PollAndHandleWorkflowTask(tv, childHandler)
	s.True(childCancelRequested, "cancel child should have received cancellation request")

	// Verify terminate child is terminated
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().DescribeWorkflowExecution(
			testcore.NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: childTerminateID},
			},
		)
		if err != nil {
			return false
		}
		return resp.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	}, 10*time.Second, 200*time.Millisecond, "terminate child should be terminated")

	// Verify cancel child is canceled
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().DescribeWorkflowExecution(
			testcore.NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: childCancelID},
			},
		)
		if err != nil {
			return false
		}
		return resp.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
	}, 10*time.Second, 200*time.Millisecond, "cancel child should be canceled")

	// Verify abandon child is still running
	abandonDesc, err := s.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: childAbandonID},
		},
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, abandonDesc.WorkflowExecutionInfo.Status)

	// Clean up: terminate the abandon child
	_, _ = s.FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: childAbandonID},
			Reason:            "cleanup",
		},
	)
}

// TestParentClosePolicyChildAlreadyCompleted tests that parent close policy
// handles children that have already completed gracefully.
func (s *ParentClosePolicySuite) TestParentClosePolicyChildAlreadyCompleted() {
	parentID := "parent-close-policy-completed-parent"
	childID := "parent-close-policy-completed-child"
	wtParent := "parent-close-policy-completed-parent-type"
	wtChild := "parent-close-policy-completed-child-type"
	tv := testvars.New(s.T())

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueue := tv.TaskQueue()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	childStarted := false

	// Parent workflow logic
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if task.WorkflowExecution.WorkflowId != parentID {
			return nil, nil
		}
		if !childStarted {
			childStarted = true
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
						StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
							WorkflowId:          childID,
							WorkflowType:        childWorkflowType,
							TaskQueue:           taskQueue,
							Input:               payloads.EncodeString("child-input"),
							WorkflowRunTimeout:  durationpb.New(200 * time.Second),
							WorkflowTaskTimeout: durationpb.New(2 * time.Second),
							ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_TERMINATE,
						},
					},
				}},
			}, nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if task.WorkflowExecution.WorkflowId == childID {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("child-result"),
						},
					},
				}},
			}, nil
		}
		return parentHandler(task)
	}

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	// Start child execution
	_, err = poller.PollAndHandleWorkflowTask(tv, childHandler)
	s.NoError(err)
	s.True(childStarted)

	// Process child started event on parent
	_, err = poller.PollAndHandleWorkflowTask(tv, childHandler)
	s.NoError(err)

	// Process child's workflow task
	_, err = poller.PollAndHandleWorkflowTask(tv, childHandler)
	s.NoError(err)

	// Verify child is completed
	childDescribe, err := s.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
		},
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, childDescribe.WorkflowExecutionInfo.Status)

	// Terminate parent workflow when child is already completed.
	_, err = s.FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: parentID},
			Reason:            "test termination",
		},
	)
	s.NoError(err)

	// Verify parent is terminated (no errors from parent close policy)
	parentDescribe, err := s.FrontendClient().DescribeWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: parentID},
		},
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, parentDescribe.WorkflowExecutionInfo.Status)
}

// getHistoryEvents retrieves history events for a workflow execution
func (s *ParentClosePolicySuite) getHistoryEvents(wfID, runID string, eventTypes ...enumspb.EventType) []*historypb.HistoryEvent {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var events []*historypb.HistoryEvent
	iter := s.SdkClient().GetWorkflowHistory(ctx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		for _, et := range eventTypes {
			if event.GetEventType() == et {
				events = append(events, event)
				break
			}
		}
	}
	return events
}
