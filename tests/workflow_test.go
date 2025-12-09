package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowTestSuite))
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution() {
	tv := testvars.New(s.T())
	makeRequest := func() *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          s.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}
	}

	s.Run("start", func() {
		request := makeRequest()
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
		requireStartedAndRunning(s.T(), we)
		s.ProtoEqual(
			&commonpb.Link_WorkflowEvent{
				Namespace:  s.Namespace().String(),
				WorkflowId: request.WorkflowId,
				RunId:      we.RunId,
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
			we.Link.GetWorkflowEvent(),
		)

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
		requireStartedAndRunning(s.T(), we0)
		s.ProtoEqual(
			&commonpb.Link_WorkflowEvent{
				Namespace:  s.Namespace().String(),
				WorkflowId: request.WorkflowId,
				RunId:      we0.RunId,
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
			we0.Link.GetWorkflowEvent(),
		)

		we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err1)
		requireStartedAndRunning(s.T(), we1)
		s.ProtoEqual(
			&commonpb.Link_WorkflowEvent{
				Namespace:  s.Namespace().String(),
				WorkflowId: request.WorkflowId,
				RunId:      we1.RunId,
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
			we1.Link.GetWorkflowEvent(),
		)

		s.Equal(we0.RunId, we1.RunId)
	})

	s.Run("fail when already started", func() {
		request := makeRequest()
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
		requireStartedAndRunning(s.T(), we)

		request.RequestId = uuid.NewString()

		we2, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.Error(err)
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		s.ErrorAs(err, &alreadyStarted)
		s.Nil(we2)
	})
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting() {
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we0.RunId,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		we0.Link.GetWorkflowEvent(),
	)

	request.RequestId = uuid.NewString()
	request.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we1.RunId,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		we1.Link.GetWorkflowEvent(),
	)
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions() {
	s.OverrideDynamicConfig(dynamicconfig.EnableRequestIdRefLinks, true)
	s.OverrideDynamicConfig(callbacks.AllowedAddresses, []any{
		map[string]any{"Pattern": "some-secure-address", "AllowInsecure": false},
		map[string]any{"Pattern": "some-random-address", "AllowInsecure": false},
	})
	cb1 := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: "https://some-secure-address",
			},
		},
	}
	cb2 := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: "https://some-random-address",
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

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.MaxCallbacksPerWorkflow > 0 {
				s.OverrideDynamicConfig(
					dynamicconfig.MaxCallbacksPerWorkflow,
					tc.MaxCallbacksPerWorkflow,
				)
			}

			tv := testvars.New(s.T())
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           uuid.NewString(),
				Namespace:           s.Namespace().String(),
				WorkflowId:          tv.WorkflowID(),
				WorkflowType:        tv.WorkflowType(),
				TaskQueue:           tv.TaskQueue(),
				Input:               nil,
				WorkflowRunTimeout:  durationpb.New(100 * time.Second),
				Identity:            tv.WorkerIdentity(),
				CompletionCallbacks: []*commonpb.Callback{cb1},
			}

			we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err0)
			requireStartedAndRunning(s.T(), we0)
			s.ProtoEqual(
				&commonpb.Link_WorkflowEvent{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.WorkflowId,
					RunId:      we0.RunId,
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   1,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						},
					},
				},
				we0.Link.GetWorkflowEvent(),
			)

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

			descResp, err := s.FrontendClient().DescribeWorkflowExecution(
				testcore.NewContext(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: tv.WorkflowID(),
						RunId:      we0.RunId,
					},
				},
			)
			s.NoError(err)
			s.Len(descResp.Callbacks, 1)
			s.ProtoEqual(cb1, descResp.Callbacks[0].Callback)

			request.RequestId = uuid.NewString()
			request.Links = []*commonpb.Link{{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "dont-care",
						WorkflowId: "whatever",
						RunId:      uuid.NewString(),
					},
				},
			}}
			request.CompletionCallbacks = []*commonpb.Callback{cb2}
			request.WorkflowIdConflictPolicy = tc.WorkflowIdConflictPolicy
			request.OnConflictOptions = tc.OnConflictOptions
			we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			if tc.ErrMessage != "" {
				s.ErrorContains(err1, tc.ErrMessage)
				historyEvents = s.GetHistory(
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

				return
			}
			s.NoError(err1)
			s.Equal(we0.RunId, we1.RunId)
			requireNotStartedButRunning(s.T(), we1)
			s.ProtoEqual(
				&commonpb.Link_WorkflowEvent{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.WorkflowId,
					RunId:      we1.RunId,
					Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
						RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
							RequestId: request.RequestId,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
						},
					},
				},
				we1.Link.GetWorkflowEvent(),
			)

			historyEvents = s.GetHistory(
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

			numCallbacks := 1
			expectedCallbacks := []*commonpb.Callback{cb1}
			if tc.OnConflictOptions.AttachCompletionCallbacks {
				s.ProtoElementsMatch(request.CompletionCallbacks, attributes.GetAttachedCompletionCallbacks())
				numCallbacks += len(request.CompletionCallbacks)
				expectedCallbacks = append(expectedCallbacks, request.CompletionCallbacks...)
			} else {
				s.Empty(attributes.GetAttachedCompletionCallbacks())
			}

			if tc.OnConflictOptions.AttachLinks {
				s.ProtoElementsMatch(request.Links, wfOptionsUpdated.GetLinks())
			} else {
				s.Empty(wfOptionsUpdated.GetLinks())
			}

			descResp, err = s.FrontendClient().DescribeWorkflowExecution(
				testcore.NewContext(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: tv.WorkflowID(),
						RunId:      we0.RunId,
					},
				},
			)
			s.NoError(err)
			s.Len(descResp.Callbacks, numCallbacks)
			descRespCallbacks := make([]*commonpb.Callback, len(descResp.Callbacks))
			for i, cb := range descResp.Callbacks {
				descRespCallbacks[i] = cb.Callback
			}
			s.ProtoElementsMatch(expectedCallbacks, descRespCallbacks)
		})
	}
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions_Dedup() {
	s.OverrideDynamicConfig(dynamicconfig.EnableRequestIdRefLinks, true)
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.RequestID(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we0.RunId,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		we0.Link.GetWorkflowEvent(),
	)

	request.RequestId = uuid.NewString()
	request.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	request.OnConflictOptions = &workflowpb.OnConflictOptions{
		AttachRequestId: true,
	}
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we1.RunId,
			Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
				RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
					RequestId: request.RequestId,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				},
			},
		},
		we1.Link.GetWorkflowEvent(),
	)

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

	we2, err2 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err2)
	s.Equal(we0.RunId, we2.RunId)
	requireNotStartedButRunning(s.T(), we2)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we2.RunId,
			Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
				RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
					RequestId: request.RequestId,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				},
			},
		},
		we2.Link.GetWorkflowEvent(),
	)

	// History events must be the same as before.
	historyEvents = s.GetHistory(
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

	// Original request must also be deduped.
	request.RequestId = tv.RequestID()
	request.OnConflictOptions = nil
	we3, err3 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err3)
	s.Equal(we0.RunId, we3.RunId)
	requireStartedAndRunning(s.T(), we3) // Original request was the start request.
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we3.RunId,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		we3.Link.GetWorkflowEvent(),
	)
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions_NoDedup() {
	s.OverrideDynamicConfig(dynamicconfig.EnableRequestIdRefLinks, true)
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we0.RunId,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		we0.Link.GetWorkflowEvent(),
	)

	// New RequestId, but not attaching it.
	request.RequestId = uuid.NewString()
	request.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we1.RunId,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		we1.Link.GetWorkflowEvent(),
	)

	// Since OnConflictOptions is nil, no history event is added.
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

	// Same request, but attaching request ID. Since it didn't attach it before, it won't be deduped.
	// This verifies that the current execution entry in DB also don't have the request ID attached.
	request.OnConflictOptions = &workflowpb.OnConflictOptions{
		AttachRequestId: true,
	}
	we2, err2 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err2)
	s.Equal(we0.RunId, we2.RunId)
	requireNotStartedButRunning(s.T(), we2)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we2.RunId,
			Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
				RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
					RequestId: request.RequestId,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				},
			},
		},
		we2.Link.GetWorkflowEvent(),
	)

	historyEvents = s.GetHistory(
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

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			tv := testvars.New(s.T())
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:          uuid.NewString(),
				Namespace:          s.Namespace().String(),
				WorkflowId:         tv.WorkflowID(),
				WorkflowType:       tv.WorkflowType(),
				TaskQueue:          tv.TaskQueue(),
				Input:              nil,
				WorkflowRunTimeout: durationpb.New(100 * time.Second),
				Identity:           tv.WorkerIdentity(),
			}

			we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err0)
			s.ProtoEqual(
				&commonpb.Link_WorkflowEvent{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.WorkflowId,
					RunId:      we0.RunId,
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   1,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						},
					},
				},
				we0.Link.GetWorkflowEvent(),
			)

			request.RequestId = uuid.NewString()
			request.WorkflowIdReusePolicy = tc.WorkflowIdReusePolicy
			request.WorkflowIdConflictPolicy = tc.WorkflowIdConflictPolicy
			we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err1)
			s.NotEqual(we0.RunId, we1.RunId)
			s.ProtoEqual(
				&commonpb.Link_WorkflowEvent{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.WorkflowId,
					RunId:      we1.RunId,
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   1,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						},
					},
				},
				we1.Link.GetWorkflowEvent(),
			)

			descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: tv.WorkflowID(),
					RunId:      we0.RunId,
				},
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

			descResp, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: tv.WorkflowID(),
					RunId:      we1.RunId,
				},
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)
		})
	}
}

func (s *WorkflowTestSuite) TestStartWorkflowExecutionWithDelay() {
	tv := testvars.New(s.T())
	startDelay := 3 * time.Second
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
		WorkflowStartDelay: durationpb.New(startDelay),
	}

	reqStartTime := time.Now()
	we0, startErr := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(startErr)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.WorkflowId,
			RunId:      we0.RunId,
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		we0.Link.GetWorkflowEvent(),
	)

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
		TaskQueue:           tv.TaskQueue(),
		StickyTaskQueue:     tv.StickyTaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
			WorkflowId: tv.WorkflowID(),
			RunId:      we0.RunId,
		},
	})
	s.NoError(descErr)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
}

func (s *WorkflowTestSuite) TestTerminateWorkflow() {
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
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
					ActivityId:             tv.WithActivityIDNumber(int(activityCounter)).ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
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
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		},
		Reason:   "terminate reason",
		Details:  payloads.EncodeString("terminate details"),
		Identity: tv.WorkerIdentity(),
	})
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
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

	s.EqualHistoryEvents(
		fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowExecutionTerminated {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"%s","Reason":"terminate reason"}`,
			tv.WorkerIdentity(),
		),
		historyEvents)

	newExecutionStarted := false
StartNewExecutionLoop:
	for i := 0; i < 10; i++ {
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          tv.WorkflowID(),
			WorkflowType:        tv.WorkflowType(),
			TaskQueue:           tv.TaskQueue(),
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			Identity:            tv.WorkerIdentity(),
		}

		newExecution, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		if err != nil {
			s.Logger.Warn("Start New Execution failed. Error", tag.Error(err))
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue StartNewExecutionLoop
		}

		s.Logger.Info("New Execution Started with the same ID", tag.WorkflowID(tv.WorkflowID()),
			tag.WorkflowRunID(newExecution.RunId))
		newExecutionStarted = true
		break StartNewExecutionLoop
	}

	s.True(newExecutionStarted)
}

func (s *WorkflowTestSuite) TestTerminateWorkflowOnMessageTooLargeFailure() {
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	testContext := testcore.NewContext()

	// start workflow execution
	we, err0 := s.FrontendClient().StartWorkflowExecution(testContext, request)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	s.NoError(err0)

	// start workflow task, but do not respond to it
	res, err := s.FrontendClient().PollWorkflowTaskQueue(testContext, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.Logger.Info("PollWorkflowTaskQueue", tag.Error(err))
	s.NoError(err)

	// send a signal to test buffered event case.
	_, err = s.FrontendClient().SignalWorkflowExecution(testContext, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		},
		SignalName: "buffered-signal",
	})
	s.NoError(err)

	// respond workflow task as failed with grpc message too large error
	_, err = s.FrontendClient().RespondWorkflowTaskFailed(testContext, &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: res.TaskToken,
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE,
	})
	s.NoError(err)

	historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      we.RunId,
	})

	// verify that workflow is terminated and buffered signal is flushed
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionTerminated`,
		historyEvents)
}

func (s *WorkflowTestSuite) TestSequentialWorkflow() {
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
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
					ActivityId:             tv.WithActivityIDNumber(int(activityCounter)).ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
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
		s.EqualValues(tv.WorkflowID(), task.WorkflowExecution.WorkflowId)
		s.Equal(tv.ActivityType().Name, task.ActivityType.Name)
		s.Equal(tv.WithActivityIDNumber(int(expectedActivity)).ActivityID(), task.ActivityId)
		s.Equal(expectedActivity, s.DecodePayloadsByteSliceInt32(task.Input))
		s.NotNil(task.RetryPolicy)
		s.Equal(int64(1), task.RetryPolicy.InitialInterval.Seconds) // server default
		expectedActivity++

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
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
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
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
					ActivityId:             tv.WithActivityIDNumber(int(activityCounter)).ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
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
		s.EqualValues(tv.WorkflowID(), task.WorkflowExecution.WorkflowId)
		s.Equal(tv.ActivityType().Name, task.ActivityType.Name)
		s.Logger.Info("Activity ID", tag.WorkflowActivityID(task.ActivityId))
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
				WorkflowId: tv.WorkflowID(),
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
	tv := testvars.New(s.T())
	initialInterval := 1 * time.Second
	backoffCoefficient := 1.5
	maximumAttempts := 5
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
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
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	tv := testvars.New(s.T())
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
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
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
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
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
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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

// TestStartWorkflowExecution_Invalid_DeploymentSearchAttributes verifies that Worker Deployment related
// search attributes cannot be used on StartWorkflowExecution.
func (s *WorkflowTestSuite) TestStartWorkflowExecution_Invalid_DeploymentSearchAttributes() {
	tv := testvars.New(s.T())
	makeRequest := func(saFieldName string) *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          s.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					saFieldName: payload.EncodeString("1.0.0"),
				},
			},
		}
	}

	s.Run(sadefs.TemporalWorkerDeploymentVersion, func() {
		request := makeRequest(sadefs.TemporalWorkerDeploymentVersion)
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.Run(sadefs.TemporalWorkerDeployment, func() {
		request := makeRequest(sadefs.TemporalWorkerDeployment)
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.Run(sadefs.TemporalWorkflowVersioningBehavior, func() {
		request := makeRequest(sadefs.TemporalWorkflowVersioningBehavior)
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	// These are currently allowed since they are in the predefinedWhiteList. Once it's confirmed that they are not being used,
	// we can remove them from the predefinedWhiteList.
	s.Run(sadefs.BatcherUser, func() {
		request := makeRequest(sadefs.BatcherUser)
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
	})

	s.Run(sadefs.BatcherNamespace, func() {
		request := makeRequest(sadefs.BatcherNamespace)
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
	})

}

func requireNotStartedButRunning(t *testing.T, resp *workflowservice.StartWorkflowExecutionResponse) {
	t.Helper()
	require.False(t, resp.Started)
	require.Equalf(t, resp.Status, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		"Expected workflow to be running, but got %s", resp.Status)
}

func requireStartedAndRunning(t *testing.T, resp *workflowservice.StartWorkflowExecutionResponse) {
	t.Helper()
	require.True(t, resp.Started)
	require.Equalf(t, resp.Status, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		"Expected workflow to be running, but got %s", resp.Status)
}
