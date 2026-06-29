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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowTestSuite struct {
	parallelsuite.Suite[*WorkflowTestSuite]
}

func TestWorkflowTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowTestSuite{})
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution() {
	s.Run("start", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		workflowID := testcore.RandomizeStr(s.T().Name())
		we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         workflowID,
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		})
		s.NoError(err)
		requireStartedAndRunning(s.T(), we)
		s.ProtoEqual(
			&commonpb.Link_WorkflowEvent{
				Namespace:  env.Namespace().String(),
				WorkflowId: workflowID,
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
		historyEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		})
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled`, historyEvents)
	})

	s.Run("start twice - same request", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}

		we0, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.NoError(err0)
		requireStartedAndRunning(s.T(), we0)
		s.ProtoEqual(
			&commonpb.Link_WorkflowEvent{
				Namespace:  env.Namespace().String(),
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

		we1, err1 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.NoError(err1)
		requireStartedAndRunning(s.T(), we1)
		s.ProtoEqual(
			&commonpb.Link_WorkflowEvent{
				Namespace:  env.Namespace().String(),
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

	s.Run("fail when already started", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}
		we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.NoError(err)
		requireStartedAndRunning(s.T(), we)

		request.RequestId = uuid.NewString()

		we2, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.Error(err)
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		s.ErrorAs(err, &alreadyStarted)
		s.Nil(we2)
	})
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}

	we0, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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
	we1, err1 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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
	allowedAddresses := []any{
		map[string]any{"Pattern": "some-secure-address", "AllowInsecure": false},
		map[string]any{"Pattern": "some-random-address", "AllowInsecure": false},
	}
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
		s.Run(tc.name, func(s *WorkflowTestSuite) {
			opts := []testcore.TestOption{
				testcore.WithDynamicConfig(callback.AllowedAddresses, allowedAddresses),
			}
			if tc.MaxCallbacksPerWorkflow > 0 {
				opts = append(opts, testcore.WithDynamicConfig(
					dynamicconfig.MaxCallbacksPerWorkflow,
					tc.MaxCallbacksPerWorkflow,
				))
			}
			env := testcore.NewEnv(s.T(), opts...)

			tv := testvars.New(s.T())
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           uuid.NewString(),
				Namespace:           env.Namespace().String(),
				WorkflowId:          tv.WorkflowID(),
				WorkflowType:        tv.WorkflowType(),
				TaskQueue:           tv.TaskQueue(),
				Input:               nil,
				WorkflowRunTimeout:  durationpb.New(100 * time.Second),
				Identity:            tv.WorkerIdentity(),
				CompletionCallbacks: []*commonpb.Callback{cb1},
			}

			we0, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
			s.NoError(err0)
			requireStartedAndRunning(s.T(), we0)
			s.ProtoEqual(
				&commonpb.Link_WorkflowEvent{
					Namespace:  env.Namespace().String(),
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

			historyEvents := env.GetHistory(
				env.Namespace().String(),
				&commonpb.WorkflowExecution{WorkflowId: request.WorkflowId, RunId: we0.RunId},
			)
			s.EqualHistoryEvents(
				`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
				`,
				historyEvents,
			)

			descResp, err := env.FrontendClient().DescribeWorkflowExecution(
				s.Context(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
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
			we1, err1 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
			if tc.ErrMessage != "" {
				s.ErrorContains(err1, tc.ErrMessage)
				historyEvents = env.GetHistory(
					env.Namespace().String(),
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
					Namespace:  env.Namespace().String(),
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

			historyEvents = env.GetHistory(
				env.Namespace().String(),
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

			descResp, err = env.FrontendClient().DescribeWorkflowExecution(
				s.Context(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
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
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.RequestID(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}

	we0, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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
	we1, err1 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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

	historyEvents := env.GetHistory(
		env.Namespace().String(),
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

	we2, err2 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err2)
	s.Equal(we0.RunId, we2.RunId)
	requireNotStartedButRunning(s.T(), we2)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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
	historyEvents = env.GetHistory(
		env.Namespace().String(),
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
	we3, err3 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err3)
	s.Equal(we0.RunId, we3.RunId)
	requireStartedAndRunning(s.T(), we3) // Original request was the start request.
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}

	we0, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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
	we1, err1 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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
	historyEvents := env.GetHistory(
		env.Namespace().String(),
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
	we2, err2 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err2)
	s.Equal(we0.RunId, we2.RunId)
	requireNotStartedButRunning(s.T(), we2)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
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

	historyEvents = env.GetHistory(
		env.Namespace().String(),
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
		s.Run(tc.name, func(s *WorkflowTestSuite) {
			// setting this to 0 to be sure we are terminating old workflow
			env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0))
			tv := testvars.New(s.T())
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:          uuid.NewString(),
				Namespace:          env.Namespace().String(),
				WorkflowId:         tv.WorkflowID(),
				WorkflowType:       tv.WorkflowType(),
				TaskQueue:          tv.TaskQueue(),
				Input:              nil,
				WorkflowRunTimeout: durationpb.New(100 * time.Second),
				Identity:           tv.WorkerIdentity(),
			}

			we0, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
			s.NoError(err0)
			s.ProtoEqual(
				&commonpb.Link_WorkflowEvent{
					Namespace:  env.Namespace().String(),
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
			we1, err1 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
			s.NoError(err1)
			s.NotEqual(we0.RunId, we1.RunId)
			s.ProtoEqual(
				&commonpb.Link_WorkflowEvent{
					Namespace:  env.Namespace().String(),
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

			descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: tv.WorkflowID(),
					RunId:      we0.RunId,
				},
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

			descResp, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
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

func (s *WorkflowTestSuite) TestStartWorkflowExecution_HistorySizeNotDoubleCountedOnReuse() {
	// Reusing a workflow ID after the previous run has closed goes through createBrandNew
	// (which fails because the closed run is still the "current" record) and falls back to
	// createAsCurrent. A bug previously leaked the failed attempt's first-batch HistorySize
	// increment into the retry, so the reused-ID run double-counted its first event batch in
	// ExecutionStats.HistorySize. A reused-ID run has byte-identical history to a fresh run, so
	// its reported HistorySizeBytes must match a fresh control run.
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0))

	tv := testvars.New(s.T())

	startRun := func(wfID string) string {
		we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:             uuid.NewString(),
			Namespace:             env.Namespace().String(),
			WorkflowId:            wfID,
			WorkflowType:          tv.WorkflowType(),
			TaskQueue:             tv.TaskQueue(),
			WorkflowRunTimeout:    durationpb.New(100 * time.Second),
			Identity:              tv.WorkerIdentity(),
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		})
		s.NoError(err)
		return we.RunId
	}
	historySize := func(wfID, runID string) int64 {
		descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID},
		})
		s.NoError(err)
		return descResp.WorkflowExecutionInfo.GetHistorySizeBytes()
	}
	terminate := func(wfID, runID string) {
		_, err := env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID},
			Reason:            "test cleanup",
		})
		s.NoError(err)
	}

	// Control: a brand-new workflow ID. createBrandNew succeeds on the first attempt.
	freshWfID := tv.WorkflowID() + "-fresh"
	freshRunID := startRun(freshWfID)
	freshAfterStart := historySize(freshWfID, freshRunID) // size of batch 1 ([Started, WFTScheduled])
	terminate(freshWfID, freshRunID)
	freshFinal := historySize(freshWfID, freshRunID) // batch1 + batch2 (terminated)

	// Repro: reuse the same workflow ID after the first run is closed.
	reuseWfID := tv.WorkflowID() + "-reuse"
	run1ID := startRun(reuseWfID)
	run1AfterStart := historySize(reuseWfID, run1ID)
	terminate(reuseWfID, run1ID)

	run2ID := startRun(reuseWfID)
	run2AfterStart := historySize(reuseWfID, run2ID)
	terminate(reuseWfID, run2ID)
	run2Final := historySize(reuseWfID, run2ID)

	s.T().Logf("history size: fresh afterStart=%d final=%d; reuse run1AfterStart=%d run2AfterStart=%d run2Final=%d",
		freshAfterStart, freshFinal, run1AfterStart, run2AfterStart, run2Final)

	s.Positive(freshAfterStart)
	// Structurally identical runs can differ by a few bytes (varint-encoded task IDs and
	// timestamps), so compare with a small tolerance. The bug inflated the reused-ID run by a
	// whole extra first batch (run1AfterStart bytes, hundreds), far outside this tolerance.
	const tolerance = 32.0
	// run1 and run2 share the workflow ID; run1 is a clean create, run2 reuses the closed ID.
	// They must report essentially the same size — not ~2x.
	s.InDelta(run1AfterStart, run2AfterStart, tolerance,
		"reused-ID run must not double-count its first event batch in ExecutionStats.HistorySize")
	// Cross-check the reused-ID run against an independent fresh run.
	s.InDelta(freshAfterStart, run2AfterStart, tolerance, "reused-ID run should match a fresh run after start")
	s.InDelta(freshFinal, run2Final, tolerance, "reused-ID run should match a fresh run after terminate")
}

func (s *WorkflowTestSuite) TestStartWorkflowExecutionWithDelay() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	startDelay := 3 * time.Second
	reqStartTime := time.Now()
	we0, startErr := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
		WorkflowStartDelay: durationpb.New(startDelay),
	})
	s.NoError(startErr)
	s.ProtoEqual(
		&commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
			WorkflowId: tv.WorkflowID(),
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		StickyTaskQueue:     tv.StickyTaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, pollErr := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(pollErr)
	s.GreaterOrEqual(delayEndTime.Sub(reqStartTime), startDelay)

	descResp, descErr := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we0.RunId,
		},
	})
	s.NoError(descErr)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
}

func (s *WorkflowTestSuite) TestTerminateWorkflow() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
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
	for range 10 {
		historyEvents = env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		})

		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
			env.Logger.Warn("Execution not terminated yet")
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
	for range 10 {
		newExecution, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          tv.WorkflowID(),
			WorkflowType:        tv.WorkflowType(),
			TaskQueue:           tv.TaskQueue(),
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			Identity:            tv.WorkerIdentity(),
		})
		if err != nil {
			env.Logger.Warn("Start New Execution failed. Error", tag.Error(err))
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue StartNewExecutionLoop
		}

		env.Logger.Info("New Execution Started with the same ID", tag.WorkflowID(tv.WorkflowID()),
			tag.WorkflowRunID(newExecution.RunId))
		newExecutionStarted = true
		break StartNewExecutionLoop
	}

	s.True(newExecutionStarted)
}

func (s *WorkflowTestSuite) TestTerminateWorkflowOnMessageTooLargeFailure() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	testContext := s.Context()

	// start workflow execution
	we, err0 := env.FrontendClient().StartWorkflowExecution(testContext, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// start workflow task, but do not respond to it
	res, err := env.FrontendClient().PollWorkflowTaskQueue(testContext, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	env.Logger.Info("PollWorkflowTaskQueue", tag.Error(err))
	s.NoError(err)

	// send a signal to test buffered event case.
	_, err = env.FrontendClient().SignalWorkflowExecution(testContext, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		},
		SignalName: "buffered-signal",
	})
	s.NoError(err)

	// respond workflow task as failed with grpc message too large error
	_, err = env.FrontendClient().RespondWorkflowTaskFailed(testContext, &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: res.TaskToken,
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE,
	})
	s.NoError(err)

	historyEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
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
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(10)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

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
		s.Equal(tv.WorkflowID(), task.WorkflowExecution.WorkflowId)
		s.Equal(tv.ActivityType().Name, task.ActivityType.Name)
		s.Equal(tv.WithActivityIDNumber(int(expectedActivity)).ActivityID(), task.ActivityId)
		var inputBytes []byte
		s.NoError(payloads.Decode(task.Input, &inputBytes))
		s.Equal(expectedActivity, int32(binary.LittleEndian.Uint32(inputBytes)))
		s.NotNil(task.RetryPolicy)
		s.Equal(int64(1), task.RetryPolicy.InitialInterval.Seconds) // server default
		expectedActivity++

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	for i := range 10 {
		_, err := poller.PollAndProcessWorkflowTask()
		env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
		if i%2 == 0 {
			err = poller.PollAndProcessActivityTask(false)
		} else { // just for testing respondActivityTaskCompleteByID
			err = poller.PollAndProcessActivityTaskWithID(false)
		}
		env.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
		s.NoError(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *WorkflowTestSuite) TestCompleteWorkflowTaskAndCreateNewOne() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	res, err := poller.PollAndProcessWorkflowTask(testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)

	s.Equal(int64(3), newTask.WorkflowTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.WorkflowTask.GetStartedEventId())
	s.Len(newTask.WorkflowTask.History.Events, 4)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_MARKER_RECORDED, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())
}

func (s *WorkflowTestSuite) TestWorkflowTaskAndActivityTaskTimeoutsWorkflow() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(4)
	activityCounter := int32(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

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

		env.Logger.Info("Completing enums")

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(tv.WorkflowID(), task.WorkflowExecution.WorkflowId)
		s.Equal(tv.ActivityType().Name, task.ActivityType.Name)
		env.Logger.Info("Activity ID", tag.ActivityID(task.ActivityId))
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// This test is flaky: it has been seen to cause the 35m test timeout to be exceeded
	// occasionally. It currently has atypically detailed info-level logging with the aim of
	// diagnosing the reason for the timeout.
	const testTag = "[TestWorkflowTaskAndActivityTaskTimeoutsWorkflow] "
	testStart := time.Now()
	var lastDropTime time.Time
	for i := range 8 {
		// Check if test context has been cancelled/timed out
		select {
		case <-s.Context().Done():
			s.FailNow("Test timeout exceeded", "context deadline exceeded after %v", time.Since(testStart))
		default:
		}

		iterStart := time.Now()
		dropWorkflowTask := (i%2 == 0)
		env.Logger.Info(testTag+"iteration starting",
			tag.Counter(i),
			tag.Bool("drop_task", dropWorkflowTask),
			tag.Duration("time_since_test_start", time.Since(testStart)),
			tag.Duration("time_since_last_drop", time.Since(lastDropTime)))
		var err error
		if dropWorkflowTask {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithDropTask)
			lastDropTime = time.Now()
			env.Logger.Info(testTag+"dropped workflow task",
				tag.Counter(i),
				tag.Duration("poll_duration", time.Since(iterStart)))
		} else {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithExpectedAttemptCount(2))
			env.Logger.Info(testTag+"processed workflow task (expected attempt=2)",
				tag.Counter(i),
				tag.Duration("poll_duration", time.Since(iterStart)),
				tag.Duration("time_since_last_drop", time.Since(lastDropTime)),
				tag.Error(err))
		}
		if err != nil {
			s.PrintHistoryEventsCompact(env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
				WorkflowId: tv.WorkflowID(),
				RunId:      we.RunId,
			}))
		}
		s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
		if !dropWorkflowTask {
			activityStart := time.Now()
			env.Logger.Info(testTag+"polling activity task", tag.Counter(i))
			err = poller.PollAndProcessActivityTask(i%4 == 0)
			env.Logger.Info(testTag+"activity task poll completed",
				tag.Counter(i),
				tag.Duration("activity_poll_duration", time.Since(activityStart)),
				tag.Error(err))
			s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
		}
	}

	env.Logger.Info(testTag+"waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	// Check if test context has been cancelled/timed out before final poll
	select {
	case <-s.Context().Done():
		s.FailNow("Test timeout exceeded", "context deadline exceeded after %v", time.Since(testStart))
	default:
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *WorkflowTestSuite) TestWorkflowRetry() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	initialInterval := 1 * time.Second
	backoffCoefficient := 1.5
	maximumAttempts := 5
	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
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
	})
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	describeWorkflowExecution := func(execution *commonpb.WorkflowExecution) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
		})
	}

	for i := 1; i <= maximumAttempts; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
		events := env.GetHistory(env.Namespace().String(), executions[i-1])
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
			backoff = min(
				// retry backoff cannot larger than MaximumIntervalInSeconds
				time.Duration(initialInterval.Seconds()*math.Pow(backoffCoefficient, float64(i-2)))*time.Second, time.Second)
		}
		expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime().AsTime().Add(backoff)
		s.Equal(expectedExecutionTime, timestamp.TimeValue(dweResponse.WorkflowExecutionInfo.GetExecutionTime()))
		s.Equal(we.RunId, dweResponse.WorkflowExecutionInfo.GetFirstRunId())
	}

	// Check run id links
	for i := range maximumAttempts {
		events := env.GetHistory(env.Namespace().String(), executions[i])
		switch i {
		case 0:
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":""}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed {"NewExecutionRunId":"%s"}`, executions[i+1].RunId), events)
		case maximumAttempts - 1:
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":"%s"}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted {"NewExecutionRunId":""}`, executions[i-1].RunId), events)
		default:
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
		resp, err := env.FrontendClient().GetWorkflowExecutionHistory(oldSDKCtx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:              env.Namespace().String(),
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
	env := testcore.NewEnv(s.T())
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
		Namespace:           env.Namespace().String(),
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

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution
	wtHandler := workflowImpl(5, "retryable-error", false, &executions)
	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := env.GetHistory(env.Namespace().String(), executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = env.GetHistory(env.Namespace().String(), executions[1])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":2}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = env.GetHistory(env.Namespace().String(), executions[2])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":3}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	// Fail error reason
	request = &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
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

	we, err0 = env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	executions = []*commonpb.WorkflowExecution{}
	wtHandler = workflowImpl(5, "bad-bug", true, &executions)
	poller = &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = env.GetHistory(env.Namespace().String(), executions[0])
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
	makeRequest := func(env *testcore.TestEnv, tv *testvars.TestVars, t *testing.T, saFieldName string) *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(t.Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					saFieldName: sadefs.MustEncodeValue("1.0.0", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				},
			},
		}
	}

	s.Run(sadefs.TemporalWorkerDeploymentVersion, func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		request := makeRequest(env, testvars.New(s.T()), s.T(), sadefs.TemporalWorkerDeploymentVersion)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.Run(sadefs.TemporalWorkerDeployment, func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		request := makeRequest(env, testvars.New(s.T()), s.T(), sadefs.TemporalWorkerDeployment)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.Run(sadefs.TemporalWorkflowVersioningBehavior, func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		request := makeRequest(env, testvars.New(s.T()), s.T(), sadefs.TemporalWorkflowVersioningBehavior)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	// These are currently allowed since they are in the predefinedWhiteList. Once it's confirmed that they are not being used,
	// we can remove them from the predefinedWhiteList.
	s.Run(sadefs.BatcherUser, func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		request := makeRequest(env, testvars.New(s.T()), s.T(), sadefs.BatcherUser)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.NoError(err)
	})

	s.Run(sadefs.BatcherNamespace, func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		request := makeRequest(env, testvars.New(s.T()), s.T(), sadefs.BatcherNamespace)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.NoError(err)
	})

}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_InternalTaskQueue() {
	errorMessageKeyword := "internal per-namespace task queue"

	// Test StartWorkflowExecution with internal task queue
	s.Run("StartWorkflowExecution_PerNSWorkerTaskQueue", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		tvInternal := tv.WithTaskQueue(primitives.PerNSWorkerTaskQueue)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tvInternal.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		})
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
		s.Contains(err.Error(), errorMessageKeyword)
	})

	// Test SignalWithStartWorkflowExecution with internal task queue
	s.Run("SignalWithStartWorkflowExecution_PerNSWorkerTaskQueue", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		tvInternal := tv.WithTaskQueue(primitives.PerNSWorkerTaskQueue)
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tvInternal.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
			SignalName:         "test-signal",
		}
		_, err := env.FrontendClient().SignalWithStartWorkflowExecution(s.Context(), request)
		s.Error(err)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
		s.Contains(err.Error(), errorMessageKeyword)
	})

	// Test ExecuteMultiOperation with internal task queue
	s.Run("multiOp", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		tvInternal := tv.WithTaskQueue(primitives.PerNSWorkerTaskQueue)
		workflowID := testcore.RandomizeStr(s.T().Name())
		request := &workflowservice.ExecuteMultiOperationRequest{
			Namespace: env.Namespace().String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				{
					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
						StartWorkflow: &workflowservice.StartWorkflowExecutionRequest{
							RequestId:          uuid.NewString(),
							Namespace:          env.Namespace().String(),
							WorkflowId:         workflowID,
							WorkflowType:       tv.WorkflowType(),
							TaskQueue:          tvInternal.TaskQueue(),
							Input:              nil,
							WorkflowRunTimeout: durationpb.New(100 * time.Second),
							Identity:           tv.WorkerIdentity(),
						},
					},
				},
				{
					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
						UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionRequest{
							Namespace: env.Namespace().String(),
							WorkflowExecution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
							},
							Request: &updatepb.Request{
								Meta: &updatepb.Meta{
									UpdateId: uuid.NewString(),
								},
								Input: &updatepb.Input{
									Name: "update-name",
								},
							},
						},
					},
				},
			},
		}
		_, err := env.FrontendClient().ExecuteMultiOperation(s.Context(), request)
		s.Error(err)
		var multiOpErr *serviceerror.MultiOperationExecution
		s.ErrorAs(err, &multiOpErr)
		s.Equal("Update-with-Start could not be executed.", err.Error())
		opErrs := multiOpErr.OperationErrors()
		s.Contains(opErrs[0].Error(), errorMessageKeyword)
	})

}

// TestStartWorkflowExecution_FirstExecutionRunId covers the contract that StartWorkflowExecution
// (both the response and the WorkflowExecutionAlreadyStarted error) carries first_execution_run_id
// pointing at the head of the continue-as-new chain. For workflows that have never been continued
// as new, this should equal the current run id; the chained case is covered in continue_as_new_test.go.
func (s *WorkflowTestSuite) TestStartWorkflowExecution_FirstExecutionRunId() {
	s.Run("brand new start", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		req := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}
		we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), req)
		s.NoError(err)
		requireStartedAndRunning(s.T(), we)
		s.Equal(we.RunId, we.FirstExecutionRunId)
	})

	s.Run("use existing dedup", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		req := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}
		we0, err := env.FrontendClient().StartWorkflowExecution(s.Context(), req)
		s.NoError(err)

		req.RequestId = uuid.NewString()
		req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
		we1, err := env.FrontendClient().StartWorkflowExecution(s.Context(), req)
		s.NoError(err)
		requireNotStartedButRunning(s.T(), we1)
		s.Equal(we0.RunId, we1.RunId)
		s.Equal(we0.RunId, we1.FirstExecutionRunId)
	})

	s.Run("fail policy error carries first execution run id", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		req := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}
		we0, err := env.FrontendClient().StartWorkflowExecution(s.Context(), req)
		s.NoError(err)

		req.RequestId = uuid.NewString()
		// Default conflict policy is FAIL.
		_, err = env.FrontendClient().StartWorkflowExecution(s.Context(), req)
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		s.ErrorAs(err, &already)
		s.Equal(we0.RunId, already.RunId)
		s.Equal(we0.RunId, already.FirstExecutionRunId)
	})

	s.Run("retried request dedup", func(s *WorkflowTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := testvars.New(s.T())
		req := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          env.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}
		we0, err := env.FrontendClient().StartWorkflowExecution(s.Context(), req)
		s.NoError(err)

		// Re-issue with same RequestId: server dedupes against the started request.
		we1, err := env.FrontendClient().StartWorkflowExecution(s.Context(), req)
		s.NoError(err)
		s.Equal(we0.RunId, we1.RunId)
		s.Equal(we0.RunId, we1.FirstExecutionRunId)
	})
}

func requireNotStartedButRunning(t *testing.T, resp *workflowservice.StartWorkflowExecutionResponse) {
	t.Helper()
	require.False(t, resp.Started)
	require.Equalf(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, resp.Status,
		"Expected workflow to be running, but got %s", resp.Status)
}

func requireStartedAndRunning(t *testing.T, resp *workflowservice.StartWorkflowExecutionResponse) {
	t.Helper()
	require.True(t, resp.Started)
	require.Equalf(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, resp.Status,
		"Expected workflow to be running, but got %s", resp.Status)
}
