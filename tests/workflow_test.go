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
		return workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:          uuid.NewString(),
			Namespace:          s.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
		}.Build()
	}

	s.Run("start", func() {
		request := makeRequest()
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
		requireStartedAndRunning(s.T(), we)
		s.ProtoEqual(
			commonpb.Link_WorkflowEvent_builder{
				Namespace:  s.Namespace().String(),
				WorkflowId: request.GetWorkflowId(),
				RunId:      we.GetRunId(),
				EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				}.Build(),
			}.Build(),
			we.GetLink().GetWorkflowEvent(),
		)

		// Validate the default value for WorkflowTaskTimeoutSeconds
		historyEvents := s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
			WorkflowId: request.GetWorkflowId(),
			RunId:      we.GetRunId(),
		}.Build())
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
			commonpb.Link_WorkflowEvent_builder{
				Namespace:  s.Namespace().String(),
				WorkflowId: request.GetWorkflowId(),
				RunId:      we0.GetRunId(),
				EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				}.Build(),
			}.Build(),
			we0.GetLink().GetWorkflowEvent(),
		)

		we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err1)
		requireStartedAndRunning(s.T(), we1)
		s.ProtoEqual(
			commonpb.Link_WorkflowEvent_builder{
				Namespace:  s.Namespace().String(),
				WorkflowId: request.GetWorkflowId(),
				RunId:      we1.GetRunId(),
				EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				}.Build(),
			}.Build(),
			we1.GetLink().GetWorkflowEvent(),
		)

		s.Equal(we0.GetRunId(), we1.GetRunId())
	})

	s.Run("fail when already started", func() {
		request := makeRequest()
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)
		requireStartedAndRunning(s.T(), we)

		request.SetRequestId(uuid.NewString())

		we2, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.Error(err)
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		s.ErrorAs(err, &alreadyStarted)
		s.Nil(we2)
	})
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting() {
	tv := testvars.New(s.T())
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}.Build()

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we0.GetRunId(),
			EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}.Build(),
		}.Build(),
		we0.GetLink().GetWorkflowEvent(),
	)

	request.SetRequestId(uuid.NewString())
	request.SetWorkflowIdConflictPolicy(enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.GetRunId(), we1.GetRunId())
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we1.GetRunId(),
			EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}.Build(),
		}.Build(),
		we1.GetLink().GetWorkflowEvent(),
	)
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions() {
	s.OverrideDynamicConfig(dynamicconfig.EnableRequestIdRefLinks, true)
	s.OverrideDynamicConfig(callbacks.AllowedAddresses, []any{
		map[string]any{"Pattern": "some-secure-address", "AllowInsecure": false},
		map[string]any{"Pattern": "some-random-address", "AllowInsecure": false},
	})
	cb1 := commonpb.Callback_builder{
		Nexus: commonpb.Callback_Nexus_builder{
			Url: "https://some-secure-address",
		}.Build(),
	}.Build()
	cb2 := commonpb.Callback_builder{
		Nexus: commonpb.Callback_Nexus_builder{
			Url: "https://some-random-address",
		}.Build(),
	}.Build()

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
			OnConflictOptions: workflowpb.OnConflictOptions_builder{
				AttachRequestId: true,
			}.Build(),
		},
		{
			name:                     "OnConflictOptions attach request id, links, callbacks",
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: workflowpb.OnConflictOptions_builder{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			}.Build(),
		},
		{
			name:                     "OnConflictOptions failed max callbacks per workflow",
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: workflowpb.OnConflictOptions_builder{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			}.Build(),
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
			request := workflowservice.StartWorkflowExecutionRequest_builder{
				RequestId:           uuid.NewString(),
				Namespace:           s.Namespace().String(),
				WorkflowId:          tv.WorkflowID(),
				WorkflowType:        tv.WorkflowType(),
				TaskQueue:           tv.TaskQueue(),
				Input:               nil,
				WorkflowRunTimeout:  durationpb.New(100 * time.Second),
				Identity:            tv.WorkerIdentity(),
				CompletionCallbacks: []*commonpb.Callback{cb1},
			}.Build()

			we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err0)
			requireStartedAndRunning(s.T(), we0)
			s.ProtoEqual(
				commonpb.Link_WorkflowEvent_builder{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.GetWorkflowId(),
					RunId:      we0.GetRunId(),
					EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					}.Build(),
				}.Build(),
				we0.GetLink().GetWorkflowEvent(),
			)

			historyEvents := s.GetHistory(
				s.Namespace().String(),
				commonpb.WorkflowExecution_builder{WorkflowId: request.GetWorkflowId(), RunId: we0.GetRunId()}.Build(),
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
				workflowservice.DescribeWorkflowExecutionRequest_builder{
					Namespace: s.Namespace().String(),
					Execution: commonpb.WorkflowExecution_builder{
						WorkflowId: tv.WorkflowID(),
						RunId:      we0.GetRunId(),
					}.Build(),
				}.Build(),
			)
			s.NoError(err)
			s.Len(descResp.GetCallbacks(), 1)
			s.ProtoEqual(cb1, descResp.GetCallbacks()[0].GetCallback())

			request.SetRequestId(uuid.NewString())
			request.SetLinks([]*commonpb.Link{commonpb.Link_builder{
				WorkflowEvent: commonpb.Link_WorkflowEvent_builder{
					Namespace:  "dont-care",
					WorkflowId: "whatever",
					RunId:      uuid.NewString(),
				}.Build(),
			}.Build()})
			request.SetCompletionCallbacks([]*commonpb.Callback{cb2})
			request.SetWorkflowIdConflictPolicy(tc.WorkflowIdConflictPolicy)
			request.SetOnConflictOptions(tc.OnConflictOptions)
			we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			if tc.ErrMessage != "" {
				s.ErrorContains(err1, tc.ErrMessage)
				historyEvents = s.GetHistory(
					s.Namespace().String(),
					commonpb.WorkflowExecution_builder{WorkflowId: request.GetWorkflowId(), RunId: we0.GetRunId()}.Build(),
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
			s.Equal(we0.GetRunId(), we1.GetRunId())
			requireNotStartedButRunning(s.T(), we1)
			s.ProtoEqual(
				commonpb.Link_WorkflowEvent_builder{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.GetWorkflowId(),
					RunId:      we1.GetRunId(),
					RequestIdRef: commonpb.Link_WorkflowEvent_RequestIdReference_builder{
						RequestId: request.GetRequestId(),
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
					}.Build(),
				}.Build(),
				we1.GetLink().GetWorkflowEvent(),
			)

			historyEvents = s.GetHistory(
				s.Namespace().String(),
				commonpb.WorkflowExecution_builder{WorkflowId: request.GetWorkflowId(), RunId: we0.GetRunId()}.Build(),
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

			if tc.OnConflictOptions.GetAttachRequestId() {
				s.Equal(request.GetRequestId(), attributes.GetAttachedRequestId())
			} else {
				s.Empty(attributes.GetAttachedRequestId())
			}

			numCallbacks := 1
			expectedCallbacks := []*commonpb.Callback{cb1}
			if tc.OnConflictOptions.GetAttachCompletionCallbacks() {
				s.ProtoElementsMatch(request.GetCompletionCallbacks(), attributes.GetAttachedCompletionCallbacks())
				numCallbacks += len(request.GetCompletionCallbacks())
				expectedCallbacks = append(expectedCallbacks, request.GetCompletionCallbacks()...)
			} else {
				s.Empty(attributes.GetAttachedCompletionCallbacks())
			}

			if tc.OnConflictOptions.GetAttachLinks() {
				s.ProtoElementsMatch(request.GetLinks(), wfOptionsUpdated.GetLinks())
			} else {
				s.Empty(wfOptionsUpdated.GetLinks())
			}

			descResp, err = s.FrontendClient().DescribeWorkflowExecution(
				testcore.NewContext(),
				workflowservice.DescribeWorkflowExecutionRequest_builder{
					Namespace: s.Namespace().String(),
					Execution: commonpb.WorkflowExecution_builder{
						WorkflowId: tv.WorkflowID(),
						RunId:      we0.GetRunId(),
					}.Build(),
				}.Build(),
			)
			s.NoError(err)
			s.Len(descResp.GetCallbacks(), numCallbacks)
			descRespCallbacks := make([]*commonpb.Callback, len(descResp.GetCallbacks()))
			for i, cb := range descResp.GetCallbacks() {
				descRespCallbacks[i] = cb.GetCallback()
			}
			s.ProtoElementsMatch(expectedCallbacks, descRespCallbacks)
		})
	}
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions_Dedup() {
	s.OverrideDynamicConfig(dynamicconfig.EnableRequestIdRefLinks, true)
	tv := testvars.New(s.T())
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:          tv.RequestID(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}.Build()

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we0.GetRunId(),
			EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}.Build(),
		}.Build(),
		we0.GetLink().GetWorkflowEvent(),
	)

	request.SetRequestId(uuid.NewString())
	request.SetWorkflowIdConflictPolicy(enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
	request.SetOnConflictOptions(workflowpb.OnConflictOptions_builder{
		AttachRequestId: true,
	}.Build())
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.GetRunId(), we1.GetRunId())
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we1.GetRunId(),
			RequestIdRef: commonpb.Link_WorkflowEvent_RequestIdReference_builder{
				RequestId: request.GetRequestId(),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			}.Build(),
		}.Build(),
		we1.GetLink().GetWorkflowEvent(),
	)

	historyEvents := s.GetHistory(
		s.Namespace().String(),
		commonpb.WorkflowExecution_builder{WorkflowId: request.GetWorkflowId(), RunId: we0.GetRunId()}.Build(),
	)
	s.EqualHistoryEvents(
		fmt.Sprintf(
			`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated {"VersioningOverride": null, "UnsetVersioningOverride": false, "AttachedRequestId": "%s", "AttachedCompletionCallbacks": null}
			`,
			request.GetRequestId(),
		),
		historyEvents,
	)

	we2, err2 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err2)
	s.Equal(we0.GetRunId(), we2.GetRunId())
	requireNotStartedButRunning(s.T(), we2)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we2.GetRunId(),
			RequestIdRef: commonpb.Link_WorkflowEvent_RequestIdReference_builder{
				RequestId: request.GetRequestId(),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			}.Build(),
		}.Build(),
		we2.GetLink().GetWorkflowEvent(),
	)

	// History events must be the same as before.
	historyEvents = s.GetHistory(
		s.Namespace().String(),
		commonpb.WorkflowExecution_builder{WorkflowId: request.GetWorkflowId(), RunId: we0.GetRunId()}.Build(),
	)
	s.EqualHistoryEvents(
		fmt.Sprintf(
			`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated {"VersioningOverride": null, "UnsetVersioningOverride": false, "AttachedRequestId": "%s", "AttachedCompletionCallbacks": null}
			`,
			request.GetRequestId(),
		),
		historyEvents,
	)

	// Original request must also be deduped.
	request.SetRequestId(tv.RequestID())
	request.ClearOnConflictOptions()
	we3, err3 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err3)
	s.Equal(we0.GetRunId(), we3.GetRunId())
	requireStartedAndRunning(s.T(), we3) // Original request was the start request.
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we3.GetRunId(),
			EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}.Build(),
		}.Build(),
		we3.GetLink().GetWorkflowEvent(),
	)
}

func (s *WorkflowTestSuite) TestStartWorkflowExecution_UseExisting_OnConflictOptions_NoDedup() {
	s.OverrideDynamicConfig(dynamicconfig.EnableRequestIdRefLinks, true)
	tv := testvars.New(s.T())
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
	}.Build()

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	requireStartedAndRunning(s.T(), we0)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we0.GetRunId(),
			EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}.Build(),
		}.Build(),
		we0.GetLink().GetWorkflowEvent(),
	)

	// New RequestId, but not attaching it.
	request.SetRequestId(uuid.NewString())
	request.SetWorkflowIdConflictPolicy(enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
	we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.GetRunId(), we1.GetRunId())
	requireNotStartedButRunning(s.T(), we1)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we1.GetRunId(),
			EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}.Build(),
		}.Build(),
		we1.GetLink().GetWorkflowEvent(),
	)

	// Since OnConflictOptions is nil, no history event is added.
	historyEvents := s.GetHistory(
		s.Namespace().String(),
		commonpb.WorkflowExecution_builder{WorkflowId: request.GetWorkflowId(), RunId: we0.GetRunId()}.Build(),
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
	request.SetOnConflictOptions(workflowpb.OnConflictOptions_builder{
		AttachRequestId: true,
	}.Build())
	we2, err2 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err2)
	s.Equal(we0.GetRunId(), we2.GetRunId())
	requireNotStartedButRunning(s.T(), we2)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we2.GetRunId(),
			RequestIdRef: commonpb.Link_WorkflowEvent_RequestIdReference_builder{
				RequestId: request.GetRequestId(),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			}.Build(),
		}.Build(),
		we2.GetLink().GetWorkflowEvent(),
	)

	historyEvents = s.GetHistory(
		s.Namespace().String(),
		commonpb.WorkflowExecution_builder{WorkflowId: request.GetWorkflowId(), RunId: we0.GetRunId()}.Build(),
	)
	s.EqualHistoryEvents(
		fmt.Sprintf(
			`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated {"VersioningOverride": null, "UnsetVersioningOverride": false, "AttachedRequestId": "%s", "AttachedCompletionCallbacks": null}
			`,
			request.GetRequestId(),
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
			request := workflowservice.StartWorkflowExecutionRequest_builder{
				RequestId:          uuid.NewString(),
				Namespace:          s.Namespace().String(),
				WorkflowId:         tv.WorkflowID(),
				WorkflowType:       tv.WorkflowType(),
				TaskQueue:          tv.TaskQueue(),
				Input:              nil,
				WorkflowRunTimeout: durationpb.New(100 * time.Second),
				Identity:           tv.WorkerIdentity(),
			}.Build()

			we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err0)
			s.ProtoEqual(
				commonpb.Link_WorkflowEvent_builder{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.GetWorkflowId(),
					RunId:      we0.GetRunId(),
					EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					}.Build(),
				}.Build(),
				we0.GetLink().GetWorkflowEvent(),
			)

			request.SetRequestId(uuid.NewString())
			request.SetWorkflowIdReusePolicy(tc.WorkflowIdReusePolicy)
			request.SetWorkflowIdConflictPolicy(tc.WorkflowIdConflictPolicy)
			we1, err1 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
			s.NoError(err1)
			s.NotEqual(we0.GetRunId(), we1.GetRunId())
			s.ProtoEqual(
				commonpb.Link_WorkflowEvent_builder{
					Namespace:  s.Namespace().String(),
					WorkflowId: request.GetWorkflowId(),
					RunId:      we1.GetRunId(),
					EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					}.Build(),
				}.Build(),
				we1.GetLink().GetWorkflowEvent(),
			)

			descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
				Namespace: s.Namespace().String(),
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: tv.WorkflowID(),
					RunId:      we0.GetRunId(),
				}.Build(),
			}.Build())
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.GetWorkflowExecutionInfo().GetStatus())

			descResp, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
				Namespace: s.Namespace().String(),
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: tv.WorkflowID(),
					RunId:      we1.GetRunId(),
				}.Build(),
			}.Build())
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.GetWorkflowExecutionInfo().GetStatus())
		})
	}
}

func (s *WorkflowTestSuite) TestStartWorkflowExecutionWithDelay() {
	tv := testvars.New(s.T())
	startDelay := 3 * time.Second
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           tv.WorkerIdentity(),
		WorkflowStartDelay: durationpb.New(startDelay),
	}.Build()

	reqStartTime := time.Now()
	we0, startErr := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(startErr)
	s.ProtoEqual(
		commonpb.Link_WorkflowEvent_builder{
			Namespace:  s.Namespace().String(),
			WorkflowId: request.GetWorkflowId(),
			RunId:      we0.GetRunId(),
			EventRef: commonpb.Link_WorkflowEvent_EventReference_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}.Build(),
		}.Build(),
		we0.GetLink().GetWorkflowEvent(),
	)

	delayEndTime := time.Now()
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		delayEndTime = time.Now()
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

	descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: tv.WorkflowID(),
			RunId:      we0.GetRunId(),
		}.Build(),
	}.Build())
	s.NoError(descErr)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().GetStatus())
}

func (s *WorkflowTestSuite) TestTerminateWorkflow() {
	tv := testvars.New(s.T())
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             tv.WithActivityIDNumber(int(activityCounter)).ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
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

	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.GetRunId(),
		}.Build(),
		Reason:   "terminate reason",
		Details:  payloads.EncodeString("terminate details"),
		Identity: tv.WorkerIdentity(),
	}.Build())
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.GetRunId(),
		}.Build())

		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
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
		request := workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:           uuid.NewString(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          tv.WorkflowID(),
			WorkflowType:        tv.WorkflowType(),
			TaskQueue:           tv.TaskQueue(),
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			Identity:            tv.WorkerIdentity(),
		}.Build()

		newExecution, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		if err != nil {
			s.Logger.Warn("Start New Execution failed. Error", tag.Error(err))
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue StartNewExecutionLoop
		}

		s.Logger.Info("New Execution Started with the same ID", tag.WorkflowID(tv.WorkflowID()),
			tag.WorkflowRunID(newExecution.GetRunId()))
		newExecutionStarted = true
		break StartNewExecutionLoop
	}

	s.True(newExecutionStarted)
}

func (s *WorkflowTestSuite) TestTerminateWorkflowOnMessageTooLargeFailure() {
	tv := testvars.New(s.T())
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}.Build()

	testContext := testcore.NewContext()

	// start workflow execution
	we, err0 := s.FrontendClient().StartWorkflowExecution(testContext, request)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))
	s.NoError(err0)

	// start workflow task, but do not respond to it
	res, err := s.FrontendClient().PollWorkflowTaskQueue(testContext, workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	}.Build())
	s.Logger.Info("PollWorkflowTaskQueue", tag.Error(err))
	s.NoError(err)

	// send a signal to test buffered event case.
	_, err = s.FrontendClient().SignalWorkflowExecution(testContext, workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: "buffered-signal",
	}.Build())
	s.NoError(err)

	// respond workflow task as failed with grpc message too large error
	_, err = s.FrontendClient().RespondWorkflowTaskFailed(testContext, workflowservice.RespondWorkflowTaskFailedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: res.GetTaskToken(),
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE,
	}.Build())
	s.NoError(err)

	historyEvents := s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
		WorkflowId: tv.WorkflowID(),
		RunId:      we.GetRunId(),
	}.Build())

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
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	workflowComplete := false
	activityCount := int32(10)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             tv.WithActivityIDNumber(int(activityCounter)).ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
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

	expectedActivity := int32(1)
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.EqualValues(tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		s.Equal(tv.ActivityType().GetName(), task.GetActivityType().GetName())
		s.Equal(tv.WithActivityIDNumber(int(expectedActivity)).ActivityID(), task.GetActivityId())
		s.Equal(expectedActivity, s.DecodePayloadsByteSliceInt32(task.GetInput()))
		s.NotNil(task.GetRetryPolicy())
		s.Equal(int64(1), task.GetRetryPolicy().GetInitialInterval().Seconds) // server default
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
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	commandCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if commandCount < 2 {
			commandCount++
			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				RecordMarkerCommandAttributes: commandpb.RecordMarkerCommandAttributes_builder{
					MarkerName: "test-marker",
				}.Build(),
			}.Build()}, nil
		}

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
	s.NotNil(newTask.GetWorkflowTask())

	s.Equal(int64(3), newTask.GetWorkflowTask().GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.GetWorkflowTask().GetStartedEventId())
	s.Equal(4, len(newTask.GetWorkflowTask().GetHistory().GetEvents()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.GetWorkflowTask().GetHistory().GetEvents()[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_MARKER_RECORDED, newTask.GetWorkflowTask().GetHistory().GetEvents()[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.GetWorkflowTask().GetHistory().GetEvents()[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.GetWorkflowTask().GetHistory().GetEvents()[3].GetEventType())
}

func (s *WorkflowTestSuite) TestWorkflowTaskAndActivityTaskTimeoutsWorkflow() {
	tv := testvars.New(s.T())
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	workflowComplete := false
	activityCount := int32(4)
	activityCounter := int32(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             tv.WithActivityIDNumber(int(activityCounter)).ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
					ScheduleToStartTimeout: durationpb.New(1 * time.Second),
					StartToCloseTimeout:    durationpb.New(1 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		s.Logger.Info("Completing enums")

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.EqualValues(tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		s.Equal(tv.ActivityType().GetName(), task.GetActivityType().GetName())
		s.Logger.Info("Activity ID", tag.WorkflowActivityID(task.GetActivityId()))
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
			s.PrintHistoryEventsCompact(s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
				WorkflowId: tv.WorkflowID(),
				RunId:      we.GetRunId(),
			}.Build()))
		}
		s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
		if !dropWorkflowTask {
			s.Logger.Info("Calling PollAndProcessActivityTask", tag.Counter(i))
			err = poller.PollAndProcessActivityTask(i%4 == 0)
			s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.GetRunId()))

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
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			// Intentionally test server-initialization of Initial Interval value (which should be 1 second)
			MaximumAttempts:        int32(maximumAttempts),
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     backoffCoefficient,
		}.Build(),
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	var executions []*commonpb.WorkflowExecution

	attemptCount := 1

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.GetWorkflowExecution())
		attemptCount++
		if attemptCount > maximumAttempts {
			return []*commandpb.Command{
				commandpb.Command_builder{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
						Result: payloads.EncodeString("succeed-after-retry"),
					}.Build(),
				}.Build()}, nil
		}
		return []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				FailWorkflowExecutionCommandAttributes: commandpb.FailWorkflowExecutionCommandAttributes_builder{
					Failure: failure.NewServerFailure("retryable-error", false),
				}.Build(),
			}.Build()}, nil
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
		return s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
			Namespace: s.Namespace().String(),
			Execution: execution,
		}.Build())
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
		expectedExecutionTime := dweResponse.GetWorkflowExecutionInfo().GetStartTime().AsTime().Add(backoff)
		s.Equal(expectedExecutionTime, timestamp.TimeValue(dweResponse.GetWorkflowExecutionInfo().GetExecutionTime()))
		s.Equal(we.GetRunId(), dweResponse.GetWorkflowExecutionInfo().GetFirstRunId())
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
  5 WorkflowExecutionFailed {"NewExecutionRunId":"%s"}`, executions[i+1].GetRunId()), events)
		} else if i == maximumAttempts-1 {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":"%s"}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted {"NewExecutionRunId":""}`, executions[i-1].GetRunId()), events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":"%s"}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed {"NewExecutionRunId":"%s"}`, executions[i-1].GetRunId(), executions[i+1].GetRunId()), events)
		}

		// Test get history from old SDKs
		// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
		// See comment in workflowHandler.go:GetWorkflowExecutionHistory
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		oldSDKCtx := headers.SetVersionsForTests(ctx, "1.3.1", headers.ClientNameJavaSDK, headers.SupportedServerVersions, "")
		resp, err := s.FrontendClient().GetWorkflowExecutionHistory(oldSDKCtx, workflowservice.GetWorkflowExecutionHistoryRequest_builder{
			Namespace:              s.Namespace().String(),
			Execution:              executions[i],
			MaximumPageSize:        5,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		}.Build())
		cancel()
		s.NoError(err)
		events = resp.GetHistory().GetEvents()
		if i == maximumAttempts-1 {
			s.EqualHistoryEvents(`
  5 WorkflowExecutionCompleted {"NewExecutionRunId":""}`, events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  5 WorkflowExecutionContinuedAsNew {"NewExecutionRunId":"%s"}`, executions[i+1].GetRunId()), events)
		}
	}
}

func (s *WorkflowTestSuite) TestWorkflowRetryFailures() {
	tv := testvars.New(s.T())
	workflowImpl := func(attempts int, errorReason string, nonRetryable bool, executions *[]*commonpb.WorkflowExecution) testcore.WorkflowTaskHandler {
		attemptCount := 1

		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			*executions = append(*executions, task.GetWorkflowExecution())
			attemptCount++
			if attemptCount > attempts {
				return []*commandpb.Command{
					commandpb.Command_builder{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
							Result: payloads.EncodeString("succeed-after-retry"),
						}.Build(),
					}.Build()}, nil
			}
			return []*commandpb.Command{
				commandpb.Command_builder{
					CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
					FailWorkflowExecutionCommandAttributes: commandpb.FailWorkflowExecutionCommandAttributes_builder{
						// Reason:  "retryable-error",
						Failure: failure.NewServerFailure(errorReason, nonRetryable),
					}.Build(),
				}.Build()}, nil
		}

		return wtHandler
	}

	// Fail using attempt
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		}.Build(),
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

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
	request = workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		}.Build(),
	}.Build()

	we, err0 = s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

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
		return workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:          uuid.NewString(),
			Namespace:          s.Namespace().String(),
			WorkflowId:         testcore.RandomizeStr(s.T().Name()),
			WorkflowType:       tv.WorkflowType(),
			TaskQueue:          tv.TaskQueue(),
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           tv.WorkerIdentity(),
			SearchAttributes: commonpb.SearchAttributes_builder{
				IndexedFields: map[string]*commonpb.Payload{
					saFieldName: payload.EncodeString("1.0.0"),
				},
			}.Build(),
		}.Build()
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
	require.False(t, resp.GetStarted())
	require.Equalf(t, resp.GetStatus(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		"Expected workflow to be running, but got %s", resp.GetStatus())
}

func requireStartedAndRunning(t *testing.T, resp *workflowservice.StartWorkflowExecutionResponse) {
	t.Helper()
	require.True(t, resp.GetStarted())
	require.Equalf(t, resp.GetStatus(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		"Expected workflow to be running, but got %s", resp.GetStatus())
}
