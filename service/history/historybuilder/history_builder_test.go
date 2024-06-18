// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package historybuilder

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tests"
)

type (
	historyBuilderSuite struct {
		suite.Suite
		*require.Assertions

		now            time.Time
		version        int64
		nextEventID    int64
		nextTaskID     int64
		mockTimeSource *clock.EventTimeSource

		historyBuilder *HistoryBuilder
	}
)

var (
	testNamespaceID   = namespace.ID(uuid.New())
	testNamespaceName = namespace.Name("test namespace")
	testWorkflowID    = "test workflow ID"
	testRunID         = uuid.New()

	testParentNamespaceID      = uuid.New()
	testParentNamespaceName    = "test parent namespace"
	testParentWorkflowID       = "test parent workflow ID"
	testParentRunID            = uuid.New()
	testParentInitiatedID      = rand.Int63()
	testParentInitiatedVersion = rand.Int63()

	testRootWorkflowID = "test root workflow ID"
	testRootRunID      = uuid.New()

	testIdentity  = "test identity"
	testRequestID = uuid.New()

	testPayload = &commonpb.Payload{
		Metadata: map[string][]byte{
			"random metadata key": []byte("random metadata value"),
		},
		Data: []byte("random data"),
	}
	testPayloads     = &commonpb.Payloads{Payloads: []*commonpb.Payload{testPayload}}
	testWorkflowType = &commonpb.WorkflowType{
		Name: "test workflow type",
	}
	testActivityType = &commonpb.ActivityType{
		Name: "test activity type",
	}
	testTaskQueue = &taskqueuepb.TaskQueue{
		Name: "test task queue",
		Kind: enumspb.TaskQueueKind(rand.Int31n(int32(len(enumspb.TaskQueueKind_name)))),
	}
	testRetryPolicy = &commonpb.RetryPolicy{
		InitialInterval:        durationpb.New(time.Duration(rand.Int63())),
		BackoffCoefficient:     rand.Float64(),
		MaximumAttempts:        rand.Int31(),
		MaximumInterval:        durationpb.New(time.Duration(rand.Int63())),
		NonRetryableErrorTypes: []string{"test non retryable error type"},
	}
	testCronSchedule = "12 * * * *"
	testMemo         = &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"random memo key": testPayload,
		},
	}
	testSearchAttributes = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"random search attribute key": testPayload,
		},
	}
	testHeader = &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"random header key": testPayload,
		},
	}
	testFailure       = &failurepb.Failure{}
	testRequestReason = "test request reason"
)

func TestHistoryBuilderSuite(t *testing.T) {
	s := new(historyBuilderSuite)
	suite.Run(t, s)
}

func (s *historyBuilderSuite) SetupSuite() {
}

func (s *historyBuilderSuite) TearDownSuite() {
}

func (s *historyBuilderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.now = time.Now().UTC()
	s.version = rand.Int63()
	s.nextEventID = rand.Int63()
	s.nextTaskID = rand.Int63()
	s.mockTimeSource = clock.NewEventTimeSource()
	s.mockTimeSource.Update(s.now)

	s.historyBuilder = New(
		s.mockTimeSource,
		s.taskIDGenerator,
		s.version,
		s.nextEventID,
		nil,
		metrics.NoopMetricsHandler,
	)
}

func (s *historyBuilderSuite) TearDownTest() {

}

/* workflow */
func (s *historyBuilderSuite) TestWorkflowExecutionStarted() {
	attempt := rand.Int31()
	workflowExecutionExpirationTime := timestamppb.New(time.Unix(0, rand.Int63()))
	continueAsNewInitiator := enumspb.ContinueAsNewInitiator(rand.Int31n(int32(len(enumspb.ContinueAsNewInitiator_name))))
	firstWorkflowTaskBackoff := durationpb.New(time.Duration(rand.Int63()))

	workflowExecutionTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowRunTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))

	resetPoints := &workflowpb.ResetPoints{}
	prevRunID := uuid.New()
	firstRunID := uuid.New()
	originalRunID := uuid.New()

	request := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId: testNamespaceID.String(),
		ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
			NamespaceId: testParentNamespaceID,
			Namespace:   testParentNamespaceName,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: testParentWorkflowID,
				RunId:      testParentRunID,
			},
			InitiatedId:      testParentInitiatedID,
			InitiatedVersion: testParentInitiatedVersion,
		},
		Attempt:                         attempt,
		WorkflowExecutionExpirationTime: workflowExecutionExpirationTime,
		ContinueAsNewInitiator:          continueAsNewInitiator,
		ContinuedFailure:                testFailure,
		LastCompletionResult:            testPayloads,
		FirstWorkflowTaskBackoff:        firstWorkflowTaskBackoff,
		RootExecutionInfo: &workflowspb.RootExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: testRootWorkflowID,
				RunId:      testRootRunID,
			},
		},

		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                testNamespaceName.String(),
			WorkflowId:               testWorkflowID,
			WorkflowType:             testWorkflowType,
			TaskQueue:                testTaskQueue,
			Input:                    testPayloads,
			WorkflowExecutionTimeout: workflowExecutionTimeout,
			WorkflowRunTimeout:       workflowRunTimeout,
			WorkflowTaskTimeout:      workflowTaskStartToCloseTimeout,
			Identity:                 testIdentity,
			RequestId:                testRequestID,
			// WorkflowIdReusePolicy: not used for event generation
			RetryPolicy:      testRetryPolicy,
			CronSchedule:     testCronSchedule,
			Memo:             testMemo,
			SearchAttributes: testSearchAttributes,
			Header:           testHeader,
		},
	}

	event := s.historyBuilder.AddWorkflowExecutionStartedEvent(
		s.now,
		request,
		resetPoints,
		prevRunID,
		firstRunID,
		originalRunID,
	)
	s.Equal(event, s.flush())
	protorequire.ProtoEqual(
		s.T(),
		&historypb.HistoryEvent{
			EventId:   s.nextEventID,
			TaskId:    s.nextTaskID,
			EventTime: timestamppb.New(s.now),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Version:   s.version,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:                    testWorkflowType,
					TaskQueue:                       testTaskQueue,
					Header:                          testHeader,
					Input:                           testPayloads,
					WorkflowRunTimeout:              workflowRunTimeout,
					WorkflowExecutionTimeout:        workflowExecutionTimeout,
					WorkflowTaskTimeout:             workflowTaskStartToCloseTimeout,
					ContinuedExecutionRunId:         prevRunID,
					PrevAutoResetPoints:             resetPoints,
					Identity:                        testIdentity,
					RetryPolicy:                     testRetryPolicy,
					Attempt:                         attempt,
					WorkflowExecutionExpirationTime: workflowExecutionExpirationTime,
					CronSchedule:                    testCronSchedule,
					LastCompletionResult:            testPayloads,
					ContinuedFailure:                testFailure,
					Initiator:                       continueAsNewInitiator,
					FirstWorkflowTaskBackoff:        firstWorkflowTaskBackoff,
					FirstExecutionRunId:             firstRunID,
					OriginalExecutionRunId:          originalRunID,
					Memo:                            testMemo,
					SearchAttributes:                testSearchAttributes,
					WorkflowId:                      testWorkflowID,

					ParentWorkflowNamespace:   testParentNamespaceName,
					ParentWorkflowNamespaceId: testParentNamespaceID,
					ParentWorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: testParentWorkflowID,
						RunId:      testParentRunID,
					},
					ParentInitiatedEventId:      testParentInitiatedID,
					ParentInitiatedEventVersion: testParentInitiatedVersion,

					RootWorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: testRootWorkflowID,
						RunId:      testRootRunID,
					},
				},
			},
		},
		event,
	)
}

func (s *historyBuilderSuite) TestWorkflowExecutionCancelRequested() {
	initiatedEventID := rand.Int63()
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			// Namespace: not used for test
			// WorkflowExecution: not used for test
			// FirstExecutionRunId: not used for test
			Identity:  testIdentity,
			RequestId: testRequestID,
			Reason:    testRequestReason,
		},
		ExternalInitiatedEventId: initiatedEventID,
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: testParentWorkflowID,
			RunId:      testParentRunID,
		},
		// ChildWorkflowOnly: not used for test
	}

	event := s.historyBuilder.AddWorkflowExecutionCancelRequestedEvent(
		request,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
				Cause:                    testRequestReason,
				Identity:                 testIdentity,
				ExternalInitiatedEventId: initiatedEventID,
				ExternalWorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testParentWorkflowID,
					RunId:      testParentRunID,
				},
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionSignaled() {
	signalName := "random signal name"
	event := s.historyBuilder.AddWorkflowExecutionSignaledEvent(
		signalName, testPayloads, testIdentity, testHeader, false, nil,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: signalName,
				Input:      testPayloads,
				Identity:   testIdentity,
				Header:     testHeader,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionMarkerRecord() {
	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.RecordMarkerCommandAttributes{
		MarkerName: "random marker name",
		Details: map[string]*commonpb.Payloads{
			"random marker details key": testPayloads,
		},
		Header:  testHeader,
		Failure: testFailure,
	}
	event := s.historyBuilder.AddMarkerRecordedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{
			MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				MarkerName:                   attributes.MarkerName,
				Details:                      attributes.Details,
				Header:                       attributes.Header,
				Failure:                      attributes.Failure,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionSearchAttribute() {
	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
		SearchAttributes: testSearchAttributes,
	}
	event := s.historyBuilder.AddUpsertWorkflowSearchAttributesEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
			UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				SearchAttributes:             attributes.SearchAttributes,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionMemo() {
	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.ModifyWorkflowPropertiesCommandAttributes{
		UpsertedMemo: testMemo,
	}
	event := s.historyBuilder.AddWorkflowPropertiesModifiedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowPropertiesModifiedEventAttributes{
			WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				UpsertedMemo:                 attributes.UpsertedMemo,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionCompleted() {
	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.CompleteWorkflowExecutionCommandAttributes{
		Result: testPayloads,
	}
	event := s.historyBuilder.AddCompletedWorkflowEvent(
		workflowTaskCompletionEventID,
		attributes,
		"",
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Result:                       attributes.Result,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionFailed() {
	workflowTaskCompletionEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	attributes := &commandpb.FailWorkflowExecutionCommandAttributes{
		Failure: testFailure,
	}
	event, batchID := s.historyBuilder.AddFailWorkflowEvent(
		workflowTaskCompletionEventID,
		retryState,
		attributes,
		"",
	)
	s.Equal(event, s.flush())
	s.Equal(batchID, event.EventId)
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Failure:                      attributes.Failure,
				RetryState:                   retryState,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionTimeout() {
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddTimeoutWorkflowEvent(
		retryState,
		"",
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
				RetryState: retryState,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionCancelled() {
	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.CancelWorkflowExecutionCommandAttributes{
		Details: testPayloads,
	}
	event := s.historyBuilder.AddWorkflowExecutionCanceledEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Details:                      attributes.Details,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionTerminated() {
	reason := "random reason"
	event := s.historyBuilder.AddWorkflowExecutionTerminatedEvent(
		reason,
		testPayloads,
		testIdentity,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason:   reason,
				Details:  testPayloads,
				Identity: testIdentity,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionContinueAsNew() {
	workflowTaskCompletionEventID := rand.Int63()
	initiator := enumspb.ContinueAsNewInitiator(rand.Int31n(int32(len(enumspb.ContinueAsNewInitiator_name))))
	firstWorkflowTaskBackoff := durationpb.New(time.Duration(rand.Int63()))
	workflowRunTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))

	attributes := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
		WorkflowType:         testWorkflowType,
		TaskQueue:            testTaskQueue,
		Input:                testPayloads,
		WorkflowRunTimeout:   workflowRunTimeout,
		WorkflowTaskTimeout:  workflowTaskStartToCloseTimeout,
		BackoffStartInterval: firstWorkflowTaskBackoff,
		RetryPolicy:          testRetryPolicy,
		Initiator:            initiator,
		Failure:              testFailure,
		LastCompletionResult: testPayloads,
		CronSchedule:         testCronSchedule,
		Header:               testHeader,
		Memo:                 testMemo,
		SearchAttributes:     testSearchAttributes,
	}
	event := s.historyBuilder.AddContinuedAsNewEvent(
		workflowTaskCompletionEventID,
		testRunID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				NewExecutionRunId:            testRunID,
				WorkflowType:                 testWorkflowType,
				TaskQueue:                    testTaskQueue,
				Header:                       testHeader,
				Input:                        testPayloads,
				WorkflowRunTimeout:           workflowRunTimeout,
				WorkflowTaskTimeout:          workflowTaskStartToCloseTimeout,
				BackoffStartInterval:         firstWorkflowTaskBackoff,
				Initiator:                    initiator,
				Failure:                      testFailure,
				LastCompletionResult:         testPayloads,
				Memo:                         testMemo,
				SearchAttributes:             testSearchAttributes,
			},
		},
	}, event)
}

/* workflow */

/* workflow tasks */
func (s *historyBuilderSuite) TestWorkflowTaskScheduled() {
	startToCloseTimeout := time.Duration(rand.Int31()) * time.Second
	attempt := rand.Int31()
	event := s.historyBuilder.AddWorkflowTaskScheduledEvent(
		testTaskQueue,
		durationpb.New(startToCloseTimeout),
		attempt,
		s.now,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           testTaskQueue,
				StartToCloseTimeout: durationpb.New(startToCloseTimeout),
				Attempt:             attempt,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowTaskStarted() {
	scheduledEventID := rand.Int63()
	event := s.historyBuilder.AddWorkflowTaskStartedEvent(
		scheduledEventID,
		testRequestID,
		testIdentity,
		s.now,
		false,
		123678,
		nil,
		int64(0),
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId:     scheduledEventID,
				Identity:             testIdentity,
				RequestId:            testRequestID,
				SuggestContinueAsNew: false,
				HistorySizeBytes:     123678,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowTaskCompleted() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	checksum := "random checksum"
	sdkMetadata := &sdkpb.WorkflowTaskCompletedMetadata{CoreUsedFlags: []uint32{1, 2, 3}, LangUsedFlags: []uint32{4, 5, 6}}
	meteringMeta := &commonpb.MeteringMetadata{NonfirstLocalActivityExecutionAttempts: 42}
	event := s.historyBuilder.AddWorkflowTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		testIdentity,
		checksum,
		&commonpb.WorkerVersionStamp{BuildId: "build_id_9"},
		sdkMetadata,
		meteringMeta,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Identity:         testIdentity,
				BinaryChecksum:   checksum,
				WorkerVersion:    &commonpb.WorkerVersionStamp{BuildId: "build_id_9"},
				SdkMetadata:      sdkMetadata,
				MeteringMetadata: meteringMeta,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowTaskFailed() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	cause := enumspb.WorkflowTaskFailedCause(rand.Int31n(int32(len(enumspb.WorkflowTaskFailedCause_name))))
	baseRunID := uuid.New()
	newRunID := uuid.New()
	forkEventVersion := rand.Int63()
	checksum := "random checksum"
	event := s.historyBuilder.AddWorkflowTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		cause,
		testFailure,
		testIdentity,
		baseRunID,
		newRunID,
		forkEventVersion,
		checksum,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Cause:            cause,
				Failure:          testFailure,
				Identity:         testIdentity,
				BaseRunId:        baseRunID,
				NewRunId:         newRunID,
				ForkEventVersion: forkEventVersion,
				BinaryChecksum:   checksum,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowTaskTimeout() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	timeoutType := enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name))))
	event := s.historyBuilder.AddWorkflowTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		timeoutType,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{
			WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				TimeoutType:      timeoutType,
			},
		},
	}, event)
}

/* workflow tasks */

/* activity tasks */
func (s *historyBuilderSuite) TestActivityTaskScheduled() {
	workflowTaskCompletionEventID := rand.Int63()
	activityID := "random activity ID"
	scheduleToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))
	scheduleToStartTimeout := durationpb.New(time.Duration(rand.Int63()))
	startToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))
	heartbeatTimeout := durationpb.New(time.Duration(rand.Int63()))
	attributes := &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             activityID,
		ActivityType:           testActivityType,
		TaskQueue:              testTaskQueue,
		Header:                 testHeader,
		Input:                  testPayloads,
		RetryPolicy:            testRetryPolicy,
		ScheduleToCloseTimeout: scheduleToCloseTimeout,
		ScheduleToStartTimeout: scheduleToStartTimeout,
		StartToCloseTimeout:    startToCloseTimeout,
		HeartbeatTimeout:       heartbeatTimeout,
	}
	event := s.historyBuilder.AddActivityTaskScheduledEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				ActivityId:                   activityID,
				ActivityType:                 testActivityType,
				TaskQueue:                    testTaskQueue,
				Header:                       testHeader,
				Input:                        testPayloads,
				RetryPolicy:                  testRetryPolicy,
				ScheduleToCloseTimeout:       scheduleToCloseTimeout,
				ScheduleToStartTimeout:       scheduleToStartTimeout,
				StartToCloseTimeout:          startToCloseTimeout,
				HeartbeatTimeout:             heartbeatTimeout,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskStarted() {
	scheduledEventID := rand.Int63()
	attempt := rand.Int31()
	stamp := &commonpb.WorkerVersionStamp{BuildId: "bld", UseVersioning: false}
	event := s.historyBuilder.AddActivityTaskStartedEvent(
		scheduledEventID,
		attempt,
		testRequestID,
		testIdentity,
		testFailure,
		stamp,
		int64(0),
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				Attempt:          attempt,
				Identity:         testIdentity,
				RequestId:        testRequestID,
				LastFailure:      testFailure,
				WorkerVersion:    stamp,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskCancelRequested() {
	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	event := s.historyBuilder.AddActivityTaskCancelRequestedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
			ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				ScheduledEventId:             scheduledEventID,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskCompleted() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := s.historyBuilder.AddActivityTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		testIdentity,
		testPayloads,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Result:           testPayloads,
				Identity:         testIdentity,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskFailed() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddActivityTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		testFailure,
		retryState,
		testIdentity,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Failure:          testFailure,
				RetryState:       retryState,
				Identity:         testIdentity,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskTimeout() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddActivityTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		testFailure,
		retryState,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Failure:          testFailure,
				RetryState:       retryState,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskCancelled() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	cancelRequestedEventID := rand.Int63()
	event := s.historyBuilder.AddActivityTaskCanceledEvent(
		scheduledEventID,
		startedEventID,
		cancelRequestedEventID,
		testPayloads,
		testIdentity,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				ScheduledEventId:             scheduledEventID,
				StartedEventId:               startedEventID,
				LatestCancelRequestedEventId: cancelRequestedEventID,
				Details:                      testPayloads,
				Identity:                     testIdentity,
			},
		},
	}, event)
}

/* activity tasks */

/* timer */
func (s *historyBuilderSuite) TestTimerStarted() {
	workflowTaskCompletionEventID := rand.Int63()
	timerID := "random timer ID"
	startToFireTimeout := durationpb.New(time.Duration(rand.Int63()))
	attributes := &commandpb.StartTimerCommandAttributes{
		TimerId:            timerID,
		StartToFireTimeout: startToFireTimeout,
	}
	event := s.historyBuilder.AddTimerStartedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				TimerId:                      timerID,
				StartToFireTimeout:           startToFireTimeout,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestTimerFired() {
	startedEventID := rand.Int63()
	timerID := "random timer ID"
	event := s.historyBuilder.AddTimerFiredEvent(
		startedEventID,
		timerID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
				TimerId:        timerID,
				StartedEventId: startedEventID,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestTimerCancelled() {
	workflowTaskCompletionEventID := rand.Int63()
	startedEventID := rand.Int63()
	timerID := "random timer ID"
	event := s.historyBuilder.AddTimerCanceledEvent(
		workflowTaskCompletionEventID,
		startedEventID,
		timerID,
		testIdentity,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_TIMER_CANCELED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				StartedEventId:               startedEventID,
				TimerId:                      timerID,
				Identity:                     testIdentity,
			},
		},
	}, event)
}

/* timer */

/* cancellation of external workflow */
func (s *historyBuilderSuite) TestRequestCancelExternalWorkflowExecutionInitiated() {
	workflowTaskCompletionEventID := rand.Int63()
	control := "random control"
	childWorkflowOnly := rand.Int31()%2 == 0
	attributes := &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
		Namespace:         testNamespaceName.String(),
		WorkflowId:        testWorkflowID,
		RunId:             testRunID,
		Control:           control,
		ChildWorkflowOnly: childWorkflowOnly,
	}
	event := s.historyBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
		testNamespaceID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Control:           control,
				ChildWorkflowOnly: childWorkflowOnly,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestRequestCancelExternalWorkflowExecutionSuccess() {
	scheduledEventID := rand.Int63()
	event := s.historyBuilder.AddExternalWorkflowExecutionCancelRequested(
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
			ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
				InitiatedEventId: scheduledEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestRequestCancelExternalWorkflowExecutionFailed() {
	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	cause := enumspb.CancelExternalWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.CancelExternalWorkflowExecutionFailedCause_name))))
	event := s.historyBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
		cause,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				InitiatedEventId:             scheduledEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Cause: cause,
			},
		},
	}, event)
}

/* cancellation of external workflow */

/* signal to external workflow */
func (s *historyBuilderSuite) TestSignalExternalWorkflowExecutionInitiated() {
	workflowTaskCompletionEventID := rand.Int63()
	signalName := "random signal name"
	control := "random control"
	childWorkflowOnly := rand.Int31()%2 == 0
	attributes := &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
		Namespace: testNamespaceName.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		SignalName:        signalName,
		Input:             testPayloads,
		Control:           control,
		ChildWorkflowOnly: childWorkflowOnly,
		Header:            testHeader,
	}
	event := s.historyBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
		testNamespaceID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
			SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				SignalName:        signalName,
				Input:             testPayloads,
				Control:           control,
				ChildWorkflowOnly: childWorkflowOnly,
				Header:            testHeader,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestSignalExternalWorkflowExecutionSuccess() {
	scheduledEventID := rand.Int63()
	control := "random control"
	event := s.historyBuilder.AddExternalWorkflowExecutionSignaled(
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
		control,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
			ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
				InitiatedEventId: scheduledEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Control: control,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestSignalExternalWorkflowExecutionFailed() {
	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	control := "random control"
	cause := enumspb.SignalExternalWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.SignalExternalWorkflowExecutionFailedCause_name))))
	event := s.historyBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
		control,
		cause,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
			SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				InitiatedEventId:             scheduledEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Control: control,
				Cause:   cause,
			},
		},
	}, event)
}

/* signal to external workflow */

/* child workflow */
func (s *historyBuilderSuite) TestStartChildWorkflowExecutionInitiated() {
	workflowTaskCompletionEventID := rand.Int63()
	workflowExecutionTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowRunTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))
	parentClosePolicy := enumspb.ParentClosePolicy(rand.Int31n(int32(len(enumspb.ParentClosePolicy_name))))
	workflowIdReusePolicy := enumspb.WorkflowIdReusePolicy(rand.Int31n(int32(len(enumspb.WorkflowIdReusePolicy_name))))
	control := "random control"

	attributes := &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:                testNamespaceName.String(),
		WorkflowId:               testWorkflowID,
		WorkflowType:             testWorkflowType,
		TaskQueue:                testTaskQueue,
		Input:                    testPayloads,
		WorkflowExecutionTimeout: workflowExecutionTimeout,
		WorkflowRunTimeout:       workflowRunTimeout,
		WorkflowTaskTimeout:      workflowTaskStartToCloseTimeout,
		ParentClosePolicy:        parentClosePolicy,
		Control:                  control,
		WorkflowIdReusePolicy:    workflowIdReusePolicy,
		RetryPolicy:              testRetryPolicy,
		CronSchedule:             testCronSchedule,
		Memo:                     testMemo,
		SearchAttributes:         testSearchAttributes,
		Header:                   testHeader,
	}
	event := s.historyBuilder.AddStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
		testNamespaceID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowId:                   testWorkflowID,
				WorkflowType:                 testWorkflowType,
				TaskQueue:                    testTaskQueue,
				Input:                        testPayloads,
				WorkflowExecutionTimeout:     workflowExecutionTimeout,
				WorkflowRunTimeout:           workflowRunTimeout,
				WorkflowTaskTimeout:          workflowTaskStartToCloseTimeout,
				ParentClosePolicy:            parentClosePolicy,
				Control:                      control,
				WorkflowIdReusePolicy:        workflowIdReusePolicy,
				RetryPolicy:                  testRetryPolicy,
				CronSchedule:                 testCronSchedule,
				Memo:                         testMemo,
				SearchAttributes:             testSearchAttributes,
				Header:                       testHeader,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestStartChildWorkflowExecutionSuccess() {
	scheduledEventID := rand.Int63()
	event := s.historyBuilder.AddChildWorkflowExecutionStartedEvent(
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testHeader,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
			ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
				Namespace:   testNamespaceName.String(),
				NamespaceId: testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType:     testWorkflowType,
				InitiatedEventId: scheduledEventID,
				Header:           testHeader,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestStartChildWorkflowExecutionFailed() {
	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	control := "random control"
	cause := enumspb.StartChildWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.StartChildWorkflowExecutionFailedCause_name))))
	event := s.historyBuilder.AddStartChildWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
		cause,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testWorkflowType,
		control,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
			StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowId:                   testWorkflowID,
				WorkflowType:                 testWorkflowType,
				InitiatedEventId:             scheduledEventID,
				Control:                      control,
				Cause:                        cause,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestChildWorkflowExecutionCompleted() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()

	event := s.historyBuilder.AddChildWorkflowExecutionCompletedEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testPayloads,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
			ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				Result:       testPayloads,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestChildWorkflowExecutionFailed() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddChildWorkflowExecutionFailedEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testFailure,
		retryState,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
			ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				Failure:      testFailure,
				RetryState:   retryState,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestChildWorkflowExecutionTimeout() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddChildWorkflowExecutionTimedOutEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		retryState,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
			ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				RetryState:   retryState,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestChildWorkflowExecutionCancelled() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := s.historyBuilder.AddChildWorkflowExecutionCanceledEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testPayloads,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
			ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				Details:      testPayloads,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestChildWorkflowExecutionTerminated() {
	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := s.historyBuilder.AddChildWorkflowExecutionTerminatedEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
			ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
			},
		},
	}, event)
}

/* child workflow */

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithoutBuffer_SingleBatch_WithoutFlushBuffer() {
	s.testAppendFlushFinishEventWithoutBufferSingleBatch(false)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithoutBuffer_SingleBatch_WithFlushBuffer() {
	s.testAppendFlushFinishEventWithoutBufferSingleBatch(true)
}

func (s *historyBuilderSuite) testAppendFlushFinishEventWithoutBufferSingleBatch(
	flushBuffer bool,
) {
	s.historyBuilder.dbBufferBatch = nil
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}

	s.historyBuilder.add(event1)
	s.historyBuilder.add(event2)
	historyMutation, err := s.historyBuilder.Finish(flushBuffer)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithoutBuffer_MultiBatch_WithoutFlushBuffer() {
	s.testAppendFlushFinishEventWithoutBufferMultiBatch(false)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithoutBuffer_MultiBatch_WithFlushBuffer() {
	s.testAppendFlushFinishEventWithoutBufferMultiBatch(true)
}

func (s *historyBuilderSuite) testAppendFlushFinishEventWithoutBufferMultiBatch(
	flushBuffer bool,
) {
	s.historyBuilder.dbBufferBatch = nil
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	event11 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event12 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event21 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event22 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event31 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event32 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}

	// 1st batch
	s.historyBuilder.add(event11)
	s.historyBuilder.add(event12)
	s.historyBuilder.FlushAndCreateNewBatch()

	// 2nd batch
	s.historyBuilder.add(event21)
	s.historyBuilder.add(event22)
	s.historyBuilder.FlushAndCreateNewBatch()

	// 3rd batch
	s.historyBuilder.add(event31)
	s.historyBuilder.add(event32)
	s.historyBuilder.FlushAndCreateNewBatch()

	historyMutation, err := s.historyBuilder.Finish(flushBuffer)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches: [][]*historypb.HistoryEvent{
			{event11, event12},
			{event21, event22},
			{event31, event32},
		},
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithBuffer_WithoutDBBuffer_WithoutFlushBuffer() {
	s.historyBuilder.dbBufferBatch = nil
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	s.historyBuilder.add(event1)
	s.historyBuilder.add(event2)
	historyMutation, err := s.historyBuilder.Finish(false)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:        nil,
		DBClearBuffer:          false,
		DBBufferBatch:          []*historypb.HistoryEvent{event1, event2},
		MemBufferBatch:         []*historypb.HistoryEvent{event1, event2},
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithBuffer_WithoutDBBuffer_WithFlushBuffer() {
	s.historyBuilder.dbBufferBatch = nil
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	s.historyBuilder.add(event1)
	s.historyBuilder.add(event2)
	historyMutation, err := s.historyBuilder.Finish(true)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithoutBuffer_WithDBBuffer_WithoutFlushBuffer() {
	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	s.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event1, event2}
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	historyMutation, err := s.historyBuilder.Finish(false)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:        nil,
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         []*historypb.HistoryEvent{event1, event2},
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithoutBuffer_WithDBBuffer_WithFlushBuffer() {
	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	s.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event1, event2}
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	historyMutation, err := s.historyBuilder.Finish(true)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:          true,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithBuffer_WithDBBuffer_WithoutFlushBuffer() {
	event0 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	s.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event0}
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	s.historyBuilder.add(event1)
	s.historyBuilder.add(event2)
	historyMutation, err := s.historyBuilder.Finish(false)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:        nil,
		DBClearBuffer:          false,
		DBBufferBatch:          []*historypb.HistoryEvent{event1, event2},
		MemBufferBatch:         []*historypb.HistoryEvent{event0, event1, event2},
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestAppendFlushFinishEvent_WithBuffer_WithDBBuffer_WithFlushBuffer() {
	event0 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	s.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event0}
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	s.historyBuilder.add(event1)
	s.historyBuilder.add(event2)
	historyMutation, err := s.historyBuilder.Finish(true)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event0, event1, event2}},
		DBClearBuffer:          true,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestWireEventIDs_Activity() {
	scheduledEventID := rand.Int63()
	startEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	completeEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	failedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	timeoutEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	cancelEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}

	s.testWireEventIDs(scheduledEventID, startEvent, completeEvent)
	s.testWireEventIDs(scheduledEventID, startEvent, failedEvent)
	s.testWireEventIDs(scheduledEventID, startEvent, timeoutEvent)
	s.testWireEventIDs(scheduledEventID, startEvent, cancelEvent)
}

func (s *historyBuilderSuite) TestWireEventIDs_ChildWorkflow() {
	initiatedEventID := rand.Int63()
	startEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
			ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	completeEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
			ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	failedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
			ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	timeoutEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
			ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	cancelEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
			ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	terminatedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
			ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}

	s.testWireEventIDs(initiatedEventID, startEvent, completeEvent)
	s.testWireEventIDs(initiatedEventID, startEvent, failedEvent)
	s.testWireEventIDs(initiatedEventID, startEvent, timeoutEvent)
	s.testWireEventIDs(initiatedEventID, startEvent, cancelEvent)
	s.testWireEventIDs(initiatedEventID, startEvent, terminatedEvent)
}

func (s *historyBuilderSuite) testWireEventIDs(
	scheduledEventID int64,
	startEvent *historypb.HistoryEvent,
	finishEvent *historypb.HistoryEvent,
) {
	s.historyBuilder = New(
		s.mockTimeSource,
		s.taskIDGenerator,
		s.version,
		s.nextEventID,
		nil,
		metrics.NoopMetricsHandler,
	)
	s.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{startEvent}
	s.historyBuilder.memEventsBatches = nil
	s.historyBuilder.memLatestBatch = nil
	s.historyBuilder.memBufferBatch = []*historypb.HistoryEvent{finishEvent}
	s.historyBuilder.FlushBufferToCurrentBatch()

	s.Empty(s.historyBuilder.dbBufferBatch)
	s.Empty(s.historyBuilder.memEventsBatches)
	s.Equal([]*historypb.HistoryEvent{startEvent, finishEvent}, s.historyBuilder.memLatestBatch)
	s.Empty(s.historyBuilder.memBufferBatch)

	s.Equal(map[int64]int64{
		scheduledEventID: startEvent.GetEventId(),
	}, s.historyBuilder.scheduledIDToStartedID)

	switch finishEvent.GetEventType() {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		s.Equal(startEvent.GetEventId(), finishEvent.GetActivityTaskCompletedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		s.Equal(startEvent.GetEventId(), finishEvent.GetActivityTaskFailedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		s.Equal(startEvent.GetEventId(), finishEvent.GetActivityTaskTimedOutEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		s.Equal(startEvent.GetEventId(), finishEvent.GetActivityTaskCanceledEventAttributes().GetStartedEventId())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
		s.Equal(startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionCompletedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
		s.Equal(startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionFailedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
		s.Equal(startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionTimedOutEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
		s.Equal(startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionCanceledEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		s.Equal(startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionTerminatedEventAttributes().GetStartedEventId())
	}
}

func (s *historyBuilderSuite) TestHasBufferEvent() {
	historyBuilder := New(
		s.mockTimeSource,
		s.taskIDGenerator,
		s.version,
		s.nextEventID,
		nil,
		metrics.NoopMetricsHandler,
	)
	historyBuilder.dbBufferBatch = nil
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = nil
	s.False(historyBuilder.HasBufferEvents())

	historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
	}}
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = nil
	s.True(historyBuilder.HasBufferEvents())

	historyBuilder.dbBufferBatch = nil
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
	}}
	s.True(historyBuilder.HasBufferEvents())

	historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
	}}
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
	}}
	s.True(historyBuilder.HasBufferEvents())
}

func (s *historyBuilderSuite) TestBufferEvent() {
	// workflow status events will be assign event ID immediately
	workflowEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:          true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:        true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:           true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:        true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:       true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW: true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:         true,
	}

	// workflow task events will be assign event ID immediately
	workflowTaskEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED: true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:   true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED: true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:    true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT: true,
	}

	// events corresponding to commands from client will be assigned an event ID immediately
	commandEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:                         true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:                            true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:                          true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:                  true,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:                              true,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:                       true,
		enumspb.EVENT_TYPE_TIMER_STARTED:                                        true,
		enumspb.EVENT_TYPE_TIMER_CANCELED:                                       true,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED: true,
		enumspb.EVENT_TYPE_MARKER_RECORDED:                                      true,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:             true,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:         true,
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:                    true,
		enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED:                         true,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:                            true,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED:                     true,
	}

	// events corresponding to message from client will be assigned an event ID immediately
	messageEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED:  true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:  true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED: true,
	}

	// other events will not be assigned an event ID immediately (created automatically)
	otherEvents := map[enumspb.EventType]bool{}
	for _, eventType := range enumspb.EventType_value {
		if _, ok := workflowEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		if _, ok := workflowTaskEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		if _, ok := commandEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		if _, ok := messageEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		otherEvents[enumspb.EventType(eventType)] = true
	}

	// test workflowEvents, workflowTaskEvents, commandEvents will return true
	for eventType := range workflowEvents {
		s.False(s.historyBuilder.bufferEvent(eventType))
	}
	for eventType := range workflowTaskEvents {
		s.False(s.historyBuilder.bufferEvent(eventType))
	}
	for eventType := range commandEvents {
		s.False(s.historyBuilder.bufferEvent(eventType))
	}
	for eventType := range messageEvents {
		s.False(s.historyBuilder.bufferEvent(eventType))
	}
	// other events will return false
	for eventType := range otherEvents {
		s.True(s.historyBuilder.bufferEvent(eventType))
	}

	commandsWithEventsCount := 0
	for ct := range enumspb.CommandType_name {
		commandType := enumspb.CommandType(ct)
		// Unspecified is not counted.
		// ProtocolMessage command doesn't have corresponding event.
		if commandType == enumspb.COMMAND_TYPE_UNSPECIFIED || commandType == enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE {
			continue
		}
		commandsWithEventsCount++
	}
	s.Equal(
		commandsWithEventsCount,
		len(commandEvents),
		"This assertion is broken when a new command is added and no corresponding logic for corresponding command event is added to HistoryBuilder.bufferEvent",
	)
}

func (s *historyBuilderSuite) TestReorder() {
	// Only completion events are reordered.
	reorderEventTypes := map[enumspb.EventType]struct{}{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:             {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:                {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:             {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:              {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:  {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:     {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:  {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:   {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED: {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:           {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:              {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:            {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:           {},
	}
	var reorderEvents []*historypb.HistoryEvent
	for eventType := range reorderEventTypes {
		reorderEvents = append(reorderEvents, &historypb.HistoryEvent{
			EventType: eventType,
		})
	}

	var nonReorderEvents []*historypb.HistoryEvent
	for eventTypeValue := range enumspb.EventType_name {
		eventType := enumspb.EventType(eventTypeValue)
		if _, ok := reorderEventTypes[eventType]; ok || eventType == enumspb.EVENT_TYPE_UNSPECIFIED {
			continue
		}

		nonReorderEvents = append(nonReorderEvents, &historypb.HistoryEvent{
			EventType: eventType,
		})
	}

	s.Equal(
		append(nonReorderEvents, reorderEvents...),
		s.historyBuilder.reorderBuffer(append(reorderEvents, nonReorderEvents...)),
	)
}

func (s *historyBuilderSuite) TestBufferSize_Memory() {
	s.Assert().Zero(s.historyBuilder.NumBufferedEvents())
	s.Assert().Zero(s.historyBuilder.SizeInBytesOfBufferedEvents())
	s.historyBuilder.AddWorkflowExecutionSignaledEvent(
		"signal-name",
		&commonpb.Payloads{},
		"identity",
		&commonpb.Header{},
		false,
		nil,
	)
	s.Assert().Equal(1, s.historyBuilder.NumBufferedEvents())
	// the size of the proto  is non-deterministic, so just assert that it's non-zero, and it isn't really high
	s.Assert().Greater(s.historyBuilder.SizeInBytesOfBufferedEvents(), 0)
	s.Assert().Less(s.historyBuilder.SizeInBytesOfBufferedEvents(), 100)
	s.flush()
	s.Assert().Zero(s.historyBuilder.NumBufferedEvents())
	s.Assert().Zero(s.historyBuilder.SizeInBytesOfBufferedEvents())
}

func (s *historyBuilderSuite) TestBufferSize_DB() {
	s.Assert().Zero(s.historyBuilder.NumBufferedEvents())
	s.Assert().Zero(s.historyBuilder.SizeInBytesOfBufferedEvents())
	s.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}}
	s.Assert().Equal(1, s.historyBuilder.NumBufferedEvents())
	// the size of the proto  is non-deterministic, so just assert that it's non-zero, and it isn't really high
	s.Assert().Greater(s.historyBuilder.SizeInBytesOfBufferedEvents(), 0)
	s.Assert().Less(s.historyBuilder.SizeInBytesOfBufferedEvents(), 100)
	s.flush()
	s.Assert().Zero(s.historyBuilder.NumBufferedEvents())
	s.Assert().Zero(s.historyBuilder.SizeInBytesOfBufferedEvents())
}

func (s *historyBuilderSuite) TestLastEventVersion() {
	_, ok := s.historyBuilder.LastEventVersion()
	s.False(ok)

	s.historyBuilder.AddWorkflowExecutionStartedEvent(
		time.Now(),
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		},
		nil,
		"",
		"",
		"",
	)
	version, ok := s.historyBuilder.LastEventVersion()
	s.True(ok)
	s.Equal(s.version, version)

	s.historyBuilder.FlushAndCreateNewBatch()
	version, ok = s.historyBuilder.LastEventVersion()
	s.True(ok)
	s.Equal(s.version, version)

	_, err := s.historyBuilder.Finish(true)
	s.NoError(err)
	_, ok = s.historyBuilder.LastEventVersion()
	s.False(ok)

}

func (s *historyBuilderSuite) assertEventIDTaskID(
	historyMutation *HistoryMutation,
) {

	for _, event := range historyMutation.DBBufferBatch {
		s.Equal(common.BufferedEventID, event.EventId)
		s.Equal(common.EmptyEventTaskID, event.TaskId)
	}

	for _, event := range historyMutation.MemBufferBatch {
		s.Equal(common.BufferedEventID, event.EventId)
		s.Equal(common.EmptyEventTaskID, event.TaskId)
	}

	for _, eventBatch := range historyMutation.DBEventsBatches {
		for _, event := range eventBatch {
			s.NotEqual(common.BufferedEventID, event.EventId)
			s.NotEqual(common.EmptyEventTaskID, event.TaskId)
		}
	}
}

func (s *historyBuilderSuite) flush() *historypb.HistoryEvent {
	hasBufferEvents := s.historyBuilder.HasBufferEvents()
	historyMutation, err := s.historyBuilder.Finish(false)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)
	s.Equal(make(map[int64]int64), historyMutation.ScheduledIDToStartedID)

	if !hasBufferEvents {
		s.Equal(1, len(historyMutation.DBEventsBatches))
		s.Equal(1, len(historyMutation.DBEventsBatches[0]))
		return historyMutation.DBEventsBatches[0][0]
	}

	if len(historyMutation.MemBufferBatch) > 0 {
		s.Equal(1, len(historyMutation.MemBufferBatch))
		return historyMutation.MemBufferBatch[0]
	}

	s.Fail("expect one and only event")
	return nil
}

func (s *historyBuilderSuite) taskIDGenerator(number int) ([]int64, error) {
	nextTaskID := s.nextTaskID
	result := make([]int64, number)
	for i := 0; i < number; i++ {
		result[i] = nextTaskID
		nextTaskID++
	}
	return result, nil
}
