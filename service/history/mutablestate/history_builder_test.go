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

package mutablestate

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
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/primitives/timestamp"
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
	testNamespaceID   = uuid.New()
	testNamespaceName = "test namespace"
	testWorkflowID    = "test workflow ID"
	testRunID         = uuid.New()

	testParentNamespaceID   = uuid.New()
	testParentNamespaceName = "test parent namespace"
	testParentWorkflowID    = "test parent workflow ID"
	testParentRunID         = uuid.New()
	testParentInitiatedID   = rand.Int63()

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
		InitialInterval:        timestamp.DurationPtr(time.Duration(rand.Int63())),
		BackoffCoefficient:     rand.Float64(),
		MaximumAttempts:        rand.Int31(),
		MaximumInterval:        timestamp.DurationPtr(time.Duration(rand.Int63())),
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
	testFailure = &failurepb.Failure{}
)

func TestHistoryBuilderSuite(t *testing.T) {
	s := new(historyBuilderSuite)
	suite.Run(t, s)
}

func (s *historyBuilderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.now = time.Now().UTC()
	s.version = rand.Int63()
	s.nextEventID = rand.Int63()
	s.nextTaskID = rand.Int63()
	s.mockTimeSource = clock.NewEventTimeSource()
	s.mockTimeSource.Update(s.now)

	s.historyBuilder = NewMutableHistoryBuilder(
		s.mockTimeSource,
		s.taskIDGenerator,
		s.version,
		s.nextEventID,
		nil,
	)
}

func (s *historyBuilderSuite) TearDownTest() {

}

/* workflow */
func (s *historyBuilderSuite) TestWorkflowExecutionStarted() {
	attempt := rand.Int31()
	workflowExecutionExpirationTime := timestamp.TimePtr(time.Unix(0, rand.Int63()))
	continueAsNewInitiator := enumspb.ContinueAsNewInitiator(rand.Int31n(int32(len(enumspb.ContinueAsNewInitiator_name))))
	firstWorkflowTaskBackoff := timestamp.DurationPtr(time.Duration(rand.Int63()))

	workflowExecutionTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	workflowRunTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))

	resetPoints := &workflowpb.ResetPoints{}
	prevRunID := uuid.New()
	firstRunID := uuid.New()
	originalRunID := uuid.New()

	request := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId: testNamespaceID,
		ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
			NamespaceId: testParentNamespaceID,
			Namespace:   testParentNamespaceName,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: testParentWorkflowID,
				RunId:      testParentRunID,
			},
			InitiatedId: testParentInitiatedID,
		},
		Attempt:                         attempt,
		WorkflowExecutionExpirationTime: workflowExecutionExpirationTime,
		ContinueAsNewInitiator:          continueAsNewInitiator,
		ContinuedFailure:                testFailure,
		LastCompletionResult:            testPayloads,
		FirstWorkflowTaskBackoff:        firstWorkflowTaskBackoff,

		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                testNamespaceName,
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
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
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

				ParentWorkflowNamespace: testParentNamespaceName,
				ParentWorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testParentWorkflowID,
					RunId:      testParentRunID,
				},
				ParentInitiatedEventId: testParentInitiatedID,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowExecutionCancelRequested() {
	initiatedEventID := rand.Int63()
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: testNamespaceID,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			// Namespace: not used for test
			// WorkflowExecution: not used for test
			// FirstExecutionRunId: not used for test
			Identity:  testIdentity,
			RequestId: testRequestID,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
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
		signalName,
		testPayloads,
		testIdentity,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: signalName,
				Input:      testPayloads,
				Identity:   testIdentity,
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
		EventTime: timestamp.TimePtr(s.now),
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
		EventTime: timestamp.TimePtr(s.now),
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

func (s *historyBuilderSuite) TestWorkflowExecutionCompleted() {
	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.CompleteWorkflowExecutionCommandAttributes{
		Result: testPayloads,
	}
	event := s.historyBuilder.AddCompletedWorkflowEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
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
	event := s.historyBuilder.AddFailWorkflowEvent(
		workflowTaskCompletionEventID,
		retryState,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
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
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
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
		EventTime: timestamp.TimePtr(s.now),
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
		EventTime: timestamp.TimePtr(s.now),
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
	firstWorkflowTaskBackoff := timestamp.DurationPtr(time.Duration(rand.Int63()))
	workflowRunTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))

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
		EventTime: timestamp.TimePtr(s.now),
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
				Initiator:                    enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
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
	startToCloseTimeoutSeconds := rand.Int31()
	attempt := rand.Int31()
	event := s.historyBuilder.AddWorkflowTaskScheduledEvent(
		testTaskQueue,
		startToCloseTimeoutSeconds,
		attempt,
		s.now,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           testTaskQueue,
				StartToCloseTimeout: timestamp.DurationPtr(time.Duration(startToCloseTimeoutSeconds) * time.Second),
				Attempt:             attempt,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowTaskStarted() {
	scheduleEventID := rand.Int63()
	event := s.historyBuilder.AddWorkflowTaskStartedEvent(
		scheduleEventID,
		testRequestID,
		testIdentity,
		s.now,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: scheduleEventID,
				Identity:         testIdentity,
				RequestId:        testRequestID,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowTaskCompleted() {
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	checksum := "random checksum"
	event := s.historyBuilder.AddWorkflowTaskCompletedEvent(
		scheduleEventID,
		startedEventID,
		testIdentity,
		checksum,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Identity:         testIdentity,
				BinaryChecksum:   checksum,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestWorkflowTaskFailed() {
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	cause := enumspb.WorkflowTaskFailedCause(rand.Int31n(int32(len(enumspb.WorkflowTaskFailedCause_name))))
	baseRunID := uuid.New()
	newRunID := uuid.New()
	forkEventVersion := rand.Int63()
	checksum := "random checksum"
	event := s.historyBuilder.AddWorkflowTaskFailedEvent(
		scheduleEventID,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				ScheduledEventId: scheduleEventID,
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
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	timeoutType := enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name))))
	event := s.historyBuilder.AddWorkflowTaskTimedOutEvent(
		scheduleEventID,
		startedEventID,
		timeoutType,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{
			WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
				ScheduledEventId: scheduleEventID,
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
	scheduleToCloseTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	scheduleToStartTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	startToCloseTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	heartbeatTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	attributes := &commandpb.ScheduleActivityTaskCommandAttributes{
		Namespace:              testNamespaceName,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName,
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
	scheduleEventID := rand.Int63()
	attempt := rand.Int31()
	event := s.historyBuilder.AddActivityTaskStartedEvent(
		scheduleEventID,
		attempt,
		testRequestID,
		testIdentity,
		testFailure,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduleEventID,
				Attempt:          attempt,
				Identity:         testIdentity,
				RequestId:        testRequestID,
				LastFailure:      testFailure,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskCancelRequested() {
	workflowTaskCompletionEventID := rand.Int63()
	scheduleEventID := rand.Int63()
	event := s.historyBuilder.AddActivityTaskCancelRequestedEvent(
		workflowTaskCompletionEventID,
		scheduleEventID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
			ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				ScheduledEventId:             scheduleEventID,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskCompleted() {
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := s.historyBuilder.AddActivityTaskCompletedEvent(
		scheduleEventID,
		startedEventID,
		testIdentity,
		testPayloads,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Result:           testPayloads,
				Identity:         testIdentity,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskFailed() {
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddActivityTaskFailedEvent(
		scheduleEventID,
		startedEventID,
		testFailure,
		retryState,
		testIdentity,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Failure:          testFailure,
				RetryState:       retryState,
				Identity:         testIdentity,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskTimeout() {
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddActivityTaskTimedOutEvent(
		scheduleEventID,
		startedEventID,
		testFailure,
		retryState,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Failure:          testFailure,
				RetryState:       retryState,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestActivityTaskCancelled() {
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	cancelRequestedEventID := rand.Int63()
	event := s.historyBuilder.AddActivityTaskCanceledEvent(
		scheduleEventID,
		startedEventID,
		cancelRequestedEventID,
		testPayloads,
		testIdentity,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				ScheduledEventId:             scheduleEventID,
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
	startToFireTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
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
		EventTime: timestamp.TimePtr(s.now),
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
		EventTime: timestamp.TimePtr(s.now),
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
		EventTime: timestamp.TimePtr(s.now),
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
		Namespace:         testNamespaceName,
		WorkflowId:        testWorkflowID,
		RunId:             testRunID,
		Control:           control,
		ChildWorkflowOnly: childWorkflowOnly,
	}
	event := s.historyBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName,
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
	scheduleEventID := rand.Int63()
	event := s.historyBuilder.AddExternalWorkflowExecutionCancelRequested(
		scheduleEventID,
		testNamespaceName,
		testWorkflowID,
		testRunID,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
			ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
				InitiatedEventId: scheduleEventID,
				Namespace:        testNamespaceName,
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
	scheduleEventID := rand.Int63()
	cause := enumspb.CancelExternalWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.CancelExternalWorkflowExecutionFailedCause_name))))
	event := s.historyBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduleEventID,
		testNamespaceName,
		testWorkflowID,
		testRunID,
		cause,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				InitiatedEventId:             scheduleEventID,
				Namespace:                    testNamespaceName,
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
		Namespace: testNamespaceName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		SignalName:        signalName,
		Input:             testPayloads,
		Control:           control,
		ChildWorkflowOnly: childWorkflowOnly,
	}
	event := s.historyBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
			SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				SignalName:        signalName,
				Input:             testPayloads,
				Control:           control,
				ChildWorkflowOnly: childWorkflowOnly,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestSignalExternalWorkflowExecutionSuccess() {
	scheduleEventID := rand.Int63()
	control := "random control"
	event := s.historyBuilder.AddExternalWorkflowExecutionSignaled(
		scheduleEventID,
		testNamespaceName,
		testWorkflowID,
		testRunID,
		control,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
			ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
				InitiatedEventId: scheduleEventID,
				Namespace:        testNamespaceName,
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
	scheduleEventID := rand.Int63()
	control := "random control"
	cause := enumspb.SignalExternalWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.SignalExternalWorkflowExecutionFailedCause_name))))
	event := s.historyBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduleEventID,
		testNamespaceName,
		testWorkflowID,
		testRunID,
		control,
		cause,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
			SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				InitiatedEventId:             scheduleEventID,
				Namespace:                    testNamespaceName,
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
	workflowExecutionTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	workflowRunTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := timestamp.DurationPtr(time.Duration(rand.Int63()))
	parentClosePolicy := enumspb.ParentClosePolicy(rand.Int31n(int32(len(enumspb.ParentClosePolicy_name))))
	workflowIdReusePolicy := enumspb.WorkflowIdReusePolicy(rand.Int31n(int32(len(enumspb.WorkflowIdReusePolicy_name))))
	control := "random control"

	attributes := &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:                testNamespaceName,
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
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   s.nextEventID,
		TaskId:    s.nextTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName,
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
	scheduleEventID := rand.Int63()
	event := s.historyBuilder.AddChildWorkflowExecutionStartedEvent(
		scheduleEventID,
		testNamespaceName,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
			ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
				Namespace: testNamespaceName,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType:     testWorkflowType,
				InitiatedEventId: scheduleEventID,
				Header:           testHeader,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestStartChildWorkflowExecutionFailed() {
	workflowTaskCompletionEventID := rand.Int63()
	scheduleEventID := rand.Int63()
	control := "random control"
	cause := enumspb.StartChildWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.StartChildWorkflowExecutionFailedCause_name))))
	event := s.historyBuilder.AddStartChildWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduleEventID,
		cause,
		testNamespaceName,
		testWorkflowID,
		testWorkflowType,
		control,
	)
	s.Equal(event, s.flush())
	s.Equal(&historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
			StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName,
				WorkflowId:                   testWorkflowID,
				WorkflowType:                 testWorkflowType,
				InitiatedEventId:             scheduleEventID,
				Control:                      control,
				Cause:                        cause,
			},
		},
	}, event)
}

func (s *historyBuilderSuite) TestChildWorkflowExecutionCompleted() {
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()

	event := s.historyBuilder.AddChildWorkflowExecutionCompletedEvent(
		scheduleEventID,
		startedEventID,
		testNamespaceName,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
			ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
				InitiatedEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName,
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
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddChildWorkflowExecutionFailedEvent(
		scheduleEventID,
		startedEventID,
		testNamespaceName,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
			ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
				InitiatedEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName,
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
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := s.historyBuilder.AddChildWorkflowExecutionTimedOutEvent(
		scheduleEventID,
		startedEventID,
		testNamespaceName,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
			ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
				InitiatedEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName,
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
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := s.historyBuilder.AddChildWorkflowExecutionCanceledEvent(
		scheduleEventID,
		startedEventID,
		testNamespaceName,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
			ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
				InitiatedEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName,
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
	scheduleEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := s.historyBuilder.AddChildWorkflowExecutionTerminatedEvent(
		scheduleEventID,
		startedEventID,
		testNamespaceName,
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
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		Version:   s.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
			ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
				InitiatedEventId: scheduleEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName,
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

	s.historyBuilder.appendEvents(event1)
	s.historyBuilder.appendEvents(event2)
	historyMutation, err := s.historyBuilder.Finish(flushBuffer)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:     [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:       false,
		DBBufferBatch:       nil,
		MemBufferBatch:      nil,
		ScheduleIDToStartID: make(map[int64]int64),
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
	s.historyBuilder.appendEvents(event11)
	s.historyBuilder.appendEvents(event12)
	s.historyBuilder.FlushAndCreateNewBatch()

	// 2nd batch
	s.historyBuilder.appendEvents(event21)
	s.historyBuilder.appendEvents(event22)
	s.historyBuilder.FlushAndCreateNewBatch()

	// 3rd batch
	s.historyBuilder.appendEvents(event31)
	s.historyBuilder.appendEvents(event32)
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
		DBClearBuffer:       false,
		DBBufferBatch:       nil,
		MemBufferBatch:      nil,
		ScheduleIDToStartID: make(map[int64]int64),
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

	s.historyBuilder.appendEvents(event1)
	s.historyBuilder.appendEvents(event2)
	historyMutation, err := s.historyBuilder.Finish(false)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:     nil,
		DBClearBuffer:       false,
		DBBufferBatch:       []*historypb.HistoryEvent{event1, event2},
		MemBufferBatch:      []*historypb.HistoryEvent{event1, event2},
		ScheduleIDToStartID: make(map[int64]int64),
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

	s.historyBuilder.appendEvents(event1)
	s.historyBuilder.appendEvents(event2)
	historyMutation, err := s.historyBuilder.Finish(true)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:     [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:       false,
		DBBufferBatch:       nil,
		MemBufferBatch:      nil,
		ScheduleIDToStartID: make(map[int64]int64),
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
		DBEventsBatches:     nil,
		DBClearBuffer:       false,
		DBBufferBatch:       nil,
		MemBufferBatch:      []*historypb.HistoryEvent{event1, event2},
		ScheduleIDToStartID: make(map[int64]int64),
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
		DBEventsBatches:     [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:       true,
		DBBufferBatch:       nil,
		MemBufferBatch:      nil,
		ScheduleIDToStartID: make(map[int64]int64),
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

	s.historyBuilder.appendEvents(event1)
	s.historyBuilder.appendEvents(event2)
	historyMutation, err := s.historyBuilder.Finish(false)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:     nil,
		DBClearBuffer:       false,
		DBBufferBatch:       []*historypb.HistoryEvent{event1, event2},
		MemBufferBatch:      []*historypb.HistoryEvent{event0, event1, event2},
		ScheduleIDToStartID: make(map[int64]int64),
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

	s.historyBuilder.appendEvents(event1)
	s.historyBuilder.appendEvents(event2)
	historyMutation, err := s.historyBuilder.Finish(true)
	s.NoError(err)
	s.assertEventIDTaskID(historyMutation)

	s.Equal(&HistoryMutation{
		DBEventsBatches:     [][]*historypb.HistoryEvent{{event0, event1, event2}},
		DBClearBuffer:       true,
		DBBufferBatch:       nil,
		MemBufferBatch:      nil,
		ScheduleIDToStartID: make(map[int64]int64),
	}, historyMutation)
}

func (s *historyBuilderSuite) TestWireEventIDs_Activity() {
	scheduleEventID := rand.Int63()
	startEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduleEventID,
			},
		},
	}
	completeEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduleEventID,
			},
		},
	}
	failedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduleEventID,
			},
		},
	}
	timeoutEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: scheduleEventID,
			},
		},
	}
	cancelEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				ScheduledEventId: scheduleEventID,
			},
		},
	}

	s.testWireEventIDs(scheduleEventID, startEvent, completeEvent)
	s.testWireEventIDs(scheduleEventID, startEvent, failedEvent)
	s.testWireEventIDs(scheduleEventID, startEvent, timeoutEvent)
	s.testWireEventIDs(scheduleEventID, startEvent, cancelEvent)
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
	scheduleID int64,
	startEvent *historypb.HistoryEvent,
	finishEvent *historypb.HistoryEvent,
) {
	s.historyBuilder = NewMutableHistoryBuilder(
		s.mockTimeSource,
		s.taskIDGenerator,
		s.version,
		s.nextEventID,
		nil,
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
		scheduleID: startEvent.GetEventId(),
	}, s.historyBuilder.scheduleIDToStartedID)

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
	historyBuilder := NewMutableHistoryBuilder(
		s.mockTimeSource,
		s.taskIDGenerator,
		s.version,
		s.nextEventID,
		nil,
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

	// events corresponding to commands from client will be assign event ID immediately
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
	}

	// other events will not be assign event ID immediately
	otherEvents := map[enumspb.EventType]bool{}
OtherEventsLoop:
	for _, eventType := range enumspb.EventType_value {
		if _, ok := workflowEvents[enumspb.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		if _, ok := workflowTaskEvents[enumspb.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		if _, ok := commandEvents[enumspb.EventType(eventType)]; ok {
			continue OtherEventsLoop
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
	// other events will return false
	for eventType := range otherEvents {
		s.True(s.historyBuilder.bufferEvent(eventType))
	}

	commandTypes := enumspb.CommandType_name
	delete(commandTypes, 0) // Remove Unspecified.
	s.Equal(
		len(commandTypes),
		len(commandEvents),
		"This assertion will be broken a new command is added and no corresponding logic added to shouldBufferEvent()",
	)
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
	s.Equal(make(map[int64]int64), historyMutation.ScheduleIDToStartID)

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
