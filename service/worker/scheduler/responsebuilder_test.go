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

package scheduler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
)

func TestResponseBuilder(t *testing.T) {
	nilLogger := log.NewNoopLogger()
	request := schedspb.WatchWorkflowRequest{
		Execution: &common.WorkflowExecution{WorkflowId: "workflow-id-1"},
	}
	t.Run("when execution status is RUNNING", func(t *testing.T) {
		status := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		t.Run("when LogPoll requested will return errTryAgain", func(t *testing.T) {
			longPollRequest := schedspb.WatchWorkflowRequest{LongPoll: true}
			rb := newResponseBuilder(&longPollRequest, status, nilLogger, eventStorageSize-recordOverheadSize)
			event := historypb.HistoryEvent{}

			response, err := rb.Build(&event)

			assertError(t, err, errTryAgain)
			assertResponseIsNil(t, response)
		})
		t.Run("when it is not a LongPoll will return response with nil result", func(t *testing.T) {
			rb := newResponseBuilder(&request, status, nilLogger, eventStorageSize-recordOverheadSize)
			event := historypb.HistoryEvent{}

			response, err := rb.Build(&event)

			assertError(t, err, nil)
			assertResponseResult(t, response, nil)
			assertResponseStatus(t, response, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

		})
	})

	t.Run("when status is COMPLETED", func(t *testing.T) {
		status := enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		t.Run("when no attrs return errNoAttrs", func(t *testing.T) {
			rb := newResponseBuilder(&request, status, nilLogger, eventStorageSize-recordOverheadSize)
			event := historypb.HistoryEvent{}

			response, err := rb.Build(&event)

			assertError(t, err, errNoAttrs)
			assertResponseIsNil(t, response)
		})
		t.Run("when NewExecutionRunId is non empty returns errFollow", func(t *testing.T) {
			rb := newResponseBuilder(&request, status, nilLogger, eventStorageSize-recordOverheadSize)
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
						NewExecutionRunId: "some-run-id",
					},
				},
			}

			response, err := rb.Build(&event)

			assertError(t, err, errFollow("some-run-id"))
			assertResponseIsNil(t, response)
		})
		t.Run("when result is smaller then maximum should place result in response", func(t *testing.T) {
			rb := newResponseBuilder(&request, status, nilLogger, eventStorageSize-recordOverheadSize)
			data1K := make([]byte, 1024)
			payload := common.Payload{
				Data: data1K,
			}
			payloads := []*common.Payload{&payload}
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
						Result: &common.Payloads{
							Payloads: payloads,
						},
					},
				},
			}

			response, err := rb.Build(&event)

			assertError(t, err, nil)
			assertResponsePayload(t, response, payloads)
			assertResponseStatus(t, response, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
		})
		t.Run("when result is bigger then eventStorageSize will drop the result", func(t *testing.T) {
			rb := newResponseBuilder(&request, status, nilLogger, eventStorageSize-recordOverheadSize)
			hugeData := make([]byte, eventStorageSize-recordOverheadSize+1)
			payload := common.Payload{
				Data: hugeData,
			}
			payloads := []*common.Payload{&payload}
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
						Result: &common.Payloads{
							Payloads: payloads,
						},
					},
				},
			}

			response, err := rb.Build(&event)

			assertError(t, err, nil)
			assertResponsePayload(t, response, nil)
			assertResponseStatus(t, response, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
		})
	})

	t.Run("when status is FAILED", func(t *testing.T) {
		rb := newResponseBuilder(&request, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nilLogger, eventStorageSize-recordOverheadSize)
		t.Run("when result is nil returns errNoAttrs", func(t *testing.T) {
			event := historypb.HistoryEvent{
				Attributes: nil,
			}

			response, err := rb.Build(&event)

			assertError(t, err, errNoAttrs)
			assertResponseIsNil(t, response)
		})

		t.Run("when NewExecutionRunId not empty returns errFollow", func(t *testing.T) {
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						NewExecutionRunId: "some-run-id",
					},
				},
			}

			response, err := rb.Build(&event)

			assertError(t, err, errFollow("some-run-id"))
			assertResponseIsNil(t, response)
		})

		t.Run("when NewExecutionRunId empty return failure from event", func(t *testing.T) {
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						Failure: &failurepb.Failure{Message: "some failure"},
					},
				},
			}

			response, err := rb.Build(&event)

			assertError(t, err, nil)
			assertResponsePayload(t, response, nil)
			assertResponseStatus(t, response, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED)
		})
	})

	cancelledOrTerminated := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED}
	for _, status := range cancelledOrTerminated {
		t.Run(fmt.Sprintf("when status is %v return empty", status), func(t *testing.T) {
			rb := newResponseBuilder(&request, status, nilLogger, eventStorageSize-recordOverheadSize)
			event := historypb.HistoryEvent{}

			response, err := rb.Build(&event)

			assertError(t, err, nil)
			assertResponseResult(t, response, nil)
			assertResponseStatus(t, response, status)
		})
	}

	t.Run("when status CONTINUED_AS_NEW", func(t *testing.T) {
		rb := newResponseBuilder(
			&request,
			enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			nilLogger,
			eventStorageSize-recordOverheadSize,
		)

		t.Run("when scheduled workflow result is nil returns errNoAttr", func(t *testing.T) {
			event := historypb.HistoryEvent{Attributes: nil}

			response, err := rb.Build(&event)

			assertError(t, err, errNoAttrs)
			assertResponseIsNil(t, response)
		})

		t.Run("when result not nil will return errFollow", func(t *testing.T) {
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
					WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
						NewExecutionRunId: "some-run-id",
					},
				},
			}

			response, err := rb.Build(&event)

			assertError(t, err, errFollow("some-run-id"))
			assertResponseIsNil(t, response)
		})
	})

	t.Run("when status TIMED_OUT", func(t *testing.T) {
		rb := newResponseBuilder(
			&request, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nilLogger, eventStorageSize-recordOverheadSize)
		t.Run("when scheduled workflow result is nil returns errNoAttr", func(t *testing.T) {
			event := historypb.HistoryEvent{Attributes: nil}

			response, err := rb.Build(&event)

			assertError(t, err, errNoAttrs)
			assertResponseIsNil(t, response)
		})
		t.Run("when newExecutionRunId is not empty will return errFollow", func(t *testing.T) {
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
					WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
						NewExecutionRunId: "some-run-id",
					},
				},
			}

			response, err := rb.Build(&event)

			assertError(t, err, errFollow("some-run-id"))
			assertResponseIsNil(t, response)
		})
		t.Run("when newExecutionRunId is empty will return empty result", func(t *testing.T) {
			event := historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
					WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{},
				},
			}

			response, err := rb.Build(&event)
			assertError(t, err, nil)
			assertResponseResult(t, response, nil)
			assertResponseStatus(t, response, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT)

		})
	})
	t.Run("when status is UNSPECIFIED will return errUnknownFlow", func(t *testing.T) {
		rb := newResponseBuilder(
			&request, enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, nilLogger, eventStorageSize-recordOverheadSize)
		event := historypb.HistoryEvent{}

		response, err := rb.Build(&event)

		assertError(t, err, errUnkownWorkflowStatus)
		assertResponseIsNil(t, response)
	})
}

func assertResponseResult(t *testing.T, response *schedspb.WatchWorkflowResponse, expectedResult *common.Payloads) {
	t.Helper()
	require.Truef(
		t,
		proto.Equal(response.GetResult(), expectedResult),
		"incorrect response result expected %v, got %v",
		expectedResult,
		response.GetResult(),
	)
}

func assertResponseIsNil(t *testing.T, response *schedspb.WatchWorkflowResponse) {
	t.Helper()
	require.Nilf(t, response, "expected response to be nil, got %v", response)
}

func assertResponsePayload(t *testing.T, response *schedspb.WatchWorkflowResponse, expectedPayload []*common.Payload) {
	t.Helper()
	var actualPayloads = response.GetResult().GetPayloads()
	require.Equal(t, expectedPayload, actualPayloads, "incorrect response payload expected %v, got %v")
}

func assertResponseStatus(t *testing.T, response *schedspb.WatchWorkflowResponse, expectedStatus enumspb.WorkflowExecutionStatus) {
	t.Helper()
	require.Equal(
		t,
		expectedStatus,
		response.Status,
		"wrong response status expected %v, got %v",
		expectedStatus,
		response.Status,
	)
}

func assertError(t *testing.T, err error, expectedError error) {
	t.Helper()
	require.ErrorIsf(t, err, expectedError, "expected error %v, got %v", expectedError, err)
}
