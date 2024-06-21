// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAddChild(t *testing.T) {
	cases := []struct {
		name        string
		timeout     time.Duration
		assertTasks func(t *testing.T, tasks []hsm.Task)
	}{
		{
			name:    "with timeout",
			timeout: time.Hour,
			assertTasks: func(t *testing.T, tasks []hsm.Task) {
				require.Equal(t, 2, len(tasks))
				require.Equal(t, nexusoperations.TaskTypeInvocation, tasks[0].Type())
				require.Equal(t, nexusoperations.TaskTypeTimeout, tasks[1].Type())
			},
		},
		{
			name:    "without timeout",
			timeout: 0,
			assertTasks: func(t *testing.T, tasks []hsm.Task) {
				require.Equal(t, 1, len(tasks))
				require.Equal(t, nexusoperations.TaskTypeInvocation, tasks[0].Type())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := newRoot(t, &hsmtest.NodeBackend{})
			schedTime := timestamppb.Now()
			event := &historypb.HistoryEvent{
				EventTime: schedTime,
				Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
					NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
						EndpointId:             "endpoint-id",
						Endpoint:               "endpoint",
						Service:                "service",
						Operation:              "operation",
						RequestId:              "request-id",
						ScheduleToCloseTimeout: durationpb.New(tc.timeout),
					},
				},
			}
			child, err := nexusoperations.AddChild(root, "test-id", event, []byte("token"), false)
			require.NoError(t, err)
			oap := root.Outputs()
			require.Equal(t, 1, len(oap))
			require.Equal(t, 1, len(oap[0].Outputs))
			tc.assertTasks(t, oap[0].Outputs[0].Tasks)
			op, err := hsm.MachineData[nexusoperations.Operation](child)
			require.NoError(t, err)
			require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, op.State())
			require.Equal(t, "endpoint", op.Endpoint)
			require.Equal(t, "endpoint-id", op.EndpointId)
			require.Equal(t, "service", op.Service)
			require.Equal(t, "operation", op.Operation)
			require.Equal(t, schedTime, op.ScheduledTime)
			require.Equal(t, "request-id", op.RequestId)
			require.Equal(t, tc.timeout, op.ScheduleToCloseTimeout.AsDuration())
			require.Equal(t, int32(0), op.Attempt)
			require.Equal(t, []byte("token"), op.ScheduledEventToken)
		})
	}
}

func TestRegenerateTasks(t *testing.T) {
	cases := []struct {
		name        string
		timeout     time.Duration
		state       enumsspb.NexusOperationState
		assertTasks func(t *testing.T, tasks []hsm.Task)
	}{
		{
			name:    "scheduled | with timeout",
			timeout: time.Hour,
			state:   enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
			assertTasks: func(t *testing.T, tasks []hsm.Task) {
				require.Equal(t, 2, len(tasks))
				require.Equal(t, nexusoperations.TaskTypeInvocation, tasks[0].Type())
				require.Equal(t, tasks[0].(nexusoperations.InvocationTask).Destination, "endpoint-id")
				require.Equal(t, nexusoperations.TaskTypeTimeout, tasks[1].Type())
			},
		},
		{
			name:    "scheduled | without timeout",
			timeout: 0,
			state:   enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
			assertTasks: func(t *testing.T, tasks []hsm.Task) {
				require.Equal(t, 1, len(tasks))
				require.Equal(t, nexusoperations.TaskTypeInvocation, tasks[0].Type())
			},
		},
		{
			name:    "backing off | with timeout",
			timeout: time.Hour,
			state:   enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
			assertTasks: func(t *testing.T, tasks []hsm.Task) {
				require.Equal(t, 2, len(tasks))
				require.Equal(t, nexusoperations.TaskTypeBackoff, tasks[0].Type())
				require.Equal(t, nexusoperations.TaskTypeTimeout, tasks[1].Type())
			},
		},
		{
			name:    "backing off | without timeout",
			timeout: 0,
			state:   enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
			assertTasks: func(t *testing.T, tasks []hsm.Task) {
				require.Equal(t, 1, len(tasks))
				require.Equal(t, nexusoperations.TaskTypeBackoff, tasks[0].Type())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), tc.timeout))

			if tc.state == enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF {
				require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
					return nexusoperations.TransitionAttemptFailed.Apply(op, nexusoperations.EventAttemptFailed{
						Time:        time.Now(),
						Err:         fmt.Errorf("test"), // nolint:goerr113
						Node:        node,
						RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
					})
				}))
			}

			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			tasks, err := op.RegenerateTasks(node)
			require.NoError(t, err)
			tc.assertTasks(t, tasks)
		})
	}
}

func TestRetry(t *testing.T) {
	node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Minute))
	// Reset any outputs generated from nexusoperations.AddChild, we tested those already.
	node.ClearTransactionState()
	require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionAttemptFailed.Apply(op, nexusoperations.EventAttemptFailed{
			Time:        time.Now(),
			Err:         fmt.Errorf("test"), // nolint:goerr113
			Node:        node,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
		})
	}))
	oap := node.Outputs()
	require.Equal(t, 1, len(oap))
	require.Equal(t, 1, len(oap[0].Outputs))
	require.Equal(t, 1, len(oap[0].Outputs[0].Tasks))
	boTask := oap[0].Outputs[0].Tasks[0].(nexusoperations.BackoffTask) // nolint:revive
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
	require.Equal(t, int32(1), op.Attempt)
	require.Equal(t, op.NextAttemptScheduleTime.AsTime(), boTask.Deadline)
	require.NotNil(t, op.LastAttemptFailure)

	node.ClearTransactionState()
	require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionRescheduled.Apply(op, nexusoperations.EventRescheduled{
			Node: node,
		})
	}))
	oap = node.Outputs()
	require.Equal(t, 1, len(oap))
	require.Equal(t, 1, len(oap[0].Outputs))
	require.Equal(t, 1, len(oap[0].Outputs[0].Tasks))
	invocationTask := oap[0].Outputs[0].Tasks[0].(nexusoperations.InvocationTask) // nolint:revive
	require.Equal(t, "endpoint-id", invocationTask.Destination)
	op, err = hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, op.State())
	require.NotNil(t, op.LastAttemptFailure)

	// Also verify that the last attempt failure is cleared on success.
	require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionSucceeded.Apply(op, nexusoperations.EventSucceeded{
			Node:             node,
			Time:             time.Now(),
			CompletionSource: nexusoperations.CompletionSourceResponse,
		})
	}))
	op, err = hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	require.Nil(t, op.LastAttemptFailure)
}

func TestCompleteFromAttempt(t *testing.T) {
	cases := []struct {
		name        string
		transition  func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error)
		assertState func(t *testing.T, op nexusoperations.Operation)
	}{
		{
			name: "succeeded",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionSucceeded.Apply(op, nexusoperations.EventSucceeded{
					Node:             node,
					Time:             time.Now(),
					CompletionSource: nexusoperations.CompletionSourceResponse,
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, int32(1), op.Attempt)
				require.NotNil(t, op.LastAttemptCompleteTime)
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED, op.State())
			},
		},
		{
			name: "failed",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionFailed.Apply(op, nexusoperations.EventFailed{
					Node: node,
					Time: time.Now(),
					Attributes: &historypb.NexusOperationFailedEventAttributes{
						Failure: &failure.Failure{
							Message: "test",
						},
					},
					CompletionSource: nexusoperations.CompletionSourceResponse,
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, int32(1), op.Attempt)
				require.NotNil(t, op.LastAttemptCompleteTime)
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
			},
		},
		{
			name: "canceled",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionCanceled.Apply(op, nexusoperations.EventCanceled{
					Time:             time.Now(),
					Node:             node,
					CompletionSource: nexusoperations.CompletionSourceResponse,
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, int32(1), op.Attempt)
				require.NotNil(t, op.LastAttemptCompleteTime)
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_CANCELED, op.State())
			},
		},
		{
			name: "started",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
					Node: node,
					Time: time.Now(),
					Attributes: &historypb.NexusOperationStartedEventAttributes{
						OperationId: "op-id",
					},
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, int32(1), op.Attempt)
				require.NotNil(t, op.LastAttemptCompleteTime)
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
				require.Equal(t, "op-id", op.OperationId)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Minute))
			// Reset any outputs generated from nexusoperations.AddChild, we tested those already.
			node.ClearTransactionState()
			require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return tc.transition(node, op)
			}))
			oap := node.Outputs()
			require.Equal(t, 1, len(oap))
			require.Equal(t, 1, len(oap[0].Outputs))
			require.Equal(t, 0, len(oap[0].Outputs[0].Tasks))
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			tc.assertState(t, op)
		})
	}
}

func TestCompleteExternally(t *testing.T) {
	setups := []struct {
		name string
		fn   func(t *testing.T) *hsm.Node
	}{
		{
			name: "scheduled",
			fn: func(t *testing.T) *hsm.Node {
				return newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Minute))
			},
		},
		{
			name: "backing off",
			fn: func(t *testing.T) *hsm.Node {
				node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Minute))
				require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
					return nexusoperations.TransitionAttemptFailed.Apply(op, nexusoperations.EventAttemptFailed{
						Node:        node,
						Time:        time.Now(),
						Err:         fmt.Errorf("test"), // nolint:goerr113
						RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
					})
				}))
				return node
			},
		},
		{
			name: "started",
			fn: func(t *testing.T) *hsm.Node {
				node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Minute))
				require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
					return nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
						Node: node,
						Time: time.Now(),
						Attributes: &historypb.NexusOperationStartedEventAttributes{
							OperationId: "op-id",
						},
					})
				}))
				return node
			},
		},
	}
	cases := []struct {
		name        string
		transition  func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error)
		assertState func(t *testing.T, op nexusoperations.Operation)
	}{
		{
			name: "succeeded",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionSucceeded.Apply(op, nexusoperations.EventSucceeded{
					Node:             node,
					CompletionSource: nexusoperations.CompletionSourceCallback,
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED, op.State())
			},
		},
		{
			name: "failed",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionFailed.Apply(op, nexusoperations.EventFailed{
					Node:             node,
					CompletionSource: nexusoperations.CompletionSourceCallback,
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
			},
		},
		{
			name: "canceled",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionCanceled.Apply(op, nexusoperations.EventCanceled{
					Node:             node,
					CompletionSource: nexusoperations.CompletionSourceCallback,
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_CANCELED, op.State())
			},
		},
		{
			name: "timedout",
			transition: func(node *hsm.Node, op nexusoperations.Operation) (hsm.TransitionOutput, error) {
				return nexusoperations.TransitionTimedOut.Apply(op, nexusoperations.EventTimedOut{
					Node: node,
				})
			},
			assertState: func(t *testing.T, op nexusoperations.Operation) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT, op.State())
			},
		},
	}
	for _, setup := range setups {
		setup := setup
		for _, tc := range cases {
			t.Run(setup.name+"-"+tc.name, func(t *testing.T) {
				node := setup.fn(t)
				node.ClearTransactionState()
				require.NoError(t, hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
					return tc.transition(node, op)
				}))
				oap := node.Outputs()
				require.Equal(t, 1, len(oap))
				require.Equal(t, 1, len(oap[0].Outputs))
				require.Equal(t, 0, len(oap[0].Outputs[0].Tasks))
				op, err := hsm.MachineData[nexusoperations.Operation](node)
				require.NoError(t, err)
				tc.assertState(t, op)
			})
		}
	}
}

func TestCancel(t *testing.T) {
	t.Run("before started", func(t *testing.T) {
		backend := &hsmtest.NodeBackend{}
		root := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), time.Hour))
		op, err := hsm.MachineData[nexusoperations.Operation](root)
		require.NoError(t, err)
		_, err = op.Cancel(root, time.Now())
		require.NoError(t, err)
		require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_CANCELED, op.State())
		require.Equal(t, 1, len(backend.Events))
		attrs := backend.Events[0].GetNexusOperationCanceledEventAttributes()
		require.Equal(t, int64(1), attrs.ScheduledEventId)
		require.Equal(t, "operation canceled before started", attrs.Failure.Cause.Message)
		require.NotNil(t, attrs.Failure.Cause.GetCanceledFailureInfo())
	})

	t.Run("after started", func(t *testing.T) {
		backend := &hsmtest.NodeBackend{}
		root := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), time.Hour))
		op, err := hsm.MachineData[nexusoperations.Operation](root)
		require.NoError(t, err)
		_, err = nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
			Time: time.Now(),
			Attributes: &historypb.NexusOperationStartedEventAttributes{
				OperationId: "op-id",
			},
			Node: root,
		})
		require.NoError(t, err)
		_, err = op.Cancel(root, time.Now())
		require.NoError(t, err)
		require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
		node, err := root.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
		require.NoError(t, err)
		cancelation, err := hsm.MachineData[nexusoperations.Cancelation](node)
		require.NoError(t, err)
		require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, cancelation.State())
	})
}

func TestCancelationValidTransitions(t *testing.T) {
	// Setup
	root := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Hour))
	// We don't support cancel before started. Mark the operation as started.
	require.NoError(t, hsm.MachineTransition(root, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
			Time: time.Now(),
			Node: root,
			Attributes: &historypb.NexusOperationStartedEventAttributes{
				OperationId: "test-operation-id",
			},
		})
	}))
	require.NoError(t, hsm.MachineTransition(root, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return op.Cancel(root, time.Now())
	}))
	node, err := root.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
	require.NoError(t, err)
	cancelation, err := hsm.MachineData[nexusoperations.Cancelation](node)
	require.NoError(t, err)
	currentTime := time.Now().UTC()

	// AttemptFailed
	out, err := nexusoperations.TransitionCancelationAttemptFailed.Apply(cancelation, nexusoperations.EventCancelationAttemptFailed{
		Time:        currentTime,
		Err:         fmt.Errorf("test"),
		Node:        node,
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF, cancelation.State())
	require.Equal(t, int32(1), cancelation.Attempt)
	require.Equal(t, "test", cancelation.LastAttemptFailure.Message)
	require.False(t, cancelation.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, cancelation.LastAttemptCompleteTime.AsTime())
	dt := currentTime.Add(time.Second).Sub(cancelation.NextAttemptScheduleTime.AsTime())
	require.True(t, dt < time.Millisecond*200)

	// Assert backoff task is generated
	require.Equal(t, 1, len(out.Tasks))
	boTask := out.Tasks[0].(nexusoperations.CancelationBackoffTask) // nolint:revive
	require.Equal(t, cancelation.NextAttemptScheduleTime.AsTime(), boTask.Deadline)

	// Rescheduled
	out, err = nexusoperations.TransitionCancelationRescheduled.Apply(cancelation, nexusoperations.EventCancelationRescheduled{
		Node: node,
	})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, cancelation.State())
	require.Equal(t, int32(1), cancelation.Attempt)
	require.Equal(t, "test", cancelation.LastAttemptFailure.Message)
	// Remains unmodified
	require.Equal(t, currentTime, cancelation.LastAttemptCompleteTime.AsTime())
	require.Nil(t, cancelation.NextAttemptScheduleTime)

	// Assert cancelation task is generated
	require.Equal(t, 1, len(out.Tasks))
	cbTask := out.Tasks[0].(nexusoperations.CancelationTask) // nolint:revive
	require.Equal(t, "endpoint-id", cbTask.Destination)

	// Store the pre-succeeded state to test Failed later
	dup := nexusoperations.Cancelation{common.CloneProto(cancelation.NexusOperationCancellationInfo)}

	// Succeeded
	currentTime = currentTime.Add(time.Second)
	out, err = nexusoperations.TransitionCancelationSucceeded.Apply(cancelation, nexusoperations.EventCancelationSucceeded{
		Time: currentTime,
		Node: node,
	})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED, cancelation.State())
	require.Equal(t, int32(2), cancelation.Attempt)
	require.Nil(t, cancelation.LastAttemptFailure)
	require.Equal(t, currentTime, cancelation.LastAttemptCompleteTime.AsTime())
	require.Nil(t, cancelation.NextAttemptScheduleTime)

	// Assert no additional tasks are generated
	require.Equal(t, 0, len(out.Tasks))

	// Reset back to scheduled
	cancelation = dup
	// Increment the time to ensure it's updated in the transition
	currentTime = currentTime.Add(time.Second)

	// Failed
	out, err = nexusoperations.TransitionCancelationFailed.Apply(cancelation, nexusoperations.EventCancelationFailed{
		Time: currentTime,
		Err:  fmt.Errorf("failed"),
		Node: node,
	})
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, cancelation.State())
	require.Equal(t, int32(2), cancelation.Attempt)
	require.Equal(t, "failed", cancelation.LastAttemptFailure.Message)
	require.True(t, cancelation.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, currentTime, cancelation.LastAttemptCompleteTime.AsTime())
	require.Nil(t, cancelation.NextAttemptScheduleTime)

	// Assert no additional tasks are generated
	require.Equal(t, 0, len(out.Tasks))
}

func TestOperationCompareState(t *testing.T) {
	reg := hsm.NewRegistry()
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	def, ok := reg.Machine(nexusoperations.OperationMachineType)
	require.True(t, ok)

	cases := []struct {
		name                 string
		s1, s2               enumsspb.NexusOperationState
		attempts1, attempts2 int32
		sign                 int
		expectError          bool
	}{
		{
			name:        "succeeded not comparable to failed",
			s1:          enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED,
			s2:          enumsspb.NEXUS_OPERATION_STATE_FAILED,
			expectError: true,
		},
		{
			name: "started < succeeded",
			s1:   enumsspb.NEXUS_OPERATION_STATE_STARTED,
			s2:   enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED,
			sign: 1,
		},
		{
			name: "backing off < failed",
			s1:   enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
			s2:   enumsspb.NEXUS_OPERATION_STATE_FAILED,
			sign: 1,
		},
		{
			name: "backing off > scheduled",
			s1:   enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
			s2:   enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
			sign: -1,
		},
		{
			name:      "backing off < scheduled with greater attempt",
			s1:        enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
			s2:        enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
			attempts2: 1,
			sign:      1,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s1 := nexusoperations.Operation{
				NexusOperationInfo: &persistencespb.NexusOperationInfo{
					State:   tc.s1,
					Attempt: tc.attempts1,
				},
			}
			s2 := nexusoperations.Operation{
				NexusOperationInfo: &persistencespb.NexusOperationInfo{
					State:   tc.s2,
					Attempt: tc.attempts2,
				},
			}
			res, err := def.CompareState(s1, s2)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.sign == 0 {
				require.Equal(t, 0, res)
			} else if tc.sign > 0 {
				require.Greater(t, res, 0)
			} else {
				require.Greater(t, 0, res)
			}
		})
	}
}

func TestCancelationCompareState(t *testing.T) {
	reg := hsm.NewRegistry()
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	def, ok := reg.Machine(nexusoperations.CancelationMachineType)
	require.True(t, ok)

	cases := []struct {
		name                 string
		s1, s2               enumspb.NexusOperationCancellationState
		attempts1, attempts2 int32
		sign                 int
		expectError          bool
	}{
		{
			name:        "succeeded not comparable to failed",
			s1:          enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED,
			s2:          enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED,
			expectError: true,
		},
		{
			name: "backing off < failed",
			s1:   enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF,
			s2:   enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED,
			sign: 1,
		},
		{
			name: "backing off > scheduled",
			s1:   enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF,
			s2:   enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED,
			sign: -1,
		},
		{
			name:      "backing off < scheduled with greater attempt",
			s1:        enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF,
			s2:        enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED,
			attempts2: 1,
			sign:      1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s1 := nexusoperations.Cancelation{
				NexusOperationCancellationInfo: &persistencespb.NexusOperationCancellationInfo{
					State:   tc.s1,
					Attempt: tc.attempts1,
				},
			}
			s2 := nexusoperations.Cancelation{
				NexusOperationCancellationInfo: &persistencespb.NexusOperationCancellationInfo{
					State:   tc.s2,
					Attempt: tc.attempts2,
				},
			}
			res, err := def.CompareState(s1, s2)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.sign == 0 {
				require.Equal(t, 0, res)
			} else if tc.sign > 0 {
				require.Greater(t, res, 0)
			} else {
				require.Greater(t, 0, res)
			}
		})
	}
}
