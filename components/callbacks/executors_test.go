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

package callbacks_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/workflow"
)

type fakeEnv struct {
	node *hsm.Node
}

func (s fakeEnv) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) error {
	return accessor(s.node)
}

func (fakeEnv) Now() time.Time {
	return time.Now()
}

var _ hsm.Environment = fakeEnv{}

type mutableState struct {
	completion nexus.OperationCompletion
}

func (ms mutableState) GetNexusCompletion(ctx context.Context) (nexus.OperationCompletion, error) {
	return ms.completion, nil
}

func TestProcessInvocationTask_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		caller                callbacks.HTTPCaller
		destinationDown       bool
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, callbacks.Callback)
	}{
		{
			name: "success",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			destinationDown:       false,
			expectedMetricOutcome: "status:200",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, cb.State())
			},
		},
		{
			name: "failed",
			caller: func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("fake failure")
			},
			destinationDown:       true,
			expectedMetricOutcome: "unknown-error",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_BACKING_OFF, cb.State())
			},
		},
		{
			name: "retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			},
			destinationDown:       true,
			expectedMetricOutcome: "status:500",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_BACKING_OFF, cb.State())
			},
		},
		{
			name: "non-retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Body: http.NoBody}, nil
			},
			destinationDown:       false,
			expectedMetricOutcome: "status:400",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, cb.State())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			namespaceRegistryMock := namespace.NewMockRegistry(ctrl)
			namespaceRegistryMock.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(
				namespace.FromPersistentState(&persistence.GetNamespaceResponse{
					Namespace: &persistencespb.NamespaceDetail{
						Info: &persistencespb.NamespaceInfo{
							Id:   "namespace-id",
							Name: "namespace-name",
						},
						Config: &persistencespb.NamespaceConfig{},
					},
				}),
				nil,
			)
			metricsHandler := metrics.NewMockHandler(ctrl)
			counter := metrics.NewMockCounterIface(ctrl)
			timer := metrics.NewMockTimerIface(ctrl)
			metricsHandler.EXPECT().Counter(callbacks.RequestCounter.Name()).Return(counter)
			counter.EXPECT().Record(int64(1),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.NexusOutcomeTag(tc.expectedMetricOutcome))
			metricsHandler.EXPECT().Timer(callbacks.RequestLatencyHistogram.Name()).Return(timer)
			timer.EXPECT().Record(gomock.Any(),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.NexusOutcomeTag(tc.expectedMetricOutcome))

			root := newRoot(t)
			cb := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{
								Url: "http://localhost",
							},
						},
					},
					State: enumsspb.CALLBACK_STATE_SCHEDULED,
				},
			}
			coll := callbacks.MachineCollection(root)
			node, err := coll.Add("ID", cb)
			require.NoError(t, err)
			env := fakeEnv{node}

			key := definition.NewWorkflowKey("namespace-id", "", "")
			reg := hsm.NewRegistry()
			require.NoError(t, callbacks.RegisterExecutor(
				reg,
				callbacks.TaskExecutorOptions{
					NamespaceRegistry: namespaceRegistryMock,
					MetricsHandler:    metricsHandler,
					CallerProvider: func(nid queues.NamespaceIDAndDestination) callbacks.HTTPCaller {
						return tc.caller
					},
					Logger: log.NewNoopLogger(),
					Config: &callbacks.Config{
						RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
						RetryPolicy: func() backoff.RetryPolicy {
							return backoff.NewExponentialRetryPolicy(time.Second)
						},
					},
				},
			))

			err = reg.ExecuteImmediateTask(
				context.Background(),
				env,
				hsm.Ref{
					WorkflowKey: key,
					StateMachineRef: &persistencespb.StateMachineRef{
						Path: []*persistencespb.StateMachineKey{
							{
								Type: callbacks.StateMachineType,
								Id:   "ID",
							},
						},
					},
				},
				callbacks.InvocationTask{Destination: "http://localhost"},
			)

			if tc.destinationDown {
				var destinationDownErr *queues.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
			} else {
				require.NoError(t, err)
			}

			cb, err = coll.Data("ID")
			require.NoError(t, err)
			tc.assertOutcome(t, cb)
		})
	}
}

func TestProcessBackoffTask(t *testing.T) {
	root := newRoot(t)
	cb := callbacks.Callback{
		CallbackInfo: &persistencespb.CallbackInfo{
			Callback: &persistencespb.Callback{
				Variant: &persistencespb.Callback_Nexus_{
					Nexus: &persistencespb.Callback_Nexus{
						Url: "http://localhost",
					},
				},
			},
			State: enumsspb.CALLBACK_STATE_BACKING_OFF,
		},
	}
	coll := callbacks.MachineCollection(root)
	node, err := coll.Add("ID", cb)
	require.NoError(t, err)
	env := fakeEnv{node}

	reg := hsm.NewRegistry()
	require.NoError(t, callbacks.RegisterExecutor(
		reg,
		callbacks.TaskExecutorOptions{
			CallerProvider: func(nid queues.NamespaceIDAndDestination) callbacks.HTTPCaller {
				return nil
			},
			Logger: log.NewNoopLogger(),
			Config: &callbacks.Config{
				RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
				RetryPolicy: func() backoff.RetryPolicy {
					return backoff.NewExponentialRetryPolicy(time.Second)
				},
			},
		},
	))

	err = reg.ExecuteTimerTask(
		env,
		node,
		callbacks.BackoffTask{},
	)
	require.NoError(t, err)

	cb, err = coll.Data("ID")
	require.NoError(t, err)
	require.Equal(t, enumsspb.CALLBACK_STATE_SCHEDULED, cb.State())
}

func newMutableState(t *testing.T) mutableState {
	completion, err := nexus.NewOperationCompletionSuccessful(nil, nexus.OperationCompletionSuccesfulOptions{})
	require.NoError(t, err)
	return mutableState{
		completion: completion,
	}
}

func newRoot(t *testing.T) *hsm.Node {
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	mutableState := newMutableState(t)

	root, err := hsm.NewRoot(reg, workflow.StateMachineType, mutableState, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	return root
}
