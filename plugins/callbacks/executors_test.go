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

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/plugins/callbacks"
	"go.temporal.io/server/service/history/hsm"
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
		name          string
		caller        callbacks.HTTPCaller
		assertOutcome func(*testing.T, callbacks.Callback)
	}{
		{
			name: "success",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, cb.PublicInfo.State)
			},
		},
		{
			name: "failed",
			caller: func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("fake failure")
			},
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumspb.CALLBACK_STATE_BACKING_OFF, cb.PublicInfo.State)
			},
		},
		{
			name: "retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			},
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumspb.CALLBACK_STATE_BACKING_OFF, cb.PublicInfo.State)
			},
		},
		{
			name: "non-retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Body: http.NoBody}, nil
			},
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumspb.CALLBACK_STATE_FAILED, cb.PublicInfo.State)
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			root := newRoot(t)
			cb := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					PublicInfo: &workflowpb.CallbackInfo{
						Callback: &common.Callback{
							Variant: &common.Callback_Nexus_{
								Nexus: &common.Callback_Nexus{
									Url: "http://localhost",
								},
							},
						},
						State: enumspb.CALLBACK_STATE_SCHEDULED,
					},
				},
			}
			coll := callbacks.MachineCollection(root)
			node, err := coll.Add("ID", cb)
			require.NoError(t, err)
			env := fakeEnv{node}

			key := definition.NewWorkflowKey("namespace-id", "", "")
			reg := hsm.NewRegistry()
			require.NoError(t, callbacks.RegisterExecutor(reg, callbacks.ActiveExecutorOptions{
				CallerProvider: func(nid queues.NamespaceIDAndDestination) callbacks.HTTPCaller {
					return tc.caller
				},
			}, &callbacks.Config{InvocationTaskTimeout: func() time.Duration { return time.Second }}))

			err = hsm.Execute(context.Background(), reg, env,
				hsm.Ref{
					WorkflowKey: key,
					StateMachineRef: &persistencespb.StateMachineRef{
						Path: []*persistencespb.StateMachineKey{
							{
								Type: callbacks.StateMachineType.ID,
								Id:   "ID",
							},
						},
					},
				},
				callbacks.InvocationTask{Destination: "dont-care"},
			)
			require.NoError(t, err)

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
			PublicInfo: &workflowpb.CallbackInfo{
				Callback: &common.Callback{
					Variant: &common.Callback_Nexus_{
						Nexus: &common.Callback_Nexus{
							Url: "http://localhost",
						},
					},
				},
				State: enumspb.CALLBACK_STATE_BACKING_OFF,
			},
		},
	}
	coll := callbacks.MachineCollection(root)
	node, err := coll.Add("ID", cb)
	require.NoError(t, err)
	env := fakeEnv{node}

	reg := hsm.NewRegistry()
	require.NoError(t, callbacks.RegisterExecutor(reg, callbacks.ActiveExecutorOptions{
		CallerProvider: func(nid queues.NamespaceIDAndDestination) callbacks.HTTPCaller {
			return nil
		},
	}, &callbacks.Config{InvocationTaskTimeout: func() time.Duration { return time.Second }}))

	err = hsm.Execute(context.Background(), reg, env,
		hsm.Ref{
			StateMachineRef: &persistencespb.StateMachineRef{
				Path: []*persistencespb.StateMachineKey{
					{
						Type: callbacks.StateMachineType.ID,
						Id:   "ID",
					},
				},
			},
		},
		callbacks.BackoffTask{},
	)
	require.NoError(t, err)

	cb, err = coll.Data("ID")
	require.NoError(t, err)
	require.Equal(t, enumspb.CALLBACK_STATE_SCHEDULED, cb.PublicInfo.State)
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

	root, err := hsm.NewRoot(reg, workflow.StateMachineType.ID, mutableState, make(map[int32]*persistencespb.StateMachineMap))
	require.NoError(t, err)
	return root
}
