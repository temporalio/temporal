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

package pollupdate_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/pollupdate"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
)

type (
	mockWFConsistencyChecker struct {
		api.WorkflowConsistencyChecker
		GetWorkflowContextFunc func(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			consistencyPredicate api.MutableStateConsistencyPredicate,
			workflowKey definition.WorkflowKey,
			lockPriority workflow.LockPriority,
		) (api.WorkflowContext, error)
	}

	mockAPICtx struct {
		api.WorkflowContext
		GetUpdateRegistryFunc func(context.Context) update.Registry
		GetReleaseFnFunc      func() wcache.ReleaseCacheFunc
	}

	mockReg struct {
		update.Registry
		FindFunc func(context.Context, string) (*update.Update, bool)
	}

	mockUpdateEventStore struct {
		update.EventStore
	}
)

func (mockUpdateEventStore) OnAfterCommit(f func(context.Context))   { f(context.TODO()) }
func (mockUpdateEventStore) OnAfterRollback(f func(context.Context)) {}

func (m mockWFConsistencyChecker) GetWorkflowContext(
	ctx context.Context,
	clock *clockspb.VectorClock,
	pred api.MutableStateConsistencyPredicate,
	wfKey definition.WorkflowKey,
	prio workflow.LockPriority,
) (api.WorkflowContext, error) {
	return m.GetWorkflowContextFunc(ctx, clock, pred, wfKey, prio)
}

func (m mockAPICtx) GetReleaseFn() wcache.ReleaseCacheFunc {
	return m.GetReleaseFnFunc()
}

func (m mockAPICtx) GetUpdateRegistry(ctx context.Context) update.Registry {
	return m.GetUpdateRegistryFunc(ctx)
}

func (m mockReg) Find(ctx context.Context, updateID string) (*update.Update, bool) {
	return m.FindFunc(ctx, updateID)
}

func TestPollOutcome(t *testing.T) {
	reg := mockReg{}
	apiCtx := mockAPICtx{
		GetReleaseFnFunc: func() wcache.ReleaseCacheFunc { return func(error) {} },
		GetUpdateRegistryFunc: func(context.Context) update.Registry {
			return reg
		},
	}
	wfcc := mockWFConsistencyChecker{
		GetWorkflowContextFunc: func(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			consistencyPredicate api.MutableStateConsistencyPredicate,
			workflowKey definition.WorkflowKey,
			lockPriority workflow.LockPriority,
		) (api.WorkflowContext, error) {
			return apiCtx, nil
		},
	}

	updateID := t.Name() + "-update-id"
	req := historyservice.PollWorkflowExecutionUpdateRequest{
		Request: &workflowservice.PollWorkflowExecutionUpdateRequest{
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: t.Name() + "-workflow-id",
					RunId:      t.Name() + "-run-id",
				},
				UpdateId: updateID,
			},
		},
	}

	t.Run("update not found", func(t *testing.T) {
		reg.FindFunc = func(ctx context.Context, updateID string) (*update.Update, bool) {
			return nil, false
		}
		_, err := pollupdate.Invoke(context.TODO(), &req, wfcc)
		var notfound *serviceerror.NotFound
		require.ErrorAs(t, err, &notfound)
	})
	t.Run("future timeout", func(t *testing.T) {
		reg.FindFunc = func(ctx context.Context, updateID string) (*update.Update, bool) {
			return update.New(updateID), true
		}
		ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cncl()
		_, err := pollupdate.Invoke(ctx, &req, wfcc)
		require.Error(t, err)
	})
	t.Run("get an outcome", func(t *testing.T) {
		upd := update.New(updateID)
		reg.FindFunc = func(ctx context.Context, updateID string) (*update.Update, bool) {
			return upd, true
		}
		reqMsg := updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not_empty"},
		}
		fail := failurepb.Failure{Message: "intentional failure in " + t.Name()}
		wantOutcome := updatepb.Outcome{Value: &updatepb.Outcome_Failure{Failure: &fail}}
		rejMsg := updatepb.Rejection{
			RejectedRequestMessageId: updateID + "/request",
			RejectedRequest:          &reqMsg,
			Failure:                  &fail,
		}

		errCh := make(chan error, 1)
		respCh := make(chan *historyservice.PollWorkflowExecutionUpdateResponse, 1)
		go func() {
			resp, err := pollupdate.Invoke(context.TODO(), &req, wfcc)
			errCh <- err
			respCh <- resp
		}()

		evStore := mockUpdateEventStore{}
		require.NoError(t, upd.OnMessage(context.TODO(), &reqMsg, evStore))
		require.NoError(t, upd.OnMessage(context.TODO(), &rejMsg, evStore))

		require.NoError(t, <-errCh)
		resp := <-respCh
		require.Equal(t, &wantOutcome, resp.GetResponse().Outcome)
	})
}
