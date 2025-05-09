package pollupdate_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/pollupdate"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/anypb"
)

type (
	mockWFConsistencyChecker struct {
		api.WorkflowConsistencyChecker
		GetWorkflowContextFunc func(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			workflowKey definition.WorkflowKey,
			lockPriority locks.Priority,
		) (api.WorkflowLease, error)
	}

	mockWorkflowLeaseCtx struct {
		api.WorkflowLease
		GetContextFn   func() historyi.WorkflowContext
		GetReleaseFnFn func() historyi.ReleaseWorkflowContextFunc
	}

	mockReg struct {
		update.Registry
		FindFunc func(context.Context, string) *update.Update
	}

	mockUpdateEventStore struct {
		update.EventStore
	}
)

func (mockUpdateEventStore) OnAfterCommit(f func(context.Context))   { f(context.TODO()) }
func (mockUpdateEventStore) OnAfterRollback(f func(context.Context)) {}
func (mockUpdateEventStore) CanAddEvent() bool                       { return true }

func (m mockWFConsistencyChecker) GetWorkflowLease(
	ctx context.Context,
	clock *clockspb.VectorClock,
	wfKey definition.WorkflowKey,
	prio locks.Priority,
) (api.WorkflowLease, error) {
	return m.GetWorkflowContextFunc(ctx, clock, wfKey, prio)
}

func (m mockWorkflowLeaseCtx) GetReleaseFn() historyi.ReleaseWorkflowContextFunc {
	return m.GetReleaseFnFn()
}

func (m mockWorkflowLeaseCtx) GetContext() historyi.WorkflowContext {
	return m.GetContextFn()
}

func (m mockReg) Find(ctx context.Context, updateID string) *update.Update {
	return m.FindFunc(ctx, updateID)
}

func TestPollOutcome(t *testing.T) {
	namespaceId := t.Name() + "-namespace-id"
	workflowId := t.Name() + "-workflow-id"
	runId := t.Name() + "-run-id"
	updateID := t.Name() + "-update-id"
	reg := &mockReg{}

	mockController := gomock.NewController(t)

	wfCtx := historyi.NewMockWorkflowContext(mockController)
	wfCtx.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{NamespaceID: namespaceId, WorkflowID: workflowId, RunID: runId}).AnyTimes()
	wfCtx.EXPECT().UpdateRegistry(gomock.Any()).Return(reg).AnyTimes()

	apiCtx := mockWorkflowLeaseCtx{
		GetReleaseFnFn: func() historyi.ReleaseWorkflowContextFunc { return func(error) {} },
		GetContextFn: func() historyi.WorkflowContext {
			return wfCtx
		},
	}
	wfcc := mockWFConsistencyChecker{
		GetWorkflowContextFunc: func(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			workflowKey definition.WorkflowKey,
			lockPriority locks.Priority,
		) (api.WorkflowLease, error) {
			return apiCtx, nil
		},
	}

	serverImposedTimeout := 10 * time.Millisecond
	mockNamespaceRegistry := namespace.NewMockRegistry(mockController)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	shardContext := historyi.NewMockShardContext(mockController)
	mockConfig := tests.NewDynamicConfig()
	mockConfig.LongPollExpirationInterval = func(_ string) time.Duration { return serverImposedTimeout }
	shardContext.EXPECT().GetConfig().Return(mockConfig).AnyTimes()
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockNamespaceRegistry).AnyTimes()

	req := historyservice.PollWorkflowExecutionUpdateRequest{
		Request: &workflowservice.PollWorkflowExecutionUpdateRequest{
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				UpdateId: updateID,
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
			},
		},
	}

	t.Run("update not found", func(t *testing.T) {
		reg.FindFunc = func(ctx context.Context, updateID string) *update.Update {
			return nil
		}
		_, err := pollupdate.Invoke(context.TODO(), &req, shardContext, wfcc)
		var notfound *serviceerror.NotFound
		require.ErrorAs(t, err, &notfound)
	})
	t.Run("context deadline expiry before server-imposed deadline expiry", func(t *testing.T) {
		reg.FindFunc = func(ctx context.Context, updateID string) *update.Update {
			return update.New(updateID)
		}
		ctx, cncl := context.WithTimeout(context.Background(), serverImposedTimeout/2)
		defer cncl()
		_, err := pollupdate.Invoke(ctx, &req, shardContext, wfcc)
		require.Error(t, err)
	})
	t.Run("context deadline expiry after server-imposed deadline expiry", func(t *testing.T) {
		reg.FindFunc = func(ctx context.Context, updateID string) *update.Update {
			return update.New(updateID)
		}
		ctx, cncl := context.WithTimeout(context.Background(), serverImposedTimeout*2)
		defer cncl()
		resp, err := pollupdate.Invoke(ctx, &req, shardContext, wfcc)
		require.NoError(t, err)
		require.Nil(t, resp.GetResponse().Outcome)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, resp.Response.GetStage())
	})
	t.Run("non-blocking poll with omitted/unspecified wait policy", func(t *testing.T) {
		for _, req := range []*historyservice.PollWorkflowExecutionUpdateRequest{{
			Request: &workflowservice.PollWorkflowExecutionUpdateRequest{
				UpdateRef: req.Request.UpdateRef,
			},
		}, {
			Request: &workflowservice.PollWorkflowExecutionUpdateRequest{
				UpdateRef: &updatepb.UpdateRef{
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: workflowId,
						RunId:      runId,
					},
					UpdateId: updateID,
				},
				WaitPolicy: &updatepb.WaitPolicy{
					LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
				},
			},
		}} {
			reg.FindFunc = func(ctx context.Context, updateID string) *update.Update {
				return update.New(updateID)
			}
			resp, err := pollupdate.Invoke(context.Background(), req, shardContext, wfcc)
			require.NoError(t, err)
			require.True(t, len(resp.GetResponse().UpdateRef.GetWorkflowExecution().RunId) > 0)
			require.Nil(t, resp.GetResponse().Outcome)
			require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, resp.Response.GetStage())
		}
	})
	t.Run("get an outcome", func(t *testing.T) {
		upd := update.New(updateID)
		reg.FindFunc = func(ctx context.Context, updateID string) *update.Update {
			return upd
		}
		reqMsg := updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not_empty"},
		}
		fail := failurepb.Failure{Message: "intentional failure in " + t.Name()}
		wantOutcome := updatepb.Outcome{Value: &updatepb.Outcome_Failure{Failure: &fail}}

		rejBody := &updatepb.Rejection{
			RejectedRequestMessageId: updateID + "/request",
			RejectedRequest:          &reqMsg,
			Failure:                  &fail,
		}
		var rejBodyAny anypb.Any
		require.NoError(t, rejBodyAny.MarshalFrom(rejBody))
		rejMsg := protocolpb.Message{Body: &rejBodyAny}

		errCh := make(chan error, 1)
		respCh := make(chan *historyservice.PollWorkflowExecutionUpdateResponse, 1)
		go func() {
			resp, err := pollupdate.Invoke(context.TODO(), &req, shardContext, wfcc)
			errCh <- err
			respCh <- resp
		}()

		evStore := mockUpdateEventStore{}
		require.NoError(t, upd.Admit(&reqMsg, evStore))
		upd.Send(false, &protocolpb.Message_EventId{EventId: 2208})
		require.NoError(t, upd.OnProtocolMessage(&rejMsg, evStore))

		require.NoError(t, <-errCh)
		resp := <-respCh
		protorequire.ProtoEqual(t, &wantOutcome, resp.GetResponse().Outcome)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, resp.Response.GetStage())
	})
}
