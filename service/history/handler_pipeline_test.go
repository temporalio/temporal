package history

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

func TestForceDeleteWaitsForCommittedAccessWithoutLoadingState(t *testing.T) {
	t.Parallel()
	for _, pinnedRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "resolve current run", true: "specified run"}[pinnedRun], func(t *testing.T) {
			t.Parallel()
			tv := testvars.New(t)
			tv = tv.WithRunID(tv.Any().RunID())
			ctrl := gomock.NewController(t)
			controller := shard.NewMockController(ctrl)
			shardContext := historyi.NewMockShardContext(ctrl)
			cache := wcache.NewMockCache(ctrl)
			workflowContext := historyi.NewMockWorkflowContext(ctrl)
			executions := persistence.NewMockExecutionManager(ctrl)
			visibility := manager.NewMockVisibilityManager(ctrl)
			acquiring, allowAcquire, reading := make(chan struct{}, 1), make(chan struct{}), make(chan struct{}, 1)
			released := make(chan error, 1)
			var once sync.Once
			release := func(err error) error { once.Do(func() { released <- err }); return err }
			controller.EXPECT().GetShardByNamespaceWorkflow(tv.NamespaceID(), tv.WorkflowID()).Return(shardContext, nil)
			cache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), shardContext, tv.NamespaceID(), protomock.Eq(&commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: tv.RunID()}), locks.PriorityHigh).
				DoAndReturn(func(_ context.Context, _ historyi.ShardContext, _ namespace.ID, _ *commonpb.WorkflowExecution, _ locks.Priority) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
					acquiring <- struct{}{}
					<-allowAcquire
					return workflowContext, release, nil
				})
			if !pinnedRun {
				executions.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetCurrentExecutionResponse{RunID: tv.RunID()}, nil)
			}
			executions.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
				reading <- struct{}{}
				return nil, serviceerror.NewDataLoss("corrupt mutable state")
			})
			executions.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
			executions.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
			visibility.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
			workflowContext.EXPECT().Clear()
			h := &Handler{config: &configs.Config{NumberOfShards: 1, WorkflowPersistencePipeline: true}, controller: controller, workflowCache: cache,
				persistenceExecutionManager: executions, persistenceVisibilityManager: visibility, logger: log.NewNoopLogger()}
			execution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()}
			if pinnedRun {
				execution.RunId = tv.RunID()
			}
			request := &historyservice.ForceDeleteWorkflowExecutionRequest{NamespaceId: tv.NamespaceID().String(), ArchetypeId: chasm.WorkflowArchetypeID,
				Request: &adminservice.DeleteWorkflowExecutionRequest{Execution: execution}}
			type result struct {
				response *historyservice.ForceDeleteWorkflowExecutionResponse
				err      error
			}
			done := make(chan result, 1)
			go func() {
				response, err := h.ForceDeleteWorkflowExecution(t.Context(), request)
				done <- result{response, err}
			}()
			await.Rcv(t, acquiring)
			select {
			case <-reading:
				t.Fatal("force-delete read mutable state before the committed barrier")
			default:
			}
			close(allowAcquire)
			got := await.Rcv(t, done)
			require.NoError(t, got.err)
			require.Len(t, got.response.Response.Warnings, 1)
			require.NoError(t, await.Rcv(t, released))
			await.Rcv(t, reading)
			if !pinnedRun {
				require.Empty(t, request.Request.Execution.RunId)
			}
		})
	}
}
