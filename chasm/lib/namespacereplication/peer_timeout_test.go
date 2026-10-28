package namespacereplication

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestPeerApplyTimeoutPreservesOutcomePersistenceContext(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		shadow       bool
		cancelParent bool
	}{
		{name: "authoritative timeout"},
		{name: "shadow timeout", shadow: true},
		{name: "task cancellation", cancelParent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				env := newNsreplTestEnv(t)
				mutation := env.mutationUpdate("cellB")
				mutation.Shadow = tc.shadow
				ref := env.start(mutation, func(c *NamespaceMutationComponent) {
					c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
					if tc.shadow {
						c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW
					}
				})

				engine := chasm.NewMockEngine(gomock.NewController(t))
				engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(env.engine.ReadComponent)
				taskCtx, cancel := context.WithTimeout(chasm.NewEngineContext(t.Context(), engine), 10*time.Second)
				defer cancel()
				engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption) ([]byte, error) {
						require.Same(t, taskCtx, ctx, "persist with the task context, not the expired RPC context or a detached context")
						if err := ctx.Err(); err != nil {
							return nil, err
						}
						return env.engine.UpdateComponent(ctx, ref, update, opts...)
					})
				env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
				env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, _ *adminservice.ApplyNamespaceMutationRequest, _ ...grpc.CallOption) (*adminservice.ApplyNamespaceMutationResponse, error) {
						taskDeadline, _ := taskCtx.Deadline()
						callDeadline, ok := ctx.Deadline()
						require.True(t, ok)
						require.Equal(t, taskDeadline.Add(-time.Second), callDeadline)
						if tc.cancelParent {
							cancel()
						}
						<-ctx.Done()
						if !tc.cancelParent {
							require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
							require.NoError(t, taskCtx.Err(), "the outcome persistence budget must remain available")
						}
						return nil, ctx.Err()
					})

				err := env.peerHandler.Execute(taskCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"})
				var destinationDown *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDown)
				component := env.read(ref)
				peer := component.GetPeerApply()["cellB"]
				tasks, err := env.engine.Tasks(ref)
				require.NoError(t, err)
				if tc.cancelParent {
					// A canceled task still falls back to the history queue's retry path.
					require.ErrorIs(t, destinationDown.Unwrap(), context.Canceled)
					require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, peer.GetOutcome())
					require.Zero(t, peer.GetAttemptCount())
					require.Empty(t, tasks[historytasks.CategoryTimer])
					return
				}
				require.NoError(t, destinationDown.Unwrap(), "acknowledge the outbound task only after saving the CHASM retry")
				require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_RUNNING, component.GetStatus())
				require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE, peer.GetOutcome())
				require.Equal(t, int32(1), peer.GetAttemptCount())
				require.Len(t, tasks[historytasks.CategoryTimer], 1)
				require.Empty(t, tasks[historytasks.CategoryOutbound])
			})
		})
	}
}
