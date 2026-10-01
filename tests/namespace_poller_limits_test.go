package tests

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

func (s *NamespaceInterceptorTestSuite) TestInternalPerNamespacePollerLimits() {
	for _, tc := range []struct {
		name          string
		taskQueueType enumspb.TaskQueueType
		poll          func(context.Context, *testcore.TestEnv, *taskqueuepb.TaskQueue, string) error
	}{
		{
			name:          "workflow",
			taskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			poll: func(ctx context.Context, env *testcore.TestEnv, tq *taskqueuepb.TaskQueue, identity string) error {
				_, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: env.Namespace().String(),
					TaskQueue: tq,
					Identity:  identity,
				})
				return err
			},
		},
		{
			name:          "activity",
			taskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			poll: func(ctx context.Context, env *testcore.TestEnv, tq *taskqueuepb.TaskQueue, identity string) error {
				_, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
					Namespace: env.Namespace().String(),
					TaskQueue: tq,
					Identity:  identity,
				})
				return err
			},
		},
		{
			name:          "nexus",
			taskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS,
			poll: func(ctx context.Context, env *testcore.TestEnv, tq *taskqueuepb.TaskQueue, identity string) error {
				_, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
					Namespace: env.Namespace().String(),
					TaskQueue: tq,
					Identity:  identity,
				})
				return err
			},
		},
	} {
		s.Run(tc.name, func(s *NamespaceInterceptorTestSuite) {
			t := s.T()
			env := testcore.NewEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentLongRunningRequestsPerInstance, 1),
				testcore.WithDynamicConfig(dynamicconfig.FrontendInternalPerNSMaxConcurrentLongRunningRequestsPerInstance, 1),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
			)
			customerQueue := &taskqueuepb.TaskQueue{Name: "customer-" + uuid.NewString()}
			internalQueue := &taskqueuepb.TaskQueue{Name: "temporal-sys-per-ns-" + uuid.NewString()}

			startPoll := func(tq *taskqueuepb.TaskQueue, identity string) func() {
				ctx, cancel := context.WithCancel(s.Context())
				done := make(chan error, 1)
				go func() {
					done <- tc.poll(ctx, env, tq, identity)
				}()
				return func() {
					cancel()
					err := await.Rcv(t, done)
					require.True(t, common.IsContextCanceledErr(err), "poll returned before cancellation: %v", err)
				}
			}
			waitForPoller := func(tq *taskqueuepb.TaskQueue, identity string) {
				// Polls register asynchronously in matching after acquiring their frontend concurrency slot.
				await.Require(s.Context(), t, func(t *await.T) {
					resp, err := env.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(t.Context(),
						&matchingservice.DescribeTaskQueuePartitionRequest{
							NamespaceId: env.NamespaceID().String(),
							TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
								TaskQueue:     tq.GetName(),
								TaskQueueType: tc.taskQueueType,
							},
							Versions:      &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
							ReportPollers: true,
						})
					require.NoError(t, err)
					var identities []string
					for _, poller := range resp.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo().GetPollers() {
						identities = append(identities, poller.GetIdentity())
					}
					require.Contains(t, identities, identity)
				}, 10*time.Second, 50*time.Millisecond)
			}
			assertLimitExceeded := func(tq *taskqueuepb.TaskQueue) {
				ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
				defer cancel()
				var resourceExhausted *serviceerror.ResourceExhausted
				require.ErrorAs(t, tc.poll(ctx, env, tq, "excess-poller"), &resourceExhausted)
				require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, resourceExhausted.Cause)
				require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhausted.Scope)
			}

			defer startPoll(customerQueue, "customer-poller")()
			waitForPoller(customerQueue, "customer-poller")
			assertLimitExceeded(customerQueue)

			defer startPoll(internalQueue, "internal-poller")()
			waitForPoller(internalQueue, "internal-poller")
			assertLimitExceeded(internalQueue)
		})
	}
}
