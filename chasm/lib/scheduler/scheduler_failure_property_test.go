package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/chasmtest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func TestSchedulerStartFailureRetryAndRedeliveryProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		env := newSchedulerPropertyEnv(t, false)
		env.drain(t, schedulerConformanceDrainLimit)
		code := rapid.SampledFrom([]codes.Code{codes.Unavailable, codes.ResourceExhausted}).Draw(t, "start retryable code")
		schedulerRPCProfiles{}.startRetryableWithCode(code).Expect(&env.services.Start, startWorkflowMethod, "first attempt", nil)
		env.trigger(t)

		failedDelivery := deliverUntilStartCall(t, env)
		startCalls := env.services.StartCalls()
		require.Len(t, startCalls, 1)
		first := startCalls[0]
		require.Equal(t, code, status.Code(first.Err))
		require.NotEmpty(t, first.Request.GetRequestId())
		env.assertScripts(t)

		redelivery, err := env.engine.Redeliver(t.Context(), env.ref, failedDelivery)
		require.NoError(t, err)
		require.True(t, redelivery.Result.Dropped)
		require.Len(t, env.services.StartCalls(), 1)

		beforeReload := env.describe(t)
		env.reload(t)
		require.True(t, proto.Equal(beforeReload, env.describe(t)))

		schedulerRPCProfiles{}.startSucceeded().Expect(&env.services.Start, startWorkflowMethod, "retry", func(request *workflowservice.StartWorkflowExecutionRequest) bool {
			return request.GetRequestId() == first.Request.GetRequestId()
		})
		// RunnableDeliveries intentionally respects category heads. Advance to the
		// scheduler wakeup as well as the shorter retry deadline so the queued retry
		// is observable through the Phase 4 delivery API.
		env.timeSource.Update(schedulerPropertyStartTime.Add(defaultInterval))
		env.drain(t, schedulerConformanceDrainLimit)
		startCalls = env.services.StartCalls()
		require.GreaterOrEqual(t, len(startCalls), 2)
		var second schedulerStartCall
		for _, call := range startCalls[1:] {
			if call.Request.GetRequestId() == first.Request.GetRequestId() {
				second = call
				break
			}
		}
		require.NoError(t, second.Err)
		require.Equal(t, first.Request.GetRequestId(), second.Request.GetRequestId())
		env.assertScripts(t)

		description := env.describe(t)
		require.GreaterOrEqual(t, description.GetInfo().GetActionCount(), int64(1))
		require.NotEmpty(t, description.GetInfo().GetRunningWorkflows())
	})
}

func deliverUntilStartCall(t *rapid.T, env *schedulerPropertyEnv) chasmtest.DeliveryRef {
	t.Helper()
	for range schedulerConformanceDrainLimit {
		runnable, err := env.engine.RunnableDeliveries(env.ref)
		require.NoError(t, err)
		require.NotEmpty(t, runnable)
		delivery := runnable[0]
		_, err = env.engine.Deliver(t.Context(), env.ref, delivery)
		require.NoError(t, err)
		if len(env.services.StartCalls()) > 0 {
			env.delivered = append(env.delivered, delivery)
			return delivery
		}
		env.delivered = append(env.delivered, delivery)
	}
	t.Fatal("start RPC was not reached within drain limit")
	return chasmtest.DeliveryRef{}
}
