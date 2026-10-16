package wci

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
)

func TestRetryable(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{err: errors.New("plain"), want: true},
		{err: context.DeadlineExceeded, want: true},
		{err: compute.NewProviderError(compute.FailureUnavailable, errors.New("x")), want: true},
		{err: compute.NewProviderError(compute.FailureThrottled, errors.New("x")), want: true},
		{err: compute.NewProviderError(compute.FailureConflict, errors.New("x")), want: true},
		{err: compute.NewProviderError(compute.FailureRejected, errors.New("x"))},
		{err: compute.NewProviderError(compute.FailureNotFound, errors.New("x"))},
		{err: compute.NewProviderError(compute.FailureAccessDenied, errors.New("x"))},
		{err: compute.NewProviderError(compute.FailureInternal, errors.New("x"))},
		{err: fmt.Errorf("wrapped: %w", ErrProviderNotSupportedOnCHASM)},
	} {
		require.Equal(t, tc.want, retryable(tc.err), "%v", tc.err)
	}
}

// invokeTest gives each transaction a fresh StateTransitionCount, as the engine does.
func invokeTest() (*WorkerControllerInstance, *ScalingGroup, func() *chasm.MockMutableContext) {
	w := rootAt(wcipb.WCI_STATUS_RUNNING)
	g := NewScalingGroup("g")
	txn := int64(0)
	next := func() *chasm.MockMutableContext {
		txn++
		count := txn
		return &chasm.MockMutableContext{MockContext: chasm.MockContext{
			HandleExecutionInfo: func() chasm.ExecutionInfo { return chasm.ExecutionInfo{StateTransitionCount: count} },
		}}
	}
	return w, g, next
}

func invokeTasks(ctx *chasm.MockMutableContext) []*wcipb.InvokeTask {
	var out []*wcipb.InvokeTask
	for _, task := range ctx.Tasks {
		if it, ok := task.Payload.(*wcipb.InvokeTask); ok {
			out = append(out, it)
		}
	}
	return out
}

func validInvoke(g *ScalingGroup, task *wcipb.InvokeTask) bool {
	ok, _ := (&InvokeTaskHandler{}).Validate(nil, g, chasm.TaskInvocation{}, task)
	return ok
}

func TestInvoke_OneInFlightAndHandOff(t *testing.T) {
	w, g, next := invokeTest()

	ctx := next()
	w.requestInvoke(ctx, g, "test-invoke")
	w.requestInvoke(ctx, g, "test-invoke")
	first := invokeTasks(ctx)
	require.Len(t, first, 1, "second invoke queues behind the first")
	require.EqualValues(t, 1, g.PendingInvokes)
	require.Equal(t, "test-invoke", ctx.Tasks[0].Attributes.Destination)

	ctx = next()
	w.finishInvoke(ctx, g, "test-invoke", nil)
	second := invokeTasks(ctx)
	require.Len(t, second, 1)
	require.NotEqual(t, first[0].GetRequestId(), second[0].GetRequestId())
	require.False(t, validInvoke(g, first[0]), "finished task is invalid, so Ignore will not drop its successor")
	require.True(t, validInvoke(g, second[0]))

	ctx = next()
	w.finishInvoke(ctx, g, "test-invoke", nil)
	require.Empty(t, invokeTasks(ctx))
	require.Zero(t, g.InflightInvokeId)
}

func TestInvoke_RetryKeepsIDThenFails(t *testing.T) {
	w, g, next := invokeTest()
	transient := compute.NewProviderError(compute.FailureUnavailable, errors.New("down"))

	ctx := next()
	w.requestInvoke(ctx, g, "p")
	w.requestInvoke(ctx, g, "p")
	first := invokeTasks(ctx)[0]

	ctx = next()
	w.finishInvoke(ctx, g, "p", transient)
	retry := invokeTasks(ctx)
	require.Len(t, retry, 1)
	require.Equal(t, first.GetRequestId(), retry[0].GetRequestId(), "a retry keeps the request ID")
	require.EqualValues(t, 2, retry[0].GetAttempt())
	require.False(t, validInvoke(g, first))

	ctx = next()
	w.finishInvoke(ctx, g, "p", transient)
	require.Empty(t, invokeTasks(ctx), "retries exhausted")
	require.Zero(t, g.InflightInvokeId)
	require.Zero(t, g.PendingInvokes)
	require.Equal(t, wcipb.VALIDATION_RESULT_FAILED, w.ValidationStatus.GetResult())
	require.IsType(t, &wcipb.SignalVersionWorkflowTask{}, ctx.Tasks[len(ctx.Tasks)-1].Payload)
}

func TestInvoke_NonRetryableFailsImmediately(t *testing.T) {
	w, g, next := invokeTest()
	ctx := next()
	w.requestInvoke(ctx, g, "p")

	ctx = next()
	w.finishInvoke(ctx, g, "p", compute.NewProviderError(compute.FailureAccessDenied, errors.New("denied")))
	require.Empty(t, invokeTasks(ctx))
	require.Equal(t, wcipb.VALIDATION_RESULT_FAILED, w.ValidationStatus.GetResult())
	require.Equal(t, "denied", w.ValidationStatus.GetErrMessage())
}

func TestApplyActions_InvokeWorker(t *testing.T) {
	w, group, ctx := newDecisionTest(t, fakeAlgo{})
	require.NoError(t, w.applyActions(ctx, "g", w.spec().ScalingGroupSpecs["g"], group, []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeInvokeWorker}}, nil))
	require.Len(t, invokeTasks(ctx), 1)
	require.Equal(t, "test-invoke", ctx.Tasks[0].Attributes.Destination)
}

func TestGroupTaskValidation(t *testing.T) {
	g := groupAt(wcipb.SCALING_GROUP_STATUS_SCALING)
	ok, err := (&ResizeTaskHandler{}).Validate(nil, g, chasm.TaskInvocation{}, &wcipb.ResizeTask{Attempt: 1})
	require.NoError(t, err)
	require.True(t, ok)
	ok, _ = (&ResizeTaskHandler{}).Validate(nil, g, chasm.TaskInvocation{}, &wcipb.ResizeTask{Attempt: 2})
	require.False(t, ok, "superseded attempt")
	ok, _ = (&ScalingGroupBackoffTaskHandler{}).Validate(nil, g, chasm.TaskInvocation{}, &wcipb.ScalingGroupBackoffTask{Attempt: 1})
	require.False(t, ok, "backoff only valid while BACKING_OFF")
}

func TestValidateAlgorithms(t *testing.T) {
	w, _, ctx := newDecisionTest(t, fakeAlgo{validateErr: errors.New("bad config")})
	err := validateAlgorithms(w.algorithmContext(ctx), w.spec())
	require.ErrorContains(t, err, "g: bad config")
}

func TestRecordValidation(t *testing.T) {
	w := rootAt(wcipb.WCI_STATUS_RUNNING)
	ctx := &chasm.MockMutableContext{}
	w.recordValidation(ctx, nil)
	require.Equal(t, wcipb.VALIDATION_RESULT_SUCCESS, w.ValidationStatus.GetResult())
	task := ctx.Tasks[0].Payload.(*wcipb.SignalVersionWorkflowTask)

	ok, _ := (&SignalVersionWorkflowTaskHandler{}).Validate(nil, w, chasm.TaskInvocation{}, task)
	require.True(t, ok)
	w.ValidationStatus.LastValidationTime.Seconds++
	ok, _ = (&SignalVersionWorkflowTaskHandler{}).Validate(nil, w, chasm.TaskInvocation{}, task)
	require.False(t, ok, "only the newest status is sent")
}
