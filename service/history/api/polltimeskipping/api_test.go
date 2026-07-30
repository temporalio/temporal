package polltimeskipping

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/notification"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testWorkflowID    = "wf-id"
	testFastForwardID = "ff-id"
)

// testFastForwardTarget is the target every fixture below installs. waitFastForwardNotification
// compares the notification's target against the caller's, so the pending info and the notified
// info must agree on it or every notification reads as a replaced fast-forward.
var testFastForwardTarget = time.Unix(0, 0).Add(time.Hour)

// mockConsistencyChecker returns a fixed lease (or error) from GetWorkflowLease; the
// other WorkflowConsistencyChecker methods are unused here and left to the embedded nil.
type mockConsistencyChecker struct {
	api.WorkflowConsistencyChecker
	lease api.WorkflowLease
	err   error
}

func (m mockConsistencyChecker) GetWorkflowLease(
	context.Context, *clockspb.VectorClock, definition.WorkflowKey, locks.Priority,
) (api.WorkflowLease, error) {
	return m.lease, m.err
}

type mockLease struct {
	api.WorkflowLease
	ms historyi.MutableState
}

func (m mockLease) GetMutableState() historyi.MutableState            { return m.ms }
func (m mockLease) GetReleaseFn() historyi.ReleaseWorkflowContextFunc { return func(error) {} }

func fastForwardTSI(fastForwardID string, hasReached bool) *persistencespb.TimeSkippingInfo {
	return &persistencespb.TimeSkippingInfo{
		Config: &commonpb.TimeSkippingConfig{
			Enabled:           true,
			FastForwardConfig: &commonpb.FastForwardConfig{Id: fastForwardID},
		},
		FastForwardInfo: &persistencespb.FastForwardInfo{
			HasReached: hasReached,
			TargetTime: timestamppb.New(testFastForwardTarget),
		},
	}
}

func tsiNotif(fastForwardID string, hasReached, workflowCompleted bool) *notification.TimeSkippingFastForwardNotification {
	return &notification.TimeSkippingFastForwardNotification{
		TimeSkippingInfo:           fastForwardTSI(fastForwardID, hasReached),
		WorkflowExecutionCompleted: workflowCompleted,
	}
}

func disabledTSI(fastForwardID string) *persistencespb.TimeSkippingInfo {
	tsi := fastForwardTSI(fastForwardID, false)
	tsi.Config.Enabled = false
	return tsi
}

func mutableState(ctrl *gomock.Controller, tsi *persistencespb.TimeSkippingInfo, running bool, newRunID string) historyi.MutableState {
	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TimeSkippingInfo:  tsi,
		NewExecutionRunId: newRunID,
	}).AnyTimes()
	ms.EXPECT().IsWorkflowExecutionRunning().Return(running).AnyTimes()
	return ms
}

func shardContext(ctrl *gomock.Controller, softTimeout time.Duration) historyi.ShardContext {
	nsReg := namespace.NewMockRegistry(ctrl)
	nsReg.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	shard := historyi.NewMockShardContext(ctrl)
	cfg := tests.NewDynamicConfig()
	cfg.LongPollExpirationInterval = func(string) time.Duration { return softTimeout }
	shard.EXPECT().GetConfig().Return(cfg).AnyTimes()
	shard.EXPECT().GetNamespaceRegistry().Return(nsReg).AnyTimes()
	shard.EXPECT().GetLifecycleContext().Return(context.Background()).AnyTimes()
	return shard
}

func pollReq(namespaceID, workflowID, fastForwardID string) *historyservice.PollWorkflowExecutionTimeSkippingRequest {
	return &historyservice.PollWorkflowExecutionTimeSkippingRequest{
		NamespaceId: namespaceID,
		Request: &workflowservice.PollWorkflowExecutionTimeSkippingRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			FastForwardId:     fastForwardID,
		},
	}
}

func TestInvoke(t *testing.T) {
	t.Run("invalid namespace id is rejected", func(t *testing.T) {
		_, err := Invoke(context.Background(), pollReq("", testWorkflowID, testFastForwardID),
			nil, mockConsistencyChecker{}, notification.NoopTimeSkippingFastForwardNotifier)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
	})

	t.Run("get workflow lease error is propagated", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{err: serviceerror.NewNotFound("workflow not found")}
		_, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("no fast-forward info fails with an id mismatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, nil, true, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, resp.GetResponse().GetFastForwardPollingResult())
		require.Equal(t, failedReasonIDMismatch, resp.GetResponse().GetFailedReason())
	})

	t.Run("fast-forward id mismatch fails with an id mismatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI("other-id", false), true, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, resp.GetResponse().GetFastForwardPollingResult())
		require.Equal(t, failedReasonIDMismatch, resp.GetResponse().GetFailedReason())
	})

	t.Run("already completed returns completed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, true), true, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED, resp.GetResponse().GetFastForwardPollingResult())
		require.True(t, resp.GetResponse().GetFastForwardInfo().GetHasCompleted())
		require.Equal(t, testFastForwardID, resp.GetResponse().GetFastForwardInfo().GetFastForwardId())
	})

	t.Run("run closed before completion fails because the execution ended", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		// pending fast-forward, not running, no successor run => can never complete.
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, false), false, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, resp.GetResponse().GetFastForwardPollingResult())
		require.Equal(t, failedReasonExecutionEnded, resp.GetResponse().GetFailedReason())
	})

	t.Run("time skipping disabled before completion fails with the disabled reason", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		// matching pending fast-forward, but time skipping is disabled => can never complete.
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, disabledTSI(testFastForwardID), true, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, resp.GetResponse().GetFastForwardPollingResult())
		require.Equal(t, failedReasonTimeSkippingDisabled, resp.GetResponse().GetFailedReason())
	})

	// step-2's checks are ordered, and the states below satisfy more than one of them at once,
	// so these pin the precedence and must mirror waitFastForwardNotification's switch order.
	t.Run("step-2 mismatched id wins over completion and closed run", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI("other-id", true), false, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, resp.GetResponse().GetFastForwardPollingResult())
		require.Equal(t, failedReasonIDMismatch, resp.GetResponse().GetFailedReason())
	})

	t.Run("step-2 completion wins over closed run", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, true), false, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED, resp.GetResponse().GetFastForwardPollingResult())
		require.True(t, resp.GetResponse().GetFastForwardInfo().GetHasCompleted())
	})

	t.Run("step-2 closed run wins over time skipping disabled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, disabledTSI(testFastForwardID), false, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopTimeSkippingFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, resp.GetResponse().GetFastForwardPollingResult())
		require.Equal(t, failedReasonExecutionEnded, resp.GetResponse().GetFailedReason())
	})

	t.Run("closed run with successor is not workflow-end and long-polls until timeout", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		// not running, but NewExecutionRunId set (retry/cron/CaN) => not "closed"; polls and times out.
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, false), false, "next-run")}}
		ffNotifier := notification.NewTimeSkippingFastForwardNotifier()
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, ffNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_POLL_TIMEOUT, resp.GetResponse().GetFastForwardPollingResult())
	})

	t.Run("pending fast-forward with no notification times out and returns the pending info", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, false), true, "")}}
		// Real notifier: never notified, so the wait blocks until the soft timeout.
		ffNotifier := notification.NewTimeSkippingFastForwardNotifier()
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, ffNotifier)
		require.NoError(t, err)
		require.Equal(t, enumspb.FAST_FORWARD_POLLING_RESULT_POLL_TIMEOUT, resp.GetResponse().GetFastForwardPollingResult())
		require.Equal(t, testFastForwardID, resp.GetResponse().GetFastForwardInfo().GetFastForwardId())
	})
}

func TestWaitFastForwardNotification(t *testing.T) {
	pending := &commonpb.TimeSkippingFastForwardInfo{
		FastForwardId: testFastForwardID,
		TargetTime:    timestamppb.New(testFastForwardTarget),
	}
	completed := enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED
	failed := enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED
	pollTimeout := enumspb.FAST_FORWARD_POLLING_RESULT_POLL_TIMEOUT

	t.Run("completion notification returns completed", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- tsiNotif(testFastForwardID, true, false)
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, completed, result)
		require.Empty(t, reason, "a successful outcome must not carry a failure reason")
		require.True(t, ffinfo.GetHasCompleted())
	})

	t.Run("different fast-forward id fails with an id mismatch", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- tsiNotif("new-id", false, false)
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, failed, result)
		require.Equal(t, failedReasonIDMismatch, reason)
		require.Equal(t, "new-id", ffinfo.GetFastForwardId())
	})

	// Reusing an id does not make it the same fast-forward: a replaced target must be reported
	// rather than silently waited on.
	t.Run("same id with a different target fails as replaced", func(t *testing.T) {
		replaced := fastForwardTSI(testFastForwardID, false)
		replaced.FastForwardInfo.TargetTime = timestamppb.New(testFastForwardTarget.Add(time.Hour))
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- &notification.TimeSkippingFastForwardNotification{TimeSkippingInfo: replaced}
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, failed, result)
		require.Equal(t, failedReasonReset, reason)
		require.Equal(t, testFastForwardID, ffinfo.GetFastForwardId())
	})

	// Completion preserves the target, so the target check must not swallow the caller's own
	// completion — the arm ordering is what guarantees that.
	t.Run("completion with the caller's target still returns completed", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- tsiNotif(testFastForwardID, true, false)
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, completed, result)
		require.Empty(t, reason, "a successful outcome must not carry a failure reason")
		require.True(t, ffinfo.GetHasCompleted())
	})

	t.Run("closed run fails because the execution ended", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- tsiNotif(testFastForwardID, false, true)
		_, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, failed, result)
		require.Equal(t, failedReasonExecutionEnded, reason)
	})

	t.Run("time skipping disabled fails with the disabled reason", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- &notification.TimeSkippingFastForwardNotification{TimeSkippingInfo: disabledTSI(testFastForwardID)}
		_, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, failed, result)
		require.Equal(t, failedReasonTimeSkippingDisabled, reason)
	})

	// The switch arms are mutually reachable, so these pin the precedence rather than the
	// individual outcomes: each notification below satisfies more than one arm.
	t.Run("mismatched id wins over both completions", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- tsiNotif("new-id", true, true)
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, failed, result)
		require.Equal(t, failedReasonIDMismatch, reason)
		require.Equal(t, "new-id", ffinfo.GetFastForwardId())
	})

	t.Run("fast-forward completion wins over workflow completion", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- tsiNotif(testFastForwardID, true, true)
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, completed, result)
		require.Empty(t, reason, "a successful outcome must not carry a failure reason")
		require.True(t, ffinfo.GetHasCompleted())
	})

	t.Run("workflow completion wins over time skipping disabled", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- &notification.TimeSkippingFastForwardNotification{
			TimeSkippingInfo:           disabledTSI(testFastForwardID),
			WorkflowExecutionCompleted: true,
		}
		_, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, failed, result)
		require.Equal(t, failedReasonExecutionEnded, reason)
	})

	t.Run("no-op wake keeps waiting until a meaningful change", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 2)
		// same id, not completed, not closed => no meaningful change, must keep waiting.
		ch <- tsiNotif(testFastForwardID, false, false)
		ch <- tsiNotif(testFastForwardID, true, false)
		_, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, completed, result)
		require.Empty(t, reason)
	})

	// Delivery permits a nil payload, so this must not depend on the switch order to stay safe.
	t.Run("nil notification fails with an id mismatch without panicking", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification, 1)
		ch <- nil
		var result enumspb.FastForwardPollingResult
		var reason string
		var err error
		require.NotPanics(t, func() {
			_, result, reason, err = waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		})
		require.NoError(t, err)
		require.Equal(t, failed, result)
		require.Equal(t, failedReasonIDMismatch, reason)
	})

	t.Run("closed channel returns an internal error", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification)
		close(ch)
		_, _, _, err := waitFastForwardNotification(context.Background(), context.Background(), ch, time.Minute, testFastForwardID, pending)
		var internalErr *serviceerror.Internal
		require.ErrorAs(t, err, &internalErr)
	})

	t.Run("shard move wakes the poll before the soft timeout", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification)
		shardCtx, cancelShard := context.WithCancel(context.Background())
		cancelShard() // shard already moved/closed
		// Long soft timeout: if the shard-lifecycle case were missing, this would block for a minute.
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), shardCtx, ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, pollTimeout, result)
		require.Empty(t, reason)
		require.Same(t, pending, ffinfo)
	})

	t.Run("timeout returns the pending info unchanged", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification)
		ffinfo, result, reason, err := waitFastForwardNotification(context.Background(), context.Background(), ch, 20*time.Millisecond, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, pollTimeout, result)
		require.Empty(t, reason)
		require.Same(t, pending, ffinfo)
	})

	t.Run("caller context cancellation is propagated as an error, not a poll-timeout", func(t *testing.T) {
		ch := make(chan *notification.TimeSkippingFastForwardNotification)
		callerCtx, cancelCaller := context.WithCancel(context.Background())
		cancelCaller() // client disconnected / deadline exceeded
		// Long soft timeout so the only thing that fires is the caller's cancelled context.
		_, _, _, err := waitFastForwardNotification(callerCtx, context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.ErrorIs(t, err, context.Canceled)
	})
}
