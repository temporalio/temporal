package polltimeskipping

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
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
		Config: &commonpb.TimeSkippingConfig{FastForwardId: fastForwardID},
		FastForwardInfo: &persistencespb.FastForwardInfo{
			HasReached: hasReached,
			TargetTime: timestamppb.New(time.Unix(0, 0).Add(time.Hour)),
		},
	}
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
			nil, mockConsistencyChecker{}, notification.NoopFastForwardNotifier)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
	})

	t.Run("get workflow lease error is propagated", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{err: serviceerror.NewNotFound("workflow not found")}
		_, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopFastForwardNotifier)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("no fast-forward info returns not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, nil, true, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND, resp.GetResponse().GetResult())
	})

	t.Run("fast-forward id mismatch returns not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI("other-id", false), true, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND, resp.GetResponse().GetResult())
	})

	t.Run("already completed returns completed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, true), true, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_COMPLETED, resp.GetResponse().GetResult())
		require.True(t, resp.GetResponse().GetFastForwardInfo().GetHasCompleted())
		require.Equal(t, testFastForwardID, resp.GetResponse().GetFastForwardInfo().GetFastForwardId())
	})

	t.Run("run closed before completion returns workflow-end", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		// pending fast-forward, not running, no successor run => can never complete.
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, false), false, "")}}
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, notification.NoopFastForwardNotifier)
		require.NoError(t, err)
		require.Equal(t, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_WORKFLOW_END_BEFORE_FAST_FORWARD_COMPLETION, resp.GetResponse().GetResult())
	})

	t.Run("closed run with successor is not workflow-end and long-polls until timeout", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		// not running, but NewExecutionRunId set (retry/cron/CaN) => not "closed"; polls and times out.
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, false), false, "next-run")}}
		ffNotifier := notification.NewFastForwardNotifier(func(namespace.ID, string) int32 { return 1 })
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, ffNotifier)
		require.NoError(t, err)
		require.Equal(t, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_POLL_TIMEOUT, resp.GetResponse().GetResult())
	})

	t.Run("pending fast-forward with no notification times out and returns the pending info", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		checker := mockConsistencyChecker{lease: mockLease{ms: mutableState(ctrl, fastForwardTSI(testFastForwardID, false), true, "")}}
		// Real notifier: never notified, so the wait blocks until the soft timeout.
		ffNotifier := notification.NewFastForwardNotifier(func(namespace.ID, string) int32 { return 1 })
		resp, err := Invoke(context.Background(), pollReq(uuid.NewString(), testWorkflowID, testFastForwardID),
			shardContext(ctrl, 20*time.Millisecond), checker, ffNotifier)
		require.NoError(t, err)
		require.Equal(t, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_POLL_TIMEOUT, resp.GetResponse().GetResult())
		require.Equal(t, testFastForwardID, resp.GetResponse().GetFastForwardInfo().GetFastForwardId())
	})
}

func TestWaitFastForwardNotification(t *testing.T) {
	pending := &commonpb.TimeSkippingFastForwardInfo{FastForwardId: testFastForwardID}
	completed := workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_COMPLETED
	notFound := workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND
	workflowEnd := workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_WORKFLOW_END_BEFORE_FAST_FORWARD_COMPLETION
	pollTimeout := workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_POLL_TIMEOUT

	t.Run("completion notification returns completed", func(t *testing.T) {
		ch := make(chan *notification.FastForwardNotification, 1)
		ch <- &notification.FastForwardNotification{FastForwardInfo: &commonpb.TimeSkippingFastForwardInfo{FastForwardId: testFastForwardID, HasCompleted: true}}
		ffinfo, result, err := waitFastForwardNotification(context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, completed, result)
		require.True(t, ffinfo.GetHasCompleted())
	})

	t.Run("different fast-forward id returns not-found", func(t *testing.T) {
		ch := make(chan *notification.FastForwardNotification, 1)
		ch <- &notification.FastForwardNotification{FastForwardInfo: &commonpb.TimeSkippingFastForwardInfo{FastForwardId: "new-id"}}
		ffinfo, result, err := waitFastForwardNotification(context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, notFound, result)
		require.Equal(t, "new-id", ffinfo.GetFastForwardId())
	})

	t.Run("closed run returns workflow-end", func(t *testing.T) {
		ch := make(chan *notification.FastForwardNotification, 1)
		ch <- &notification.FastForwardNotification{FastForwardInfo: &commonpb.TimeSkippingFastForwardInfo{FastForwardId: testFastForwardID}, WorkflowExecutionCompleted: true}
		_, result, err := waitFastForwardNotification(context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, workflowEnd, result)
	})

	t.Run("no-op wake keeps waiting until a meaningful change", func(t *testing.T) {
		ch := make(chan *notification.FastForwardNotification, 2)
		// same id, not completed, not closed => no meaningful change, must keep waiting.
		ch <- &notification.FastForwardNotification{FastForwardInfo: &commonpb.TimeSkippingFastForwardInfo{FastForwardId: testFastForwardID}}
		ch <- &notification.FastForwardNotification{FastForwardInfo: &commonpb.TimeSkippingFastForwardInfo{FastForwardId: testFastForwardID, HasCompleted: true}}
		_, result, err := waitFastForwardNotification(context.Background(), ch, time.Minute, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, completed, result)
	})

	t.Run("closed channel returns an internal error", func(t *testing.T) {
		ch := make(chan *notification.FastForwardNotification)
		close(ch)
		_, _, err := waitFastForwardNotification(context.Background(), ch, time.Minute, testFastForwardID, pending)
		var internalErr *serviceerror.Internal
		require.ErrorAs(t, err, &internalErr)
	})

	t.Run("timeout returns the pending info unchanged", func(t *testing.T) {
		ch := make(chan *notification.FastForwardNotification)
		ffinfo, result, err := waitFastForwardNotification(context.Background(), ch, 20*time.Millisecond, testFastForwardID, pending)
		require.NoError(t, err)
		require.Equal(t, pollTimeout, result)
		require.Same(t, pending, ffinfo)
	})
}
