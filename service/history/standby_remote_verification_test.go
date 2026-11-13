package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestExternalTransferTaskPending(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"signal", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name    string
				mutate  func(*persistencespb.WorkflowMutableState)
				pending bool
				retry   bool
			}{
				{name: "pending", pending: true},
				{name: "resolved", mutate: func(s *persistencespb.WorkflowMutableState) {
					s.SignalInfos = nil
					s.RequestCancelInfos = nil
				}},
				{name: "different version", mutate: func(s *persistencespb.WorkflowMutableState) {
					s.SignalInfos[10].Version++
					s.RequestCancelInfos[10].Version++
				}},
				{name: "closed with pending info", mutate: func(s *persistencespb.WorkflowMutableState) {
					s.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
				}},
				{name: "remote behind initiation", mutate: func(s *persistencespb.WorkflowMutableState) {
					s.NextEventId = 10
					s.SignalInfos = nil
					s.RequestCancelInfos = nil
				}, retry: true},
				{name: "zombie", mutate: func(s *persistencespb.WorkflowMutableState) {
					s.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
				}, retry: true},
				{name: "missing execution state", mutate: func(s *persistencespb.WorkflowMutableState) {
					s.ExecutionState = nil
				}, retry: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					state := remotePendingExternalState()
					if tc.mutate != nil {
						tc.mutate(state)
					}
					pending, err := externalTransferTaskPending(remoteExternalTask(kind), state)
					require.Equal(t, tc.pending, pending)
					if tc.retry {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestVerifyPendingExternalTaskOnSource(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"signal", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name     string
				state    *persistencespb.WorkflowMutableState
				err      error
				retry    bool
				failures int
			}{
				{name: "pending", state: remotePendingExternalState(), retry: true},
				{name: "resolved", state: &persistencespb.WorkflowMutableState{
					ExecutionState: &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING},
					NextEventId:    20,
				}},
				{name: "closed", state: &persistencespb.WorkflowMutableState{
					ExecutionState: &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED},
				}},
				{name: "not found", err: serviceerror.NewNotFound("execution missing")},
				{name: "unavailable", err: serviceerror.NewUnavailable("source unavailable"), retry: true, failures: 1},
				{name: "handover", err: consts.ErrNamespaceHandover, retry: true, failures: 1},
				{name: "missing response state", retry: true, failures: 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					f := newRemoteVerificationTest(t, kind)
					f.remote.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(f.request())).
						Return(&adminservice.DescribeMutableStateResponse{DatabaseMutableState: tc.state}, tc.err)
					verify := f.executor.verifyPendingExternalTaskOnSource(f.executable)
					err := verify(t.Context(), f.task, &struct{}{}, log.NewNoopLogger())
					if !tc.retry {
						require.NoError(t, err)
						require.Equal(t, queues.RemoteVerificationState{}, f.executable.RemoteVerificationState())
						return
					}
					require.ErrorIs(t, err, consts.ErrTaskRetry)
					state := f.executable.RemoteVerificationState()
					require.Equal(t, tc.failures, state.Failures)
					require.GreaterOrEqual(t, state.NextAttemptTime.Sub(f.clock.Now()), time.Minute)
					require.Less(t, state.NextAttemptTime.Sub(f.clock.Now()), time.Minute+12*time.Second)
					for range 5 {
						require.ErrorIs(t, verify(t.Context(), f.task, &struct{}{}, log.NewNoopLogger()), consts.ErrTaskRetry)
					}
					// Local replication can finish verification during the cooldown without another remote call.
					require.NoError(t, verify(t.Context(), f.task, nil, log.NewNoopLogger()))
					require.Equal(t, time.Unix(0, 0).UTC(), f.task.GetVisibilityTime())
				})
			}
		})
	}
}

func TestRemoteVerificationFailureBackoffAndRecovery(t *testing.T) {
	t.Parallel()
	f := newRemoteVerificationTest(t, "signal")
	verify := f.executor.verifyPendingExternalTaskOnSource(f.executable)
	for attempt := range 6 {
		f.remote.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).
			Return(nil, serviceerror.NewUnavailable("source unavailable"))
		require.ErrorIs(t, verify(t.Context(), f.task, &struct{}{}, log.NewNoopLogger()), consts.ErrTaskRetry)
		state := f.executable.RemoteVerificationState()
		interval := time.Minute * time.Duration(1<<min(attempt, 3))
		require.GreaterOrEqual(t, state.NextAttemptTime.Sub(f.clock.Now()), interval)
		require.Less(t, state.NextAttemptTime.Sub(f.clock.Now()), interval+interval/5)
		f.clock.Update(state.NextAttemptTime)
	}
	f.remote.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).Return(
		&adminservice.DescribeMutableStateResponse{DatabaseMutableState: remotePendingExternalState()}, nil,
	)
	require.ErrorIs(t, verify(t.Context(), f.task, &struct{}{}, log.NewNoopLogger()), consts.ErrTaskRetry)
	state := f.executable.RemoteVerificationState()
	require.Zero(t, state.Failures)
	require.Less(t, state.NextAttemptTime.Sub(f.clock.Now()), time.Minute+12*time.Second)
	f.clock.Update(state.NextAttemptTime)
	f.remote.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("execution deleted"))
	require.NoError(t, verify(t.Context(), f.task, &struct{}{}, log.NewNoopLogger()))
}

func TestRemoteVerificationOwnershipChange(t *testing.T) {
	t.Parallel()
	f := newRemoteVerificationTest(t, "signal")
	f.remote.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *adminservice.DescribeMutableStateRequest, ...grpc.CallOption) (*adminservice.DescribeMutableStateResponse, error) {
			f.entry = f.entry.Clone(namespace.WithActiveCluster(cluster.TestAlternativeClusterName))
			return nil, serviceerror.NewNotFound("missing on old active")
		},
	)
	verify := f.executor.verifyPendingExternalTaskOnSource(f.executable)
	require.ErrorIs(t, verify(t.Context(), f.task, &struct{}{}, log.NewNoopLogger()), consts.ErrTaskRetry)
	// Local promotion bypasses the remote cooldown so the queue can execute the task as active.
	require.ErrorIs(t, verify(t.Context(), f.task, &struct{}{}, log.NewNoopLogger()), consts.ErrTaskRetry)
}

type remoteVerificationTest struct {
	executor   *transferQueueStandbyTaskExecutor
	remote     *adminservicemock.MockAdminServiceClient
	executable queues.Executable
	task       tasks.Task
	clock      *clock.EventTimeSource
	entry      *namespace.Namespace
}

func newRemoteVerificationTest(t *testing.T, kind string) *remoteVerificationTest {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &remoteVerificationTest{
		clock: clock.NewEventTimeSource().Update(time.Unix(0, 0).UTC()),
		task:  remoteExternalTask(kind),
		entry: tests.GlobalNamespaceEntry,
	}
	config := tests.NewDynamicConfig()
	shardContext := shard.NewTestContextWithTimeSource(ctrl, &persistencespb.ShardInfo{RangeId: 1}, config, f.clock)
	t.Cleanup(shardContext.StopForTest)
	registry := shardContext.Resource.NamespaceCache
	registry.EXPECT().GetNamespaceByID(gomock.Any()).DoAndReturn(func(namespace.ID) (*namespace.Namespace, error) {
		return f.entry, nil
	}).AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()
	f.remote = adminservicemock.NewMockAdminServiceClient(ctrl)
	bean := client.NewMockBean(ctrl)
	bean.EXPECT().GetRemoteAdminClient(cluster.TestCurrentClusterName).Return(f.remote, nil).AnyTimes()
	f.executor = &transferQueueStandbyTaskExecutor{
		transferQueueTaskExecutorBase: &transferQueueTaskExecutorBase{shardContext: shardContext, config: config},
		clusterName:                   cluster.TestAlternativeClusterName,
		clientBean:                    bean,
	}
	f.executable = queues.NewExecutable(
		queues.DefaultReaderId, f.task, f.executor, nil, nil, queues.NewNoopPriorityAssigner(), f.clock,
		registry, shardContext.Resource.ClusterMetadata, shardContext.ChasmRegistry(), queues.GetTaskTypeTagValue,
		log.NewNoopLogger(), metrics.NoopMetricsHandler, telemetry.NoopTracer,
	)
	return f
}

func (f *remoteVerificationTest) request() *adminservice.DescribeMutableStateRequest {
	return &adminservice.DescribeMutableStateRequest{
		Namespace: tests.Namespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: f.task.GetWorkflowID(),
			RunId:      f.task.GetRunID(),
		},
		Archetype:       chasm.WorkflowArchetype,
		SkipForceReload: true,
	}
}

func remoteExternalTask(kind string) tasks.Task {
	key := definition.NewWorkflowKey(tests.NamespaceID.String(), "workflow-id", "run-id")
	if kind == "signal" {
		return &tasks.SignalExecutionTask{WorkflowKey: key, InitiatedEventID: 10, Version: 123, VisibilityTimestamp: time.Unix(0, 0).UTC()}
	}
	return &tasks.CancelExecutionTask{WorkflowKey: key, InitiatedEventID: 10, Version: 123, VisibilityTimestamp: time.Unix(0, 0).UTC()}
}

func remotePendingExternalState() *persistencespb.WorkflowMutableState {
	return &persistencespb.WorkflowMutableState{
		ExecutionState:     &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING},
		NextEventId:        20,
		SignalInfos:        map[int64]*persistencespb.SignalInfo{10: {Version: 123}},
		RequestCancelInfos: map[int64]*persistencespb.RequestCancelInfo{10: {Version: 123}},
	}
}
