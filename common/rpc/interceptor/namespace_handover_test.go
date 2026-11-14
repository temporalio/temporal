package interceptor

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
)

func TestNamespaceHandoverConcurrentCallbacks(t *testing.T) {
	t.Parallel()
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	interceptor := handoverInterceptorForTest(registry)
	handover := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_HANDOVER, true)
	active := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_NORMAL, true)
	const attempts = 256
	registry.EXPECT().GetNamespace(handover.Name()).Return(handover, nil).Times(attempts)
	var callback namespace.StateChangeCallbackFn
	registry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any()).Do(
		func(key any, cb namespace.StateChangeCallbackFn) {
			callback = cb
			registry.EXPECT().UnregisterStateChangeCallback(key)
			// Registration catch-up can overlap notifications from a registry refresh.
			invokeHandoverCallbacksConcurrently(t, cb, active)
		},
	).Times(attempts)

	for range attempts {
		waitTime, err := interceptor.waitNamespaceHandoverUpdate(t.Context(), handover.Name(), "StartWorkflowExecution")
		require.NoError(t, err)
		require.NotNil(t, waitTime)
		// A notifier may retain the callback after it has been unregistered.
		invokeHandoverCallbacksConcurrently(t, callback, active)
	}
}

func TestNamespaceHandoverIndependentWaiters(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		registry := namespace.NewMockRegistry(gomock.NewController(t))
		interceptor := handoverInterceptorForTest(registry)
		handover := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_HANDOVER, true)
		active := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_NORMAL, true)
		callbacks := make(chan namespace.StateChangeCallbackFn, 2)
		results := make(chan error, 2)
		registry.EXPECT().GetNamespace(handover.Name()).Return(handover, nil).Times(2)
		registry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any()).Do(
			func(key any, cb namespace.StateChangeCallbackFn) {
				registry.EXPECT().UnregisterStateChangeCallback(key)
				callbacks <- cb
			},
		).Times(2)
		for range 2 {
			go func() {
				_, err := interceptor.waitNamespaceHandoverUpdate(t.Context(), handover.Name(), "StartWorkflowExecution")
				results <- err
			}()
		}
		synctest.Wait()
		first, second := await.Rcv(t, callbacks), await.Rcv(t, callbacks)
		first(active, false)
		synctest.Wait()
		require.NoError(t, await.Rcv(t, results))
		select {
		case err := <-results:
			t.Fatalf("second waiter returned before its callback: %v", err)
		default:
		}
		second(active, false)
		require.NoError(t, await.Rcv(t, results))
	})
}

func TestNamespaceHandoverCallbackConditions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		state    enumspb.NamespaceState
		repState enumspb.ReplicationState
		global   bool
		deleted  bool
		otherID  bool
		wantErr  error
	}{
		{name: "handover completed", state: enumspb.NAMESPACE_STATE_REGISTERED, repState: enumspb.REPLICATION_STATE_NORMAL, global: true},
		{name: "namespace deleted", state: enumspb.NAMESPACE_STATE_DELETED, repState: enumspb.REPLICATION_STATE_HANDOVER, global: true},
		{name: "deleted from database", state: enumspb.NAMESPACE_STATE_REGISTERED, repState: enumspb.REPLICATION_STATE_HANDOVER, global: true, deleted: true},
		{name: "local namespace", state: enumspb.NAMESPACE_STATE_REGISTERED, repState: enumspb.REPLICATION_STATE_HANDOVER},
		{name: "still in handover", state: enumspb.NAMESPACE_STATE_REGISTERED, repState: enumspb.REPLICATION_STATE_HANDOVER, global: true, wantErr: common.ErrNamespaceHandover},
		{name: "different namespace", state: enumspb.NAMESPACE_STATE_REGISTERED, repState: enumspb.REPLICATION_STATE_NORMAL, global: true, otherID: true, wantErr: common.ErrNamespaceHandover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				registry := namespace.NewMockRegistry(gomock.NewController(t))
				interceptor := handoverInterceptorForTest(registry)
				handover := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_HANDOVER, true)
				updated := handoverNamespaceForTest(t, tc.state, tc.repState, tc.global)
				if tc.otherID {
					updated = updated.Clone(namespace.WithID("other-namespace-id"))
				}
				var callback namespace.StateChangeCallbackFn
				registry.EXPECT().GetNamespace(handover.Name()).Return(handover, nil)
				registry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any()).Do(
					func(key any, cb namespace.StateChangeCallbackFn) {
						callback = cb
						registry.EXPECT().UnregisterStateChangeCallback(key)
						cb(updated, tc.deleted)
					},
				)
				waitTime, err := interceptor.waitNamespaceHandoverUpdate(t.Context(), handover.Name(), "StartWorkflowExecution")
				require.ErrorIs(t, err, tc.wantErr)
				require.NotNil(t, waitTime)
				if tc.wantErr != nil {
					require.Equal(t, time.Second, *waitTime)
				}
				active := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_NORMAL, true)
				invokeHandoverCallbacksConcurrently(t, callback, active)
			})
		})
	}
}

func TestNamespaceHandoverDeadline(t *testing.T) {
	t.Parallel()
	for _, timeout := range []time.Duration{time.Second, ctxTailRoom / 2} {
		t.Run(timeout.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				registry := namespace.NewMockRegistry(gomock.NewController(t))
				interceptor := handoverInterceptorForTest(registry)
				handover := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_HANDOVER, true)
				registry.EXPECT().GetNamespace(handover.Name()).Return(handover, nil)
				registry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any()).Do(
					func(key any, _ namespace.StateChangeCallbackFn) {
						registry.EXPECT().UnregisterStateChangeCallback(key)
					},
				)
				ctx, cancel := context.WithTimeout(t.Context(), timeout)
				defer cancel()
				waitTime, err := interceptor.waitNamespaceHandoverUpdate(ctx, handover.Name(), "StartWorkflowExecution")
				require.ErrorIs(t, err, common.ErrNamespaceHandover)
				require.NotNil(t, waitTime)
				require.Equal(t, max(0, timeout-ctxTailRoom), *waitTime)
			})
		})
	}
}

func TestNamespaceHandoverBypass(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"GetSystemInfo", "AdditionalAllowedMethod", "StartWorkflowExecution"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			interceptor := handoverInterceptorForTest(registry)
			interceptor.additionalAllowedMethodsDuringHandover = map[string]struct{}{"AdditionalAllowedMethod": {}}
			active := handoverNamespaceForTest(t, enumspb.NAMESPACE_STATE_REGISTERED, enumspb.REPLICATION_STATE_NORMAL, true)
			if method == "StartWorkflowExecution" {
				registry.EXPECT().GetNamespace(active.Name()).Return(active, nil)
			}
			waitTime, err := interceptor.waitNamespaceHandoverUpdate(t.Context(), active.Name(), method)
			require.NoError(t, err)
			require.Nil(t, waitTime)
		})
	}
}

func handoverInterceptorForTest(registry namespace.Registry) *NamespaceHandoverInterceptor {
	return &NamespaceHandoverInterceptor{
		namespaceRegistry:      registry,
		timeSource:             clock.NewRealTimeSource(),
		nsCacheRefreshInterval: dynamicconfig.GetDurationPropertyFn(time.Second),
	}
}

func handoverNamespaceForTest(t *testing.T, state enumspb.NamespaceState, repState enumspb.ReplicationState, global bool) *namespace.Namespace {
	t.Helper()
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace-name", State: state},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{State: repState},
	}
	resolver := namespace.NewDefaultReplicationResolverFactory()(detail)
	ns, err := namespace.FromPersistentState(detail, resolver, namespace.WithGlobalFlag(global))
	require.NoError(t, err)
	return ns
}

func invokeHandoverCallbacksConcurrently(t *testing.T, cb namespace.StateChangeCallbackFn, ns *namespace.Namespace) {
	t.Helper()
	const goroutines = 16
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(goroutines)
	done.Add(goroutines)
	for range goroutines {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			cb(ns, false)
		}()
	}
	ready.Wait()
	close(start)
	cb(ns, false)
	done.Wait()
}
