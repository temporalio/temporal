package nexusoperations_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCompletionHandler_EmitsCallerMetrics(t *testing.T) {
	// All cases drive a completion that arrives before the start response, so the handler
	// fabricates the started transition (emitting schedule-to-start) and then resolves the
	// operation to the terminal outcome under test.
	allOutcomeCounters := []string{
		chasmnexus.NexusOperationSuccessCount.Name(),
		chasmnexus.NexusOperationFailedCount.Name(),
		chasmnexus.NexusOperationCancelCount.Name(),
	}

	testCases := []struct {
		name        string
		result      *commonpb.Payload
		opErr       *nexus.OperationError
		wantCounter string
	}{
		{
			name:        "succeeded",
			result:      &commonpb.Payload{},
			wantCounter: chasmnexus.NexusOperationSuccessCount.Name(),
		},
		{
			name: "failed",
			opErr: &nexus.OperationError{
				State:           nexus.OperationStateFailed,
				Message:         "operation failed",
				OriginalFailure: &nexus.Failure{Message: "operation failed"},
			},
			wantCounter: chasmnexus.NexusOperationFailedCount.Name(),
		},
		{
			name: "canceled",
			opErr: &nexus.OperationError{
				State:           nexus.OperationStateCanceled,
				Message:         "operation canceled",
				OriginalFailure: &nexus.Failure{Message: "operation canceled"},
			},
			wantCounter: chasmnexus.NexusOperationCancelCount.Name(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &hsmtest.NodeBackend{}
			node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
				ScheduleToCloseTimeout: durationpb.New(time.Hour),
			}))

			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)

			completionHandler := nexusoperations.NewCompletionHandler(captureHandler, &nexusoperations.Config{})
			require.NoError(t, completionHandler.Handle(
				context.Background(),
				fakeEnv{node},
				hsm.Ref{},
				"",      // requestID (empty skips the request-id match check)
				"token", // operationToken
				nil,     // startTime
				nil,     // links
				tc.result,
				tc.opErr,
				nil, // currentRunAccess
			))

			snapshot := capture.Snapshot()

			counter := snapshot[tc.wantCounter]
			require.Len(t, counter, 1)
			require.Equal(t, int64(1), counter[0].Value)
			require.Equal(t, "namespace-name", counter[0].Tags["namespace"])
			require.Equal(t, "workflow-type", counter[0].Tags["workflowType"])
			require.Equal(t, "endpoint", counter[0].Tags["nexus_endpoint"])

			// Exactly the outcome counter under test is recorded, and no sibling counters leak.
			for _, name := range allOutcomeCounters {
				if name == tc.wantCounter {
					require.Len(t, snapshot[name], 1, "expected counter %s recorded", name)
					continue
				}
				require.Empty(t, snapshot[name], "unexpected counter %s recorded", name)
			}

			// The fabricated start emits schedule-to-start; the terminal close emits schedule-to-close.
			require.Len(t, snapshot[chasmnexus.NexusOperationScheduleToStartLatency.Name()], 1)
			require.Len(t, snapshot[chasmnexus.NexusOperationScheduleToCloseLatency.Name()], 1)
		})
	}
}

type completionTestEnv struct {
	access func(context.Context, hsm.Ref, hsm.AccessType, func(*hsm.Node) error) error
}

func (e completionTestEnv) Access(
	ctx context.Context,
	ref hsm.Ref,
	accessType hsm.AccessType,
	accessor func(*hsm.Node) error,
) error {
	return e.access(ctx, ref, accessType, accessor)
}

func (completionTestEnv) Now() time.Time {
	return time.Now()
}

func TestCompletionHandler_CurrentRunFallback(t *testing.T) {
	const (
		requestID = "request-id"
		runID     = "original-run-id"
	)

	t.Run("normal HSM access succeeds without fallback", func(t *testing.T) {
		backend := &hsmtest.NodeBackend{}
		node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			RequestId: requestID,
		}))
		var normalCalls, fallbackCalls int
		env := completionTestEnv{access: func(_ context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) error {
			normalCalls++
			require.Equal(t, runID, ref.WorkflowKey.RunID)
			require.Equal(t, hsm.AccessWrite, accessType)
			return accessor(node)
		}}
		fallback := func(_ context.Context, _ hsm.Ref, _ string, _ func(*hsm.Node) error) error {
			fallbackCalls++
			return nil
		}

		handler := nexusoperations.NewCompletionHandler(metricstest.NewCaptureHandler(), &nexusoperations.Config{})
		err := handler.Handle(
			context.Background(),
			env,
			hsm.Ref{WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", runID)},
			requestID,
			"token",
			nil,
			nil,
			&commonpb.Payload{},
			nil,
			fallback,
		)
		require.NoError(t, err)
		require.Equal(t, 1, normalCalls)
		require.Zero(t, fallbackCalls)
	})

	t.Run("NotFound falls back to current run", func(t *testing.T) {
		backend := &hsmtest.NodeBackend{}
		node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			RequestId: requestID,
		}))
		var normalCalls, fallbackCalls int
		env := completionTestEnv{access: func(_ context.Context, ref hsm.Ref, accessType hsm.AccessType, _ func(*hsm.Node) error) error {
			normalCalls++
			require.Equal(t, runID, ref.WorkflowKey.RunID)
			require.Equal(t, hsm.AccessWrite, accessType)
			return serviceerror.NewNotFound("stale reference")
		}}
		fallback := func(_ context.Context, ref hsm.Ref, gotRequestID string, accessor func(*hsm.Node) error) error {
			fallbackCalls++
			require.Empty(t, ref.WorkflowKey.RunID)
			require.Equal(t, requestID, gotRequestID)
			return accessor(node)
		}

		handler := nexusoperations.NewCompletionHandler(metricstest.NewCaptureHandler(), &nexusoperations.Config{})
		err := handler.Handle(
			context.Background(),
			env,
			hsm.Ref{WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", runID)},
			requestID,
			"token",
			nil,
			nil,
			&commonpb.Payload{},
			nil,
			fallback,
		)
		require.NoError(t, err)
		require.Equal(t, 1, normalCalls)
		require.Equal(t, 1, fallbackCalls)
	})

	t.Run("native HSM completion retains legacy reset fallback", func(t *testing.T) {
		backend := &hsmtest.NodeBackend{}
		node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			RequestId: requestID,
		}))
		var accessCalls int
		env := completionTestEnv{access: func(_ context.Context, ref hsm.Ref, _ hsm.AccessType, accessor func(*hsm.Node) error) error {
			accessCalls++
			if accessCalls == 1 {
				require.Equal(t, runID, ref.WorkflowKey.RunID)
				return serviceerror.NewNotFound("stale reference")
			}
			require.Empty(t, ref.WorkflowKey.RunID)
			require.Nil(t, ref.StateMachineRef.MutableStateVersionedTransition)
			require.Zero(t, ref.StateMachineRef.MachineInitialVersionedTransition.TransitionCount)
			require.Zero(t, ref.StateMachineRef.MachineLastUpdateVersionedTransition.TransitionCount)
			return accessor(node)
		}}
		ref := hsm.Ref{
			WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", runID),
			StateMachineRef: &persistencespb.StateMachineRef{
				MutableStateVersionedTransition:      &persistencespb.VersionedTransition{TransitionCount: 3},
				MachineInitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		}

		handler := nexusoperations.NewCompletionHandler(metricstest.NewCaptureHandler(), &nexusoperations.Config{})
		err := handler.Handle(
			context.Background(),
			env,
			ref,
			requestID,
			"token",
			nil,
			nil,
			&commonpb.Payload{},
			nil,
			nil, // currentRunAccess
		)
		require.NoError(t, err)
		require.Equal(t, 2, accessCalls)
	})

	t.Run("request ID mismatch does not mutate or fall back", func(t *testing.T) {
		backend := &hsmtest.NodeBackend{}
		node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			RequestId: "different-request-id",
		}))
		var fallbackCalls int
		env := completionTestEnv{access: func(_ context.Context, _ hsm.Ref, _ hsm.AccessType, accessor func(*hsm.Node) error) error {
			return accessor(node)
		}}
		fallback := func(_ context.Context, _ hsm.Ref, _ string, _ func(*hsm.Node) error) error {
			fallbackCalls++
			return nil
		}

		handler := nexusoperations.NewCompletionHandler(metricstest.NewCaptureHandler(), &nexusoperations.Config{})
		err := handler.Handle(
			context.Background(),
			env,
			hsm.Ref{WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", runID)},
			requestID,
			"token",
			nil,
			nil,
			&commonpb.Payload{},
			nil,
			fallback,
		)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
		require.Zero(t, fallbackCalls)
		require.Empty(t, backend.Events)
	})

	t.Run("NotFound after matching the operation does not fall back", func(t *testing.T) {
		backend := &hsmtest.NodeBackend{}
		node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			RequestId: requestID,
		}))
		var fallbackCalls int
		env := completionTestEnv{access: func(_ context.Context, _ hsm.Ref, _ hsm.AccessType, accessor func(*hsm.Node) error) error {
			require.NoError(t, accessor(node))
			return serviceerror.NewNotFound("write failed")
		}}
		fallback := func(_ context.Context, _ hsm.Ref, _ string, _ func(*hsm.Node) error) error {
			fallbackCalls++
			return nil
		}

		handler := nexusoperations.NewCompletionHandler(metricstest.NewCaptureHandler(), &nexusoperations.Config{})
		err := handler.Handle(
			context.Background(),
			env,
			hsm.Ref{WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", runID)},
			requestID,
			"token",
			nil,
			nil,
			&commonpb.Payload{},
			nil,
			fallback,
		)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
		require.Zero(t, fallbackCalls)
	})

	t.Run("native HSM completion retains legacy fallback after operation match", func(t *testing.T) {
		firstNode := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			RequestId: requestID,
		}))
		secondNode := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			RequestId: requestID,
		}))
		var accessCalls int
		env := completionTestEnv{access: func(_ context.Context, ref hsm.Ref, _ hsm.AccessType, accessor func(*hsm.Node) error) error {
			accessCalls++
			if accessCalls == 1 {
				require.Equal(t, runID, ref.WorkflowKey.RunID)
				require.NoError(t, accessor(firstNode))
				return serviceerror.NewNotFound("write failed")
			}
			require.Empty(t, ref.WorkflowKey.RunID)
			require.Nil(t, ref.StateMachineRef.MutableStateVersionedTransition)
			require.Zero(t, ref.StateMachineRef.MachineInitialVersionedTransition.TransitionCount)
			require.Zero(t, ref.StateMachineRef.MachineLastUpdateVersionedTransition.TransitionCount)
			return accessor(secondNode)
		}}
		ref := hsm.Ref{
			WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", runID),
			StateMachineRef: &persistencespb.StateMachineRef{
				MutableStateVersionedTransition:      &persistencespb.VersionedTransition{TransitionCount: 3},
				MachineInitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		}

		handler := nexusoperations.NewCompletionHandler(metricstest.NewCaptureHandler(), &nexusoperations.Config{})
		err := handler.Handle(
			context.Background(),
			env,
			ref,
			requestID,
			"token",
			nil,
			nil,
			&commonpb.Payload{},
			nil,
			nil, // currentRunAccess
		)
		require.NoError(t, err)
		require.Equal(t, 2, accessCalls)
	})
}
