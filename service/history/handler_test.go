package history

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payload"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestDescribeHistoryHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	controller := shard.NewMockController(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	h := Handler{
		config: &configs.Config{
			NumberOfShards: 10,
		},
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewNoopLogger(),
		controller:        controller,
		namespaceRegistry: namespaceRegistry,
		hostInfoProvider:  hostInfoProvider,
	}

	mockShard1 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(1)).Return(mockShard1, serviceerrors.NewShardOwnershipLost("", ""))

	_, err := h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 1,
	})
	assert.Error(t, err)
	var sol *serviceerrors.ShardOwnershipLost
	assert.True(t, errors.As(err, &sol))

	mockShard2 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 2,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(2)).Return(mockShard2, nil)
	controller.EXPECT().ShardIDs().Return([]int32{2})
	namespaceRegistry.EXPECT().GetRegistrySize().Return(int64(0), int64(0))
	hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("0.0.0.0"))
	_, err = h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 2,
	})
	assert.NoError(t, err)
}

// fakeNexusCompletionHandler is a CHASM component that records whether its completion
// handler ran and returns a canned error, used to drive the handler's run-fallback logic.
type fakeNexusCompletionHandler struct {
	chasm.UnimplementedComponent
	err error
}

func (fakeNexusCompletionHandler) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (f fakeNexusCompletionHandler) HandleNexusCompletion(chasm.MutableContext, *persistencespb.ChasmNexusCompletion) error {
	return f.err
}

func newCompleteNexusOperationChasmRequest(t *testing.T, archetypeID chasm.ArchetypeID, runID, requestID string) *historyservice.CompleteNexusOperationChasmRequest {
	t.Helper()
	pRef := &persistencespb.ChasmComponentRef{
		NamespaceId: "test-namespace-id",
		BusinessId:  "test-workflow-id",
		RunId:       runID,
		ArchetypeId: archetypeID,
	}
	refBytes, err := pRef.Marshal()
	require.NoError(t, err)
	return &historyservice.CompleteNexusOperationChasmRequest{
		Completion: &tokenspb.NexusOperationCompletion{
			RequestId:    requestID,
			ComponentRef: refBytes,
		},
		Outcome: &historyservice.CompleteNexusOperationChasmRequest_Success{
			Success: &commonpb.Payload{},
		},
	}
}

func TestCompleteNexusOperationChasm_RunFallback(t *testing.T) {
	t.Parallel()

	const (
		refRunID  = "original-run-id"
		requestID = "request-id"
	)

	// notFound is returned by run lookup / the closed-ancestor access check without ever
	// entering the completion handler; internalErr stands in for a non-NotFound failure.
	notFound := serviceerror.NewNotFound("operation not found")
	internalErr := serviceerror.NewInternal("boom")

	// A non-workflow archetype stands in for a standalone CHASM operation, which is never reset and
	// so must not trigger the current-run fallback.
	nonWorkflowArchetypeID := chasm.WorkflowArchetypeID + 1

	testCases := []struct {
		name string
		// archetypeID / runID / requestID populate the incoming ref and completion.
		archetypeID chasm.ArchetypeID
		runID       string
		requestID   string
		// setupEngine wires the mock's UpdateComponent behavior and asserts call count.
		setupEngine func(engine *chasm.MockEngine)
		// wantErrContains is the substring the returned error must contain; empty means no error.
		wantErrContains string
	}{
		{
			name:        "falls back to current run on NotFound",
			archetypeID: chasm.WorkflowArchetypeID,
			runID:       refRunID,
			requestID:   requestID,
			setupEngine: func(engine *chasm.MockEngine) {
				gomock.InOrder(
					// First lookup uses the ref's run ID and misses (run was reset).
					engine.EXPECT().UpdateComponent(gomock.Any(), matchRunID(refRunID), gomock.Any(), gomock.Any()).
						Return(nil, notFound),
					// Fallback drops the run ID and hits the current run.
					engine.EXPECT().UpdateComponent(gomock.Any(), matchRunID(""), gomock.Any(), gomock.Any()).
						Return(nil, nil),
				)
			},
			wantErrContains: "",
		},
		{
			name:        "no fallback for non-workflow archetype",
			archetypeID: nonWorkflowArchetypeID,
			runID:       refRunID,
			requestID:   requestID,
			setupEngine: func(engine *chasm.MockEngine) {
				// A standalone archetype is never reset, so the NotFound is definitive: only the
				// first lookup runs, with no current-run fallback.
				engine.EXPECT().UpdateComponent(gomock.Any(), matchRunID(refRunID), gomock.Any(), gomock.Any()).
					Return(nil, notFound)
			},
			wantErrContains: "operation not found",
		},
		{
			name:        "no fallback once the completion handler was invoked",
			archetypeID: chasm.WorkflowArchetypeID,
			runID:       refRunID,
			requestID:   requestID,
			setupEngine: func(engine *chasm.MockEngine) {
				// The ref resolved and access passed; the handler ran and rejected the
				// completion (e.g. request-ID mismatch) causes no retry.
				engine.EXPECT().UpdateComponent(gomock.Any(), matchRunID(refRunID), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, fn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
						err := fn(&chasm.MockMutableContext{}, fakeNexusCompletionHandler{err: notFound})
						return nil, err
					})
			},
			wantErrContains: "operation not found",
		},
		{
			name:        "no fallback on non-NotFound error",
			archetypeID: chasm.WorkflowArchetypeID,
			runID:       refRunID,
			requestID:   requestID,
			setupEngine: func(engine *chasm.MockEngine) {
				engine.EXPECT().UpdateComponent(gomock.Any(), matchRunID(refRunID), gomock.Any(), gomock.Any()).
					Return(nil, internalErr)
			},
			wantErrContains: "boom",
		},
		{
			name:        "no fallback when the completion has no request ID",
			archetypeID: chasm.WorkflowArchetypeID,
			runID:       refRunID,
			requestID:   "",
			setupEngine: func(engine *chasm.MockEngine) {
				engine.EXPECT().UpdateComponent(gomock.Any(), matchRunID(refRunID), gomock.Any(), gomock.Any()).
					Return(nil, notFound)
			},
			wantErrContains: "operation not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			engine := chasm.NewMockEngine(ctrl)
			tc.setupEngine(engine)

			h := &Handler{
				logger:         log.NewNoopLogger(),
				metricsHandler: metrics.NoopMetricsHandler,
			}
			ctx := chasm.NewEngineContext(context.Background(), engine)

			_, err := h.CompleteNexusOperationChasm(ctx, newCompleteNexusOperationChasmRequest(t, tc.archetypeID, tc.runID, tc.requestID))
			if tc.wantErrContains == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErrContains)
			}
		})
	}
}

// matchRunID matches a chasm.ComponentRef argument by its RunID.
func matchRunID(runID string) gomock.Matcher {
	return gomock.Cond(func(x any) bool {
		ref, ok := x.(chasm.ComponentRef)
		return ok && ref.RunID == runID
	})
}

// TestActivityMutationHandlers_MarkActivityIDInContextMetadata verifies that the activity mutation
// RPCs mark the targeted activity ID on the context, so that mutable state can resolve it to the
// activity's type and task queue during closeTransaction and the request is attributed to the
// activity rather than to its workflow. Requests that target activities by type (or match all
// pending activities) can fan out to several activities and are deliberately left unmarked.
func TestActivityMutationHandlers_MarkActivityIDInContextMetadata(t *testing.T) {
	t.Parallel()

	const (
		namespaceID = "test-namespace-id"
		workflowID  = "test-workflow-id"
		activityID  = "test-activity-id"
	)
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID}

	testCases := []struct {
		name   string
		invoke func(context.Context, *Handler) error
		want   []string
	}{
		{
			name: "PauseActivity by ID",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.PauseActivity(ctx, &historyservice.PauseActivityRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.PauseActivityRequest{
						Execution: execution,
						Activity:  &workflowservice.PauseActivityRequest_Id{Id: activityID},
					},
				})
				return err
			},
			want: []string{activityID},
		},
		{
			name: "PauseActivity by type",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.PauseActivity(ctx, &historyservice.PauseActivityRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.PauseActivityRequest{
						Execution: execution,
						Activity:  &workflowservice.PauseActivityRequest_Type{Type: "test-activity-type"},
					},
				})
				return err
			},
			want: nil,
		},
		{
			name: "UnpauseActivity by ID",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.UnpauseActivity(ctx, &historyservice.UnpauseActivityRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.UnpauseActivityRequest{
						Execution: execution,
						Activity:  &workflowservice.UnpauseActivityRequest_Id{Id: activityID},
					},
				})
				return err
			},
			want: []string{activityID},
		},
		{
			name: "UnpauseActivity unpause all",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.UnpauseActivity(ctx, &historyservice.UnpauseActivityRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.UnpauseActivityRequest{
						Execution: execution,
						Activity:  &workflowservice.UnpauseActivityRequest_UnpauseAll{UnpauseAll: true},
					},
				})
				return err
			},
			want: nil,
		},
		{
			name: "ResetActivity by ID",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.ResetActivity(ctx, &historyservice.ResetActivityRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.ResetActivityRequest{
						Execution: execution,
						Activity:  &workflowservice.ResetActivityRequest_Id{Id: activityID},
					},
				})
				return err
			},
			want: []string{activityID},
		},
		{
			name: "ResetActivity match all",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.ResetActivity(ctx, &historyservice.ResetActivityRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.ResetActivityRequest{
						Execution: execution,
						Activity:  &workflowservice.ResetActivityRequest_MatchAll{MatchAll: true},
					},
				})
				return err
			},
			want: nil,
		},
		{
			name: "UpdateActivityOptions by ID",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.UpdateActivityOptions(ctx, &historyservice.UpdateActivityOptionsRequest{
					NamespaceId: namespaceID,
					UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
						Execution: execution,
						Activity:  &workflowservice.UpdateActivityOptionsRequest_Id{Id: activityID},
					},
				})
				return err
			},
			want: []string{activityID},
		},
		{
			name: "UpdateActivityOptions match all",
			invoke: func(ctx context.Context, h *Handler) error {
				_, err := h.UpdateActivityOptions(ctx, &historyservice.UpdateActivityOptionsRequest{
					NamespaceId: namespaceID,
					UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
						Execution: execution,
						Activity:  &workflowservice.UpdateActivityOptionsRequest_MatchAll{MatchAll: true},
					},
				})
				return err
			},
			want: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			controller := shard.NewMockController(ctrl)

			// Marking happens before the shard lookup, so the mark is observable even though the
			// lookup fails and the request never reaches the engine.
			shardLookupErr := errors.New("shard lookup failed")
			controller.EXPECT().
				GetShardByNamespaceWorkflow(namespace.ID(namespaceID), workflowID).
				Return(nil, shardLookupErr)

			h := &Handler{
				logger:          log.NewNoopLogger(),
				throttledLogger: log.NewThrottledLogger(log.NewNoopLogger(), func() float64 { return 1 }),
				metricsHandler:  metrics.NoopMetricsHandler,
				controller:      controller,
			}

			ctx := contextutil.WithMetadataContext(context.Background())
			err := tc.invoke(ctx, h)
			require.ErrorIs(t, err, shardLookupErr)
			require.ElementsMatch(t, tc.want, contextutil.ContextMetadataGetMarkedActivityIDs(ctx))
		})
	}
}

func TestStartNexusOperation_SystemNexusEndpointPayloadMetadataFlag(t *testing.T) {
	registry := nexus.NewServiceRegistry()
	registry.MustRegister(chasmtests.NewTestServiceNexusService())
	nexusHandler, err := registry.NewHandler()
	require.NoError(t, err)

	h := Handler{
		logger:       log.NewNoopLogger(),
		nexusHandler: nexusHandler,
	}

	testCases := []struct {
		name      string
		operation string
	}{
		{name: "response with no nested payload", operation: "TestOperation"},
		{name: "response with nested payload", operation: "TestOperationWithPayload"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := h.StartNexusOperation(context.Background(), &historyservice.StartNexusOperationRequest{
				Request: &nexuspb.StartOperationRequest{
					Service:   "TestService",
					Operation: tc.operation,
					RequestId: "test-request-id",
					Payload:   payload.EncodeString("Temporal"),
				},
			})
			require.NoError(t, err)

			result := resp.GetResponse().GetSyncSuccess().GetPayload()
			require.NotNil(t, result)

			value, ok := result.GetMetadata()[commonnexus.SystemPayloadMetadataKey]
			require.True(t, ok, "expected %s metadata flag to be set", commonnexus.SystemPayloadMetadataKey)
			require.Equal(t, "true", string(value))
		})
	}
}
