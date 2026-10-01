package history

import (
	"context"
	"errors"
	"testing"
	"time"

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
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payload"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestIsActivityTaskValidStandaloneActivity(t *testing.T) {
	const (
		namespaceID = "test-namespace-id"
		businessID  = "test-activity-id"
		stamp       = int32(42)
	)

	controller := gomock.NewController(t)
	shardController := shard.NewMockController(controller)
	engine := chasm.NewMockEngine(controller)
	activityCtx := &chasm.MockMutableContext{}
	standaloneActivity := &activity.Activity{
		ActivityState: &activitypb.ActivityState{
			Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		LastAttempt: chasm.NewDataField(activityCtx, &activitypb.ActivityAttemptState{
			Stamp: stamp,
		}),
	}
	engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			componentRef chasm.ComponentRef,
			readFn func(chasm.Context, chasm.Component) error,
			_ ...chasm.TransitionOption,
		) error {
			require.Equal(t, namespaceID, componentRef.NamespaceID)
			require.Equal(t, businessID, componentRef.BusinessID)
			return readFn(activityCtx, standaloneActivity)
		},
	)

	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: namespaceID,
		BusinessId:  businessID,
		ArchetypeId: activity.ArchetypeID,
	}).Marshal()
	require.NoError(t, err)

	handler := &Handler{controller: shardController}
	response, err := handler.IsActivityTaskValid(
		chasm.NewEngineContext(context.Background(), engine),
		&historyservice.IsActivityTaskValidRequest{
			NamespaceId:  namespaceID,
			Execution:    &commonpb.WorkflowExecution{},
			Stamp:        stamp,
			ComponentRef: componentRef,
		},
	)

	require.NoError(t, err)
	require.True(t, response.GetIsValid())
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

// TestCompleteNexusOperationChasm_ReturnsOperation verifies that the response identifies the
// completed operation so the frontend can tag its completion metrics.
func TestCompleteNexusOperationChasm_ReturnsOperation(t *testing.T) {
	t.Parallel()

	const requestID = "request-id"
	mockCtx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Now() },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{NamespaceID: "test-namespace-id"}
			},
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "test-namespace"}, nil, false, nil, 0)
			},
			GoCtx: context.WithValue(context.Background(), chasmnexus.OperationContextKey, &chasmnexus.OperationContext{
				MetricTagConfig: dynamicconfig.GetTypedPropertyFn(chasmnexus.NexusMetricTagConfig{}),
			}),
		},
	}
	op := &chasmnexus.Operation{
		OperationState: &nexusoperationpb.OperationState{
			Status:        nexusoperationpb.OPERATION_STATUS_STARTED,
			Endpoint:      "endpoint",
			Service:       "service",
			Operation:     "operation",
			RequestId:     requestID,
			ScheduledTime: timestamppb.Now(),
		},
		Outcome: chasm.NewDataField(mockCtx, &nexusoperationpb.OperationOutcome{}),
	}

	testCases := []struct {
		name      string
		component chasm.Component
		want      *historyservice.CompleteNexusOperationChasmResponse
	}{
		{
			name:      "Nexus operation",
			component: op,
			want: &historyservice.CompleteNexusOperationChasmResponse{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "operation",
			},
		},
		{
			name:      "other completion handler",
			component: fakeNexusCompletionHandler{},
			want:      &historyservice.CompleteNexusOperationChasmResponse{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			engine := chasm.NewMockEngine(ctrl)
			engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, fn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
					return nil, fn(mockCtx, tc.component)
				})

			h := &Handler{
				logger:         log.NewNoopLogger(),
				metricsHandler: metrics.NoopMetricsHandler,
			}
			ctx := chasm.NewEngineContext(context.Background(), engine)

			resp, err := h.CompleteNexusOperationChasm(ctx, newCompleteNexusOperationChasmRequest(t, chasm.WorkflowArchetypeID, "run-id", requestID))
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.want, resp)
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

func TestStartNexusOperation_SystemNexusEndpointRejectsNonProtoResponse(t *testing.T) {
	registry := nexus.NewServiceRegistry()
	registry.MustRegister(chasmtests.NewTestServiceNexusService())
	nexusHandler, err := registry.NewHandler()
	require.NoError(t, err)

	h := Handler{
		logger:       log.NewNoopLogger(),
		nexusHandler: nexusHandler,
	}

	// TestOperationStringOutput returns a string, which the data converter encodes as JSON
	// instead of protobuf. The System Nexus Endpoint only accepts protobuf-encoded responses.
	resp, err := h.StartNexusOperation(context.Background(), &historyservice.StartNexusOperationRequest{
		Request: &nexuspb.StartOperationRequest{
			Service:   "TestService",
			Operation: "TestOperationStringOutput",
			RequestId: "test-request-id",
			Payload:   payload.EncodeString("Temporal"),
		},
	})
	require.Nil(t, resp)

	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.ErrorContains(t, err, "system payload must be encoded as binary/protobuf but got json/plain")
}
