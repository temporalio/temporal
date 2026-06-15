package nexusoperation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
)

// TestOperation_BuildExecutionInfo_ReturnsIsolatedSearchAttributes proves that the
// DescribeNexusOperation response carries an isolated copy of the Visibility component's
// search-attribute map.
//
// CustomSearchAttributes returns the component's live map by reference, and the describe
// response is marshalled by gRPC after the read lease is released, so aliasing it would
// let a concurrent mutation race the marshal ("concurrent map iteration and map write").
// Rather than race the panic (nondeterministic), we test the positive invariant: mutating
// the returned response map must not touch the live component map. Before the fix this
// fails because the maps are shared.
func TestOperation_BuildExecutionInfo_ReturnsIsolatedSearchAttributes(t *testing.T) {
	logger := log.NewNoopLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&Library{}))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
		},
	}
	root := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(context.Background(), root)

	op := NewOperation(&nexusoperationpb.OperationState{Status: nexusoperationpb.OPERATION_STATUS_STARTED})
	op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{})
	op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibilityWithData(
		ctx,
		map[string]*commonpb.Payload{"saKey": payload.EncodeString("v")},
		nil,
	))
	require.NoError(t, root.SetRootComponent(op))
	_, err := root.CloseTransaction()
	require.NoError(t, err)

	ctx = chasm.NewMutableContext(context.Background(), root)
	info := op.buildExecutionInfo(ctx)
	require.Contains(t, info.GetSearchAttributes().GetIndexedFields(), "saKey")

	// Mutating the response map must not reach back into the live Visibility component map.
	info.GetSearchAttributes().GetIndexedFields()["injected"] = payload.EncodeString("x")
	require.NotContains(t, op.Visibility.Get(ctx).CustomSearchAttributes(ctx), "injected",
		"DescribeNexusOperation response SearchAttributes must be a copy, not the live Visibility map")
}
