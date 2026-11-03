package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

func TestRecordHeartbeatDisabledReturnsSuccess(t *testing.T) {
	t.Parallel()
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
		Id: "namespace-id", Name: "namespace",
	}, nil, "cluster")
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceByID(ns.ID()).Return(ns, nil)
	h := &handler{
		namespaceRegistry: registry,
		enabled:           dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		logger:            log.NewNoopLogger(),
	}

	// No CHASM engine is supplied; disabled requests must return before updating state.
	response, err := h.RecordHeartbeat(t.Context(), &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: ns.ID().String(), WorkerGroupingKey: "group",
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &workerstatepb.RecordHeartbeatResponse{
		FrontendResponse: &workflowservice.RecordWorkerHeartbeatResponse{},
	}, response)
}
