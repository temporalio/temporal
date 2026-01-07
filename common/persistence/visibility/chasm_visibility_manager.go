package visibility

import (
	"context"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type ChasmVisibilityManager struct {
	registry      *chasm.Registry
	visibilityMgr manager.VisibilityManager
}

var _ chasm.VisibilityManager = (*ChasmVisibilityManager)(nil)

func NewChasmVisibilityManager(
	registry *chasm.Registry,
	visibilityMgr manager.VisibilityManager,
) *ChasmVisibilityManager {
	return &ChasmVisibilityManager{
		registry:      registry,
		visibilityMgr: visibilityMgr,
	}
}

func ChasmVisibilityManagerProvider(
	registry *chasm.Registry,
	visibilityMgr manager.VisibilityManager,
) chasm.VisibilityManager {
	return NewChasmVisibilityManager(registry, visibilityMgr)
}

// ListExecutions implements the Engine interface for visibility queries.
func (e *ChasmVisibilityManager) ListExecutions(
	ctx context.Context,
	archetypeType reflect.Type,
	request *chasm.ListExecutionsRequest,
) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
	archetypeID, ok := e.registry.ArchetypeIDOf(archetypeType)
	if !ok {
		return nil, serviceerror.NewInternal("unknown chasm component type: " + archetypeType.String())
	}

	visReq := &manager.ListChasmExecutionsRequest{
		ArchetypeID:   archetypeID,
		NamespaceID:   namespace.ID(request.NamespaceID),
		Namespace:     namespace.Name(request.NamespaceName),
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
		Query:         request.Query,
	}

	return e.visibilityMgr.ListChasmExecutions(ctx, visReq)
}

// CountExecutions implements the Engine interface for visibility queries.
func (e *ChasmVisibilityManager) CountExecutions(
	ctx context.Context,
	archetypeType reflect.Type,
	request *chasm.CountExecutionsRequest,
) (*chasm.CountExecutionsResponse, error) {
	archetypeID, ok := e.registry.ArchetypeIDOf(archetypeType)
	if !ok {
		return nil, serviceerror.NewInternal("unknown chasm component type: " + archetypeType.String())
	}

	visReq := &manager.CountChasmExecutionsRequest{
		ArchetypeID: archetypeID,
		NamespaceID: namespace.ID(request.NamespaceID),
		Namespace:   namespace.Name(request.NamespaceName),
		Query:       request.Query,
	}

	return e.visibilityMgr.CountChasmExecutions(ctx, visReq)
}
