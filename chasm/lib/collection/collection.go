package collection

import (
	"go.temporal.io/server/chasm"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
	"go.temporal.io/server/common"
)

const (
	libraryName   = "collection"
	componentName = "collection"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

// Collection is the CHASM archetype backing the user-facing Map. For now it is an empty,
// runnable root component: it can be started, described, closed, and deleted, but holds no
// items, operations, or config. Those are added in subsequent commits.
type Collection struct {
	chasm.UnimplementedComponent

	*collectionpb.CollectionState
}

func NewCollection(_ chasm.MutableContext) (*Collection, error) {
	return &Collection{
		CollectionState: &collectionpb.CollectionState{},
	}, nil
}

func (c *Collection) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch {
	case c.Canceled:
		return chasm.LifecycleStateFailed
	case c.Closed:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateRunning
	}
}

func (c *Collection) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	c.Closed = true
	return chasm.TerminateComponentResponse{}, nil
}

func (c *Collection) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

func (c *Collection) Describe(
	_ chasm.Context,
	_ *collectionpb.DescribeCollectionExecutionRequest,
) (*collectionpb.CollectionState, error) {
	return common.CloneProto(c.CollectionState), nil
}

func (c *Collection) Close(
	_ chasm.MutableContext,
	_ chasm.NoValue,
) (*collectionpb.CloseCollectionExecutionResponse, error) {
	c.Closed = true
	return &collectionpb.CloseCollectionExecutionResponse{}, nil
}
