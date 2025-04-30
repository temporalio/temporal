package chasm

import (
	"reflect"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

var (
	defaultShardingFn = func(key EntityKey) string { return key.NamespaceID + "_" + key.BusinessID }
	RootPath          []string
)

type EntityKey struct {
	NamespaceID string
	BusinessID  string
	EntityID    string
}

type ComponentRef struct {
	EntityKey

	shardID int32
	// entityGoType for calculating the entity shardID.
	// CHASM component author has no idea about the shardID, so they will
	// only specify the entityGoType and left the conversion to the CHASM engine.
	//
	// TODO: Can we remove this field and perform the shardID conversion while
	// creating the ComponentRef?
	entityGoType reflect.Type

	// entityLastUpdateVT is the consistency token for the entire entity.
	entityLastUpdateVT *persistencespb.VersionedTransition

	// componentType is the fully qualified component type name.
	// It is for performing partial loading more efficiently in future versions of CHASM.
	//
	// From the componentType, we can find the registered component struct definition,
	// then use reflection to find sub-components and understand if those sub-components
	// need to be loaded or not.
	// We only need to do this for sub-components, path for parent/ancenstor components
	// can be inferred from the current component path and they always needs to be loaded.
	//
	// componentType string

	// componentPath and componentInitialVT are used to identify a component.
	componentPath      []string
	componentInitialVT *persistencespb.VersionedTransition

	validationFn func(Context, Component) error
}

// NewComponentRef creates a new ComponentRef with a registered root component go type.
//
// In V1, if you don't have a ref,
// then you can only interact with the (top level) entity.
func NewComponentRef[C Component](
	entityKey EntityKey,
) ComponentRef {
	return ComponentRef{
		EntityKey:    entityKey,
		entityGoType: reflect.TypeFor[C](),
	}
}

func (r *ComponentRef) Serialize() []byte {
	if r == nil {
		return nil
	}
	panic("not implemented")
}

func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	panic("not implemented")
}
