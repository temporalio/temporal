package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
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
	// archetype is the fully qualified type name of the root component.
	// It is used to look up the component's registered sharding function,
	// which determines the shardID of the entity that contains the referenced component.
	// It is also used to validate if a given entity has the right archetype.
	// E.g. The EntityKey can be empty and the current run of the BusinessID may have a different archetype.
	archetype string
	// entityGoType is used for determining the ComponetRef's shardID and archetype.
	// When CHASM deverloper needs to create a ComponentRef, they will only provide this information,
	// and leave the work of determining the shardID and archetype to the CHASM engine.
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

// ShardingKey returns the sharding key used for determining the shardID of the run
// that contains the referenced component.
func (r *ComponentRef) ShardingKey(
	registry *Registry,
) (string, error) {
	var rc *RegistrableComponent
	var ok bool

	if r.archetype == "" {
		rc, ok = registry.componentOf(r.entityGoType)
		if !ok {
			return "", serviceerror.NewInternal("unknown chasm component type: " + r.entityGoType.String())
		}
		r.archetype = rc.fqType()
	}

	if rc == nil {
		rc, ok = registry.component(r.archetype)
		if !ok {
			return "", serviceerror.NewInternal("unknown chasm component type: " + r.archetype)
		}
	}

	if rc.shardingFn != nil {
		return rc.shardingFn(r.EntityKey), nil
	}
	return r.EntityKey.BusinessID, nil
}

// ShardID returns the shardID of the run that contains the referenced component
// given the total number of shards in the system.
func (r *ComponentRef) ShardID(
	registry *Registry,
	numberOfShards int32,
) (int32, error) {
	if r.shardID != 0 {
		return r.shardID, nil
	}
	shardingKey, err := r.ShardingKey(registry)
	if err != nil {
		return 0, err
	}

	r.shardID = common.ShardingKeyToShard(
		shardingKey,
		numberOfShards,
	)
	return r.shardID, nil
}

func (r *ComponentRef) serialize() ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	panic("not implemented")
}

func deserializeComponentRef(data []byte) (ComponentRef, error) {
	panic("not implemented")
}
