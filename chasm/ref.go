package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

var (
	defaultShardingFn = func(key EntityKey) string { return key.NamespaceID + "_" + key.BusinessID }
)

// EntityKey uniquely identifies a CHASM execution in the system.
// TODO: Rename to ExecutionKey.
type EntityKey struct {
	NamespaceID string
	// TODO: Rename to EntityID.
	BusinessID string
	// TODO: Rename to RunID.
	EntityID string
}

type ComponentRef struct {
	EntityKey

	// archetypeID is CHASM framework's internal ID for the type of the root component of the CHASM execution.
	//
	// It is used to find and validate the loaded execution has the right archetype, especially whe runID
	// is not specified in the EntityKey.
	// E.g. The EntityKey can be empty and the current run of the BusinessID may have a different archetype.
	archetypeID ArchetypeID
	// entityGoType is used for determining the ComponetRef's archetype.
	// When CHASM deverloper needs to create a ComponentRef, they will only provide this information,
	// and leave the work of determining archetype to the CHASM engine.
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

	validationFn func(NodeBackend, Context, Component) error
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

func (r *ComponentRef) ArchetypeID(
	registry *Registry,
) (ArchetypeID, error) {
	if r.archetypeID != 0 {
		return r.archetypeID, nil
	}

	rc, ok := registry.componentOf(r.entityGoType)
	if !ok {
		return 0, serviceerror.NewInternal("unknown chasm component type: " + r.entityGoType.String())
	}
	r.archetypeID = rc.componentID

	return r.archetypeID, nil
}

// ShardingKey returns the sharding key used for determining the shardID of the run
// that contains the referenced component.
// TODO: remove this method and ShardingKey concept, we don't need this functionality.
func (r *ComponentRef) ShardingKey(
	registry *Registry,
) (string, error) {

	archetypeID, err := r.ArchetypeID(registry)
	if err != nil {
		return "", err
	}

	rc, ok := registry.componentByID(archetypeID)
	if !ok {
		return "", serviceerror.NewInternalf("unknown chasm component type id: %d", archetypeID)
	}

	return rc.shardingFn(r.EntityKey), nil
}

func (r *ComponentRef) Serialize(
	registry *Registry,
) ([]byte, error) {
	if r == nil {
		return nil, nil
	}

	archetypeID, err := r.ArchetypeID(registry)
	if err != nil {
		return nil, err
	}

	pRef := persistencespb.ChasmComponentRef{
		NamespaceId:                         r.NamespaceID,
		BusinessId:                          r.BusinessID,
		EntityId:                            r.EntityID,
		ArchetypeId:                         archetypeID,
		EntityVersionedTransition:           r.entityLastUpdateVT,
		ComponentPath:                       r.componentPath,
		ComponentInitialVersionedTransition: r.componentInitialVT,
	}
	return pRef.Marshal()
}

// DeserializeComponentRef deserializes a byte slice into a ComponentRef.
// Provides caller the access to information including EntityKey, Archetype, and ShardingKey.
func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	var pRef persistencespb.ChasmComponentRef
	if err := pRef.Unmarshal(data); err != nil {
		return ComponentRef{}, err
	}

	return ProtoRefToComponentRef(&pRef), nil
}

// ProtoRefToComponentRef converts a persistence ChasmComponentRef reference to a
// ComponentRef. This is useful for situations where the protobuf ComponentRef has
// already been deserialized as part of an enclosing message.
func ProtoRefToComponentRef(pRef *persistencespb.ChasmComponentRef) ComponentRef {
	return ComponentRef{
		EntityKey: EntityKey{
			NamespaceID: pRef.NamespaceId,
			BusinessID:  pRef.BusinessId,
			EntityID:    pRef.EntityId,
		},
		archetypeID:        pRef.ArchetypeId,
		entityLastUpdateVT: pRef.EntityVersionedTransition,
		componentPath:      pRef.ComponentPath,
		componentInitialVT: pRef.ComponentInitialVersionedTransition,
	}
}
