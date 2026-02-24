package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// ErrMalformedComponentRef is returned when component ref bytes cannot be deserialized.
var ErrMalformedComponentRef = serviceerror.NewInvalidArgument("malformed component ref")

// ErrInvalidComponentRef is returned when component ref bytes deserialize to an invalid component ref.
var ErrInvalidComponentRef = serviceerror.NewInvalidArgument("invalid component ref")

// ExecutionKey uniquely identifies a CHASM execution in the system.
type ExecutionKey struct {
	NamespaceID string
	BusinessID  string
	RunID       string
}

type ComponentRef struct {
	ExecutionKey

	// archetypeID is CHASM framework's internal ID for the type of the root component of the CHASM execution.
	//
	// It is used to find and validate the loaded execution has the right archetype, especially when runID
	// is not specified in the ExecutionKey.
	archetypeID ArchetypeID
	// executionGoType is used for determining the ComponetRef's archetype.
	// When CHASM deverloper needs to create a ComponentRef, they will only provide the component type,
	// and leave the work of determining archetypeID to the CHASM framework.
	executionGoType reflect.Type

	// executionLastUpdateVT is the consistency token for the entire execution.
	executionLastUpdateVT *persistencespb.VersionedTransition

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

	validationFn func(NodeBackend, Context, Component, *Registry) error
}

// NewComponentRef creates a new ComponentRef with a registered root component go type.
//
// In V1, if you don't have a ref,
// then you can only interact with the (top level) execution.
func NewComponentRef[C Component](
	executionKey ExecutionKey,
) ComponentRef {
	return ComponentRef{
		ExecutionKey:    executionKey,
		executionGoType: reflect.TypeFor[C](),
	}
}

func (r *ComponentRef) ArchetypeID(
	registry *Registry,
) (ArchetypeID, error) {
	if r.archetypeID != UnspecifiedArchetypeID {
		return r.archetypeID, nil
	}

	rc, ok := registry.componentOf(r.executionGoType)
	if !ok {
		return 0, serviceerror.NewInternal("unknown chasm component type: " + r.executionGoType.String())
	}
	r.archetypeID = rc.componentID

	return r.archetypeID, nil
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
		RunId:                               r.RunID,
		ArchetypeId:                         archetypeID,
		ExecutionVersionedTransition:        r.executionLastUpdateVT,
		ComponentPath:                       r.componentPath,
		ComponentInitialVersionedTransition: r.componentInitialVT,
	}
	return pRef.Marshal()
}

// DeserializeComponentRef deserializes a byte slice into a ComponentRef.
// Provides caller the access to information including ExecutionKey, Archetype, and ShardingKey.
func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	if len(data) == 0 {
		return ComponentRef{}, ErrInvalidComponentRef
	}
	var pRef persistencespb.ChasmComponentRef
	if err := pRef.Unmarshal(data); err != nil {
		return ComponentRef{}, ErrMalformedComponentRef
	}

	ref := ProtoRefToComponentRef(&pRef)
	if ref.BusinessID == "" || ref.NamespaceID == "" {
		return ComponentRef{}, ErrInvalidComponentRef
	}
	return ref, nil
}

// ProtoRefToComponentRef converts a persistence ChasmComponentRef reference to a
// ComponentRef. This is useful for situations where the protobuf ComponentRef has
// already been deserialized as part of an enclosing message.
func ProtoRefToComponentRef(pRef *persistencespb.ChasmComponentRef) ComponentRef {
	return ComponentRef{
		ExecutionKey: ExecutionKey{
			NamespaceID: pRef.NamespaceId,
			BusinessID:  pRef.BusinessId,
			RunID:       pRef.RunId,
		},
		archetypeID:           pRef.ArchetypeId,
		executionLastUpdateVT: pRef.ExecutionVersionedTransition,
		componentPath:         pRef.ComponentPath,
		componentInitialVT:    pRef.ComponentInitialVersionedTransition,
	}
}
