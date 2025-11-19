package chasm

import (
	"fmt"
	"reflect"

	"github.com/dgryski/go-farm"
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	RegistrableComponent struct {
		componentType string
		goType        reflect.Type

		// Those two fields are initialized when the component is registered to a library.
		library     namer
		componentID uint32

		ephemeral     bool
		singleCluster bool
		shardingFn    func(EntityKey) string

		searchAttributesMapper *VisibilitySearchAttributesMapper
	}

	RegistrableComponentOption func(*RegistrableComponent)
)

func NewRegistrableComponent[C Component](
	componentType string,
	opts ...RegistrableComponentOption,
) *RegistrableComponent {
	rc := &RegistrableComponent{
		componentType: componentType,
		goType:        reflect.TypeFor[C](),
		shardingFn:    defaultShardingFn,
	}
	for _, opt := range opts {
		opt(rc)
	}
	return rc
}

func WithEphemeral() RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		rc.ephemeral = true
	}
}

// Is there any use case where we don't want to replicate certain instances of a archetype?
func WithSingleCluster() RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		rc.singleCluster = true
	}
}

// WithShardingFn allows specifying a custom sharding key function for the component.
// TODO: remove WithShardingFn, we don't need this functionality.
func WithShardingFn(
	shardingFn func(EntityKey) string,
) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		if shardingFn != nil {
			rc.shardingFn = shardingFn
		}
	}
}

func WithSearchAttributes(
	searchAttributes ...SearchAttribute,
) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		if len(searchAttributes) == 0 {
			return
		}
		rc.searchAttributesMapper = &VisibilitySearchAttributesMapper{
			aliasToField: make(map[string]string, len(searchAttributes)),
			fieldToAlias: make(map[string]string, len(searchAttributes)),
			saTypeMap:    make(map[string]enumspb.IndexedValueType, len(searchAttributes)),
		}

		for _, sa := range searchAttributes {
			alias := sa.definition().alias
			field := sa.definition().field
			valueType := sa.definition().valueType

			if _, ok := rc.searchAttributesMapper.aliasToField[alias]; ok {
				//nolint:forbidigo
				panic(fmt.Sprintf("registrable component validation error: search attribute alias %q is already defined", alias))
			}
			if _, ok := rc.searchAttributesMapper.fieldToAlias[field]; ok {
				//nolint:forbidigo
				panic(fmt.Sprintf("registrable component validation error: search attribute field %q is already defined", field))
			}

			rc.searchAttributesMapper.aliasToField[alias] = field
			rc.searchAttributesMapper.fieldToAlias[field] = alias
			rc.searchAttributesMapper.saTypeMap[field] = valueType
		}
	}
}

func (rc *RegistrableComponent) registerToLibrary(
	library namer,
) (string, uint32, error) {
	if rc.library != nil {
		return "", 0, fmt.Errorf("component %s is already registered in library %s", rc.componentType, rc.library.Name())
	}

	rc.library = library

	fqn := rc.fqType()
	rc.componentID = generateTypeID(fqn)
	return fqn, rc.componentID, nil
}

// fqType returns the fully qualified name of the component, which is a combination of
// the library name and the component type. This is used to uniquely identify
// the component in the registry.
func (rc *RegistrableComponent) fqType() string {
	if rc.library == nil {
		// this should never happen because the component is only accessible from the library.
		panic("component is not registered to a library")
	}
	return fullyQualifiedName(rc.library.Name(), rc.componentType)
}

func generateTypeID(fqn string) uint32 {
	return farm.Fingerprint32([]byte(fqn))
}
