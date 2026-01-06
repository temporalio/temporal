package chasm

import (
	"fmt"
	"reflect"

	"github.com/dgryski/go-farm"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
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
		shardingFn    func(ExecutionKey) string

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
	shardingFn func(ExecutionKey) string,
) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		if shardingFn != nil {
			rc.shardingFn = shardingFn
		}
	}
}

// WithBusinessIDAlias allows specifying the business ID alias of the component.
// This option must be specified if the archetype uses the Visibility component.
func WithBusinessIDAlias(
	alias string,
) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		if rc.searchAttributesMapper == nil {
			rc.searchAttributesMapper = &VisibilitySearchAttributesMapper{
				aliasToField:       make(map[string]string),
				fieldToAlias:       make(map[string]string),
				saTypeMap:          make(map[string]enumspb.IndexedValueType),
				systemAliasToField: make(map[string]string),
			}
		}
		if rc.searchAttributesMapper.systemAliasToField == nil {
			rc.searchAttributesMapper.systemAliasToField = make(map[string]string)
		}
		if _, ok := rc.searchAttributesMapper.aliasToField[alias]; ok {
			//nolint:forbidigo
			panic(fmt.Sprintf("registrable component validation error: business ID alias %q is already defined as a search attribute", alias))
		}
		if _, ok := rc.searchAttributesMapper.systemAliasToField[alias]; ok {
			//nolint:forbidigo
			panic(fmt.Sprintf("registrable component validation error: business ID alias %q is already defined as a system search attribute", alias))
		}
		rc.searchAttributesMapper.systemAliasToField[alias] = sadefs.WorkflowID
		rc.searchAttributesMapper.fieldToAlias[sadefs.WorkflowID] = alias
		rc.searchAttributesMapper.saTypeMap[sadefs.WorkflowID] = enumspb.INDEXED_VALUE_TYPE_KEYWORD
	}
}

func WithSearchAttributes(
	searchAttributes ...SearchAttribute,
) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		if len(searchAttributes) == 0 {
			return
		}

		if rc.searchAttributesMapper == nil {
			rc.searchAttributesMapper = &VisibilitySearchAttributesMapper{
				aliasToField:       make(map[string]string, len(searchAttributes)),
				fieldToAlias:       make(map[string]string, len(searchAttributes)),
				saTypeMap:          make(map[string]enumspb.IndexedValueType, len(searchAttributes)),
				systemAliasToField: make(map[string]string),
			}
		}

		for _, sa := range searchAttributes {
			alias := sa.definition().alias
			field := sa.definition().field
			valueType := sa.definition().valueType

			if sadefs.IsSystem(alias) || sadefs.IsReserved(alias) {
				//nolint:forbidigo
				panic(fmt.Sprintf("registrable component validation error: CHASM search attribute alias %q is a system or reserved search attribute", alias))
			}

			if _, ok := rc.searchAttributesMapper.systemAliasToField[alias]; ok {
				//nolint:forbidigo
				panic(fmt.Sprintf("registrable component validation error: CHASM search attribute alias %q is already defined as a system search attribute alias", alias))
			}
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

// SearchAttributesMapper returns the search attributes mapper for this component.
func (rc *RegistrableComponent) SearchAttributesMapper() *VisibilitySearchAttributesMapper {
	return rc.searchAttributesMapper
}

// hasBusinessIDAlias returns true if the component has a businessID alias configured
// via WithBusinessIDAlias option.
func (rc *RegistrableComponent) hasBusinessIDAlias() bool {
	if rc.searchAttributesMapper == nil {
		return false
	}
	_, ok := rc.searchAttributesMapper.fieldToAlias[sadefs.WorkflowID]
	return ok
}

// GoType returns the reflect.Type of the component's Go struct.
func (rc *RegistrableComponent) GoType() reflect.Type {
	return rc.goType
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
