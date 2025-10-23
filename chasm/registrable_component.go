package chasm

import (
	"fmt"
	"reflect"

	enumspb "go.temporal.io/api/enums/v1"
)

type (
	RegistrableComponent struct {
		componentType string
		library       namer
		goType        reflect.Type

		ephemeral     bool
		singleCluster bool
		shardingFn    func(EntityKey) string

		// Search attribute mappings
		aliasToField map[string]string
		fieldToAlias map[string]string
		saTypeMap    map[string]enumspb.IndexedValueType
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
		rc.aliasToField = make(map[string]string, len(searchAttributes))
		rc.fieldToAlias = make(map[string]string, len(searchAttributes))
		rc.saTypeMap = make(map[string]enumspb.IndexedValueType, len(searchAttributes))

		for _, sa := range searchAttributes {
			alias := sa.getAlias()
			field := sa.getField()
			valueType := sa.getValueType()

			rc.aliasToField[alias] = field
			rc.fieldToAlias[field] = alias
			rc.saTypeMap[field] = valueType
		}
	}
}

// validate checks for errors in the component configuration.
func (rc *RegistrableComponent) validate() error {
	// Check for duplicate field names in search attributes
	fieldToAliases := make(map[string][]string)
	for alias, field := range rc.aliasToField {
		fieldToAliases[field] = append(fieldToAliases[field], alias)
	}
	for field, aliases := range fieldToAliases {
		if len(aliases) > 1 {
			return fmt.Errorf("search attributes contain duplicate field names: field '%s' is used by multiple aliases: %v", field, aliases)
		}
	}
	return nil
}

// fqType returns the fully qualified name of the component, which is a combination of
// the library name and the component type. This is used to uniquely identify
// the component in the registry.
func (rc RegistrableComponent) fqType() string {
	if rc.library == nil {
		// this should never happen because the component is only accessible from the library.
		panic("component is not registered to a library")
	}
	return fullyQualifiedName(rc.library.Name(), rc.componentType)
}

// GetAlias returns the search attribute alias for the given field name.
func (rc *RegistrableComponent) GetAlias(field string) string {
	if rc.fieldToAlias == nil {
		return ""
	}
	return rc.fieldToAlias[field]
}

// GetField returns the search attribute field name for the given alias.
func (rc *RegistrableComponent) GetField(alias string) string {
	if rc.aliasToField == nil {
		return ""
	}
	return rc.aliasToField[alias]
}
