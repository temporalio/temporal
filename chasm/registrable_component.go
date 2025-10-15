package chasm

import (
	"fmt"
	"reflect"
)

type (
	RegistrableComponent struct {
		componentType string
		library       namer
		goType        reflect.Type

		ephemeral     bool
		singleCluster bool
		shardingFn    func(EntityKey) string
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

func WithSearchAttributes(searchAttributes []*SearchAttribute) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		if len(searchAttributes) == 0 {
			return
		}

		fieldToAlias := make(map[string]string)

		for _, key := range searchAttributes {
			field := key.GetField()

			if key.GetValueType() != field.valueType {
				panic(fmt.Sprintf(
					"search attribute alias %q has type %s but field %s has type %s",
					key.GetAlias(),
					key.GetValueType(),
					field.GetFieldName(),
					field.valueType,
				))
			}

			fieldName := field.GetFieldName()
			if existingAlias, exists := fieldToAlias[fieldName]; exists {
				panic(fmt.Sprintf(
					"duplicate search attribute field %q: aliases %q and %q both map to the same field",
					fieldName,
					existingAlias,
					key.GetAlias(),
				))
			}

			fieldToAlias[fieldName] = key.GetAlias()
		}
	}
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
