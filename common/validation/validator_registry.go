package validation

import (
	"fmt"
	"maps"
	"reflect"
)

type FieldValidator[T any, V any] func(*T, string, V) error

func Field[T any, V any](fn func(string, V) error) FieldValidator[T, V] {
	return func(_ *T, fieldName string, value V) error {
		return fn(fieldName, value)
	}
}

type requestValidator[T any] interface {
	ValidateAndNormalize(*T) error
}

type registeredValidator interface {
	RegisterValidator(*ValidatorRegistry) error
}

type ValidatorRegistry struct {
	validators map[reflect.Type]any
}

func NewValidatorRegistry() *ValidatorRegistry {
	return &ValidatorRegistry{
		validators: make(map[reflect.Type]any),
	}
}

func RegisteredValidatorsForTesting(registry *ValidatorRegistry) map[reflect.Type]any {
	if registry == nil {
		return nil
	}
	return maps.Clone(registry.validators)
}

func RegisterValidator[T any](registry *ValidatorRegistry, validator requestValidator[T]) error {
	if registry == nil {
		return fmt.Errorf("validator registry is nil")
	}
	typ := reflect.TypeFor[*T]()
	if _, ok := registry.validators[typ]; ok {
		return fmt.Errorf("validator already registered for %v", typ)
	}
	registry.validators[typ] = validator
	return nil
}

func ValidateAndNormalize[T any](registry *ValidatorRegistry, req *T) error {
	validator, ok := validatorFor[T](registry)
	if !ok {
		return fmt.Errorf("validator is not registered for %s", requestTypeName(reflect.TypeFor[T]()))
	}
	return validator.ValidateAndNormalize(req)
}

func validatorFor[T any](registry *ValidatorRegistry) (requestValidator[T], bool) {
	if registry == nil {
		return nil, false
	}
	validator, ok := registry.validators[reflect.TypeFor[*T]()]
	if !ok {
		return nil, false
	}
	typedValidator, ok := validator.(requestValidator[T])
	return typedValidator, ok
}

func requestTypeName(requestType reflect.Type) string {
	if requestType.Kind() == reflect.Pointer {
		requestType = requestType.Elem()
	}
	if requestType.Name() != "" {
		return requestType.Name()
	}
	return requestType.String()
}
