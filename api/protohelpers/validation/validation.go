// Package validation provides typed, per-field validators for proto messages,
// intended as a thin semantic layer in production code that rejects (and
// optionally normalizes) requests before they reach business logic.
//
// A FieldValidator receives the parent message, the field's proto name, and the
// field's strongly-typed value, so it can enforce size, format, and cross-field
// rules and mutate the message in place. Per-message validators are structs of
// FieldValidators (see the generated code in ../validate); the generated
// ValidateAndNormalize invokes each field's validator in order.
//
// Validators may be dispatched by request type through a ValidatorRegistry
// (wired via Module), or constructed and invoked directly when a handler
// already has the request in hand.
package validation

import (
	"errors"
	"fmt"
	"reflect"

	"go.uber.org/fx"
)

// FieldValidator validates one field of message T holding value V. It receives
// the parent message (for cross-field checks and normalization), the field's
// proto name (for error messages), and the field value.
type FieldValidator[T any, V any] func(msg *T, fieldName string, value V) error

// Field adapts a simple (fieldName, value) → error function into a
// FieldValidator that ignores the parent message.
func Field[T any, V any](fn func(fieldName string, value V) error) FieldValidator[T, V] {
	return func(_ *T, fieldName string, value V) error {
		return fn(fieldName, value)
	}
}

// Optional returns a FieldValidator that accepts any value. Use it to
// explicitly mark a field as validated elsewhere (or not at all) — every field
// of a generated validator must be assigned, so Optional documents the choice.
func Optional[T any, V any]() FieldValidator[T, V] {
	return func(*T, string, V) error { return nil }
}

// NestedFieldValidator is like FieldValidator but for nested proto messages that
// lack their own GetNamespace(). The namespace is passed explicitly so nested
// validators can apply namespace-scoped limits.
type NestedFieldValidator[T any, V any] func(ns string, msg *T, fieldName string, value V) error

// NestedField adapts a simple (fieldName, value) → error function into a
// NestedFieldValidator, ignoring namespace and parent.
func NestedField[T any, V any](fn func(fieldName string, value V) error) NestedFieldValidator[T, V] {
	return func(_ string, _ *T, fieldName string, value V) error {
		return fn(fieldName, value)
	}
}

// requestValidator is implemented by the generated per-message validators.
type requestValidator[T any] interface {
	ValidateAndNormalize(*T) error
}

// registeredValidator is implemented by the generated per-message validators so
// they can add themselves to a registry.
type registeredValidator interface {
	RegisterValidator(*ValidatorRegistry) error
}

// ValidatorRegistry maps a request type to its validator, enabling type-based
// dispatch (e.g. from a generic interceptor).
type ValidatorRegistry struct {
	validators map[reflect.Type]any
}

// NewValidatorRegistry returns an empty registry.
func NewValidatorRegistry() *ValidatorRegistry {
	return &ValidatorRegistry{validators: make(map[reflect.Type]any)}
}

// RegisterValidator adds validator for request type T. It errors if T is
// already registered.
func RegisterValidator[T any](registry *ValidatorRegistry, validator requestValidator[T]) error {
	if registry == nil {
		return errors.New("validator registry is nil")
	}
	typ := reflect.TypeFor[*T]()
	if _, ok := registry.validators[typ]; ok {
		return fmt.Errorf("validator already registered for %v", typ)
	}
	registry.validators[typ] = validator
	return nil
}

// ValidateAndNormalize looks up the validator for req's type and runs it. It
// errors if no validator is registered for T.
func ValidateAndNormalize[T any](registry *ValidatorRegistry, req *T) error {
	v, ok := validatorFor[T](registry)
	if !ok {
		return fmt.Errorf("no validator registered for %v", reflect.TypeFor[*T]())
	}
	return v.ValidateAndNormalize(req)
}

func validatorFor[T any](registry *ValidatorRegistry) (requestValidator[T], bool) {
	if registry == nil {
		return nil, false
	}
	v, ok := registry.validators[reflect.TypeFor[*T]()]
	if !ok {
		return nil, false
	}
	rv, ok := v.(requestValidator[T])
	return rv, ok
}

const validatorGroup = `group:"protohelperValidators"`

// Module builds an fx module that assembles a *ValidatorRegistry from the given
// validator constructors. Each constructor must return a type implementing
// registeredValidator.
func Module(name string, constructors ...any) fx.Option {
	options := []fx.Option{fx.Provide(newRegistryFromValidators)}
	for _, c := range constructors {
		options = append(options, fx.Provide(fx.Annotate(
			c,
			fx.As(new(registeredValidator)),
			fx.ResultTags(validatorGroup),
		)))
	}
	return fx.Module(name, options...)
}

type registryParams struct {
	fx.In
	Validators []registeredValidator `group:"protohelperValidators"`
}

func newRegistryFromValidators(p registryParams) (*ValidatorRegistry, error) {
	registry := NewValidatorRegistry()
	for _, v := range p.Validators {
		if err := v.RegisterValidator(registry); err != nil {
			return nil, err
		}
	}
	return registry, nil
}
