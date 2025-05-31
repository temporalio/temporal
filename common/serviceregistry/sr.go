// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package serviceregistry

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

func zero[T any]() T {
	var zero T
	return zero
}

type Strategy byte

const (
	STRATEGY_INSTANCE Strategy = iota
	STRATEGY_SINGLETON
	STRATEGY_TRANSIENT
)

func (s Strategy) String() string {
	switch s {
	case STRATEGY_INSTANCE:
		return "Instance"
	case STRATEGY_SINGLETON:
		return "Singleton"
	case STRATEGY_TRANSIENT:
		return "Transient"
	default:
		return "Unknown"
	}
}

type (

	// ServiceRegistry is the main object used to create and retrieve service instances.
	// Generics are used to inject and retrieve service instances in a
	// type-safe manner.
	// NOTE
	//
	// This ServiceRegistry is a lightweight registry intended for trivial use-cases
	// (mostly singletons). It does **not** detect or resolve circular dependencies;
	// if A depends on B and B depends on A you’ll deadlock or panic. For anything
	// beyond a handful of simple, singleton services, consider using Fx.

	ServiceRegistry struct {
		// dictionary of service entries
		providers map[string]*serviceEntry
		mu        sync.Mutex
	}

	// serviceEntry structure contains and instance of a specific service.
	serviceEntry struct {
		// value for this entry
		value any

		// factoryFunc is used by lazily provide values of a specific type
		// factoryFunc for type T should have the following signature:
		// func(*ServiceRegistry, ...) (T, error)
		// or
		// func(*ServiceRegistry) (T, error)
		factoryFunc any

		// strategy is used to determine how this entry should be created.
		strategy Strategy

		// used if strategy is STRATEGY_SINGLETON
		once sync.Once
	}
)

// NewServiceRegistry creates a new [ServiceRegistry] object.
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		providers: map[string]*serviceEntry{},
	}
}

func RegisterInstance[T any](l *ServiceRegistry, value T) T {
	key := getTypeName[T]()
	l.mu.Lock()
	l.providers[key] = &serviceEntry{
		value:    value,
		strategy: STRATEGY_INSTANCE,
	}
	l.mu.Unlock()
	return value
}

func RegisterSingleton[T any](l *ServiceRegistry, factoryFunc any) error {
	return registerFactory[T](l, factoryFunc, STRATEGY_SINGLETON)
}

func RegisterTransient[T any](l *ServiceRegistry, factoryFunc any) error {
	return registerFactory[T](l, factoryFunc, STRATEGY_TRANSIENT)
}

func registerFactory[T any](l *ServiceRegistry, factoryFunc any, strategy Strategy) error {

	if factoryFunc == nil {
		return errors.New("factory function must be provided")
	}

	fnValue := reflect.ValueOf(factoryFunc)
	if err := validateFunc[T](fnValue); err != nil {
		return err
	}

	key := getTypeName[T]()
	l.mu.Lock()
	l.providers[key] = &serviceEntry{
		factoryFunc: factoryFunc,
		strategy:    strategy,
	}
	l.mu.Unlock()
	return nil
}

// ValidateFunc ensures that:
//
//  1. fn is a function.
//  2. The function’s parameter is either exactly one input parameter of type ServiceRegistry.
//     or multiple parameters, with the first parameter being of type ServiceRegistry.
//  3. The function returns exactly two outputs.
//  4. The second output is an error.
//  5. The first output is assignable to T (and T can be `any` if you want to accept any type).
func validateFunc[T any](v reflect.Value) error {
	if v.Kind() != reflect.Func {
		return errors.New("provided generator is not a function")
	}

	t := v.Type()

	// Check that there is at least one parameter,
	if t.NumIn() < 1 {
		return fmt.Errorf("function must accept at least one parameter, but got %d", t.NumIn())
	}

	// Check that the first parameter is of type ServiceRegistry
	paramType := t.In(0)
	wantedParam := reflect.TypeOf((*ServiceRegistry)(nil)) // or pointer, depends on your design
	if paramType != wantedParam {
		return fmt.Errorf("function parameter must be ServiceRegistry, got %s", paramType)
	}

	// Check exactly two return values
	if t.NumOut() != 2 {
		return fmt.Errorf("function must return exactly two results, but got %d", t.NumOut())
	}
	firstResultType := t.Out(0)
	secondResultType := t.Out(1)

	// Check that second output is error
	errorType := reflect.TypeOf((*error)(nil)).Elem() // "error" interface
	if !secondResultType.Implements(errorType) {
		return fmt.Errorf("second result must be an error, but got %s", secondResultType)
	}

	// Check that first output is assignable to T
	//    If T is `any` (an alias for interface{}), then all types are acceptable.
	desiredType := reflect.TypeOf((*T)(nil)).Elem() // reflect.Type for T
	if !firstResultType.AssignableTo(desiredType) {
		return fmt.Errorf("first result type %s is not assignable to %s", firstResultType, desiredType)
	}

	// Validate that the first result is either an interface or pointer to a struct
	kind := firstResultType.Kind()
	if kind != reflect.Interface && !(kind == reflect.Ptr && firstResultType.Elem().Kind() == reflect.Struct) {
		return fmt.Errorf("first result type must be an interface or pointer to a struct, got %s", firstResultType)
	}

	return nil
}

// Get retrieves a service instance of type T from the ServiceRegistry.
// Note: there is no circular dependency check
func Get[T any](sr *ServiceRegistry, args ...any) (T, error) {
	v, err := getServiceRegistryValue[T](sr, args...)
	if err != nil {
		return zero[T](), err
	}

	return v, nil
}

// MustGet retrieves a service instance of type T from the ServiceRegistry.
// It panics if the service is not registered or if there is an error generating the value.
// Note: there is no circular dependency check
func MustGet[T any](sr *ServiceRegistry, args ...any) T {
	v, err := getServiceRegistryValue[T](sr, args...)
	if err != nil {
		panic(err) //nolint:forbidigo
	}
	return v
}

// getServiceRegistryValue tries to generate a value for the specific type, using provided type marker.
// If done correctly returns it.
func getServiceRegistryValue[T any](sr *ServiceRegistry, args ...any) (T, error) {
	key := getTypeName[T]()

	entry, ok := sr.providers[key]

	if !ok {
		return zero[T](), fmt.Errorf(`no service value for type %s; call RegisterSingleton or RegisterInstance first`, getTypeName[T]())
	}

	if err := prepareValue[T](entry, sr, args...); err != nil {
		return zero[T](), err
	}

	return entry.value.(T), nil // nolint:revive
}

func assignValue[T any](s *serviceEntry, sr *ServiceRegistry, args ...any) error {

	v, err := generateValue[T](s, sr, args...)
	if err != nil {
		return err
	}
	sr.mu.Lock()
	s.value = v
	sr.mu.Unlock()
	return nil
}

// prepareValue prepares the value for the service entry based on its strategy.
func prepareValue[T any](s *serviceEntry, sr *ServiceRegistry, args ...any) error {
	switch s.strategy {
	case STRATEGY_SINGLETON:
		var err error
		s.once.Do(func() {
			err = assignValue[T](s, sr, args...)
		})
		return err
	case STRATEGY_TRANSIENT:
		return assignValue[T](s, sr, args...)
	case STRATEGY_INSTANCE:
		return nil
	}

	return fmt.Errorf("unknown strategy: %s", s.strategy)
}

func generateValue[T any](s *serviceEntry, sr *ServiceRegistry, args ...any) (any, error) {
	if len(args) == 0 {
		// If no extra arguments, we try calling generatorFunc(sl)  first
		if noArgFunc, ok := s.factoryFunc.(func(registry *ServiceRegistry) (T, error)); ok {
			return noArgFunc(sr)
		}
		// Or if the stored function is variadic, calling it with 0 extra args is also valid
		if variadicFunc, ok := s.factoryFunc.(func(registry *ServiceRegistry, args ...any) (T, error)); ok {
			return variadicFunc(sr, args...)
		}
	} else {
		// If extra arguments are provided, we need a variadic function
		if fn, ok := s.factoryFunc.(func(registry *ServiceRegistry, args ...any) (T, error)); ok {
			return fn(sr, args...)
		}
	}

	funcType := reflect.TypeOf(s.factoryFunc)
	return nil, fmt.Errorf("no valid service value generator. Generator signature: %s", funcType)
}

// getTypeName return the name of a type (even if it is an interface type)
func getTypeName[T any]() string {
	var zero T
	// alternative way to get the type name, but it is not as fast as using reflect.TypeOf
	// return fmt.Sprintf(`%T`, &zero)[1:]
	return reflect.TypeOf(zero).String()
}
