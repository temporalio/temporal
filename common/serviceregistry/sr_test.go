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
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type BaseInterface interface {
	Foo() string
}

type BaseImplementation struct {
}

func (b *BaseImplementation) Foo() string {
	return "Foo"
}

type Config struct {
	Foo string
}

type ExampleService struct {
	Bar    string
	Config *Config
}

func NewExampleService(sr *ServiceRegistry) (*ExampleService, error) {
	config, err := Get[*Config](sr)
	if err != nil {
		return nil, err
	}
	return &ExampleService{Bar: "Bar", Config: config}, nil
}

func TestServiceRegistry_Basic(t *testing.T) {
	sr := NewServiceRegistry()
	config := &Config{
		Foo: "Bar",
	}
	value := RegisterInstance[*Config](sr, config)
	assert.NotNil(t, value)

	err := RegisterSingleton[*ExampleService](sr, func(reg *ServiceRegistry) (*ExampleService, error) {
		return NewExampleService(sr)
	})

	assert.NoError(t, err)

	configInstance, err := Get[*Config](sr)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, "Bar", configInstance.Foo)

	exampleService, err := Get[*ExampleService](sr)
	assert.NoError(t, err)
	assert.NotNil(t, exampleService.Bar)
	assert.Equal(t, "Bar", exampleService.Bar)
}

func TestServiceRegistry_RegisterInstance(t *testing.T) {
	originalValue := "Foo"
	updatedValue := "Bar"

	config := &Config{
		Foo: originalValue,
	}
	sr := NewServiceRegistry()
	value := RegisterInstance[*Config](sr, config)
	assert.NotNil(t, value)

	configInstance, err := Get[*Config](sr)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, originalValue, configInstance.Foo)

	config.Foo = updatedValue
	configInstance, err = Get[*Config](sr)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, updatedValue, configInstance.Foo)
}

func TestServiceRegistry_RegisterSingleton(t *testing.T) {
	originalValue := "Foo"
	updatedValue := "Bar"

	config := &Config{Foo: originalValue}
	sr := NewServiceRegistry()

	err := RegisterSingleton[*Config](sr, func(reg *ServiceRegistry) (*Config, error) {
		return config, nil
	})
	assert.NoError(t, err)

	configInstance, err := Get[*Config](sr)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, originalValue, configInstance.Foo)

	config.Foo = updatedValue
	configInstance, err = Get[*Config](sr)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, updatedValue, configInstance.Foo)
}

func TestServiceRegistry_RegisterTransient(t *testing.T) {
	originalValue := "Foo"
	updatedValue := "Bar"

	configValue := originalValue

	sr := NewServiceRegistry()
	err := RegisterTransient[*Config](sr, func(reg *ServiceRegistry, args ...any) (*Config, error) {
		return &Config{Foo: configValue}, nil
	})
	assert.NoError(t, err)

	configInstance, err := Get[*Config](sr)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, originalValue, configInstance.Foo)

	configValue = updatedValue
	configInstance, err = Get[*Config](sr)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, updatedValue, configInstance.Foo)
}

func TestServiceRegistry_Args(t *testing.T) {
	firstValue := "first"
	secondValue := "second"

	sr := NewServiceRegistry()
	err := RegisterTransient[*Config](sr, func(sl *ServiceRegistry, args ...any) (*Config, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		config := &Config{Foo: args[0].(string)}
		return config, nil
	})

	assert.NoError(t, err)

	configInstance, err := Get[*Config](sr, firstValue)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, firstValue, configInstance.Foo)

	configInstance, err = Get[*Config](sr, secondValue)
	assert.NotNil(t, configInstance)
	assert.NoError(t, err)
	assert.Equal(t, secondValue, configInstance.Foo)

}

func TestMustGetInstance(t *testing.T) {
	sr := NewServiceRegistry()
	RegisterInstance[*Config](sr, &Config{Foo: "Foo"})

	config := MustGet[*Config](sr)
	assert.NotNil(t, config)
	assert.Equal(t, "Foo", config.Foo)
}

func TestMustGetInstance_Panic(t *testing.T) {
	sr := NewServiceRegistry()
	assert.Panics(t, func() {
		MustGet[*Config](sr)
	})
}

func TestServiceRegistry_GeneratorValidation_Pass(t *testing.T) {
	testCases := []struct {
		fnValue reflect.Value
	}{
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) (*Config, error) {
				return &Config{Foo: "Foo"}, nil
			}),
		},
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry, foo string) (*Config, error) {
				return &Config{Foo: foo}, nil
			}),
		},
	}

	var err error
	for _, test := range testCases {
		err = validateFunc[*Config](test.fnValue)
		assert.NoError(t, err)
	}

	testCases = []struct {
		fnValue reflect.Value
	}{

		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) (*BaseImplementation, error) {
				return &BaseImplementation{}, nil
			}),
		},
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) (BaseInterface, error) {
				return &BaseImplementation{}, nil
			}),
		},
	}
	for _, test := range testCases {
		err = validateFunc[BaseInterface](test.fnValue)
		assert.NoError(t, err)
	}
}

func TestServiceRegistry_GeneratorValidation_Fail(t *testing.T) {
	testCases := []struct {
		fnValue reflect.Value
		name    string
	}{
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) (any, error) {
				return &Config{Foo: "Foo"}, nil
			}),
			name: "return type is 'any', which is not assignable to the struct pointer",
		},
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) *Config {
				return &Config{Foo: "Foo"}
			}),
			name: "function must return 2 values",
		},
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) (*Config, bool) {
				return &Config{Foo: "Foo"}, false
			}),
			name: "2nd output parameter must be error",
		},
		{
			fnValue: reflect.ValueOf(func() (*Config, error) {
				return &Config{Foo: "Foo"}, nil
			}),
			name: "function must have at least 1 parameter",
		},
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) (string, error) {
				return "hello", nil
			}),
			name: "1st output parameter must be interface or pointer to the struct",
		},
		{
			fnValue: reflect.ValueOf(func(reg *ServiceRegistry) (*ExampleService, error) {
				return &ExampleService{}, nil
			}),
			name: "1st output parameter must be castable to the generic type",
		},
		{
			fnValue: reflect.ValueOf(func(loc string) (*Config, error) {
				return &Config{Foo: "Foo"}, nil
			}),
			name: "function 1st parameter must be ServiceRegistry",
		},
	}

	for _, test := range testCases {
		err := validateFunc[*Config](test.fnValue)
		assert.Error(t, err, test.name)
	}
}

func TestServiceRegistry_Multithreaded(t *testing.T) {
	var wg sync.WaitGroup
	workerCount := 3
	iterationCount := 100

	callCount := 0
	sr := NewServiceRegistry()

	wg.Add(workerCount)

	err := RegisterSingleton[*Config](sr, func(reg *ServiceRegistry) (*Config, error) {
		callCount++
		return &Config{Foo: fmt.Sprintf("Instance_%d", callCount)}, nil
	})
	assert.NoError(t, err)

	expectedValue := "Instance_1"

	for i := 1; i <= workerCount; i++ {
		go func() {
			defer wg.Done()

			for j := 1; j <= iterationCount; j++ {
				config, err := Get[*Config](sr)
				assert.NoError(t, err)
				assert.NotNil(t, config)
				assert.Equal(t, expectedValue, config.Foo)
			}
		}()
	}

	wg.Wait()
}

// Example for RegisterInstance / STRATEGY_INSTANCE
func Example_registerInstance() {
	type Foo struct{ Name string }

	sr := NewServiceRegistry()
	// directly register a concrete instance:
	RegisterInstance(sr, &Foo{Name: "temporal"})

	foo, err := Get[*Foo](sr)
	if err != nil {
		fmt.Println("ERROR:", err)
		return
	}
	fmt.Println(foo.Name)
	// Output:
	// temporal
}

// Example for RegisterSingleton / STRATEGY_SINGLETON
func Example_registerSingleton() {
	type Counter struct{ N int }

	sr := NewServiceRegistry()
	err := RegisterSingleton[*Counter](sr, func(_ *ServiceRegistry) (*Counter, error) {
		// this factory runs exactly once:
		return &Counter{N: 42}, nil
	})
	if err != nil {
		fmt.Println("ERROR:", err)
		return
	}

	c1, _ := Get[*Counter](sr)
	c2, _ := Get[*Counter](sr)
	fmt.Println(c1 == c2, c1.N)
	// Output:
	// true 42
}

// Example for RegisterTransient / STRATEGY_TRANSIENT
func Example_registerTransient() {
	type Greeter struct{ Msg string }

	sr := NewServiceRegistry()
	err := RegisterTransient[*Greeter](sr, func(_ *ServiceRegistry, args ...any) (*Greeter, error) {
		// each call makes a fresh Greeter from args[0]
		return &Greeter{Msg: args[0].(string)}, nil
	})
	if err != nil {
		fmt.Println("ERROR:", err)
		return
	}

	g1, _ := Get[*Greeter](sr, "hello")
	g2, _ := Get[*Greeter](sr, "world")
	fmt.Println(g1.Msg, g2.Msg)
	// Output:
	// hello world
}
