package parallelsuite

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	testifysuite "github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
)

// testingSuite is the constraint for suite types.
type testingSuite interface {
	testifysuite.TestingSuite
	copySuite(t *testing.T) testingSuite
	initSuite(t *testing.T)
}

// Suite provides parallel test execution with require-style (fail-fast) assertions.
//
// It enforces a strict rule: a test method (or subtest) must either use assertions
// directly OR create subtests via Run — not both.
type Suite[T testingSuite] struct {
	testifyBase
	*require.Assertions
	protorequire.ProtoAssertions
	historyrequire.HistoryRequire

	guardT guardT
}

// copySuite creates a fresh suite instance initialized for the given *testing.T.
func (s *Suite[T]) copySuite(t *testing.T) testingSuite {
	cp := reflect.New(reflect.TypeFor[T]().Elem()).Interface().(T)
	cp.initSuite(t)
	return cp
}

func (s *Suite[T]) initSuite(t *testing.T) {
	g := &s.guardT
	g.name = t.Name()
	g.T = t
	g.asserted.Store(false)
	g.hasSubtests.Store(false)
	s.Assertions = require.New(g)
	s.ProtoAssertions = protorequire.New(g)
	s.HistoryRequire = historyrequire.New(g)
}

// T returns the *testing.T, panicking if the guard has been sealed.
func (s *Suite[T]) T() *testing.T {
	if s.guardT.hasSubtests.Load() {
		panic("parallelsuite: do not call T() after Run(); use the subtest callback's parameter instead")
	}
	return s.guardT.T
}

// Run creates a parallel subtest. The callback receives a fresh copy of the
// concrete suite type, initialized for the subtest's *testing.T.
func (s *Suite[T]) Run(name string, fn func(T)) bool {
	pt := s.guardT.T // grab T before sealing
	s.guardT.markHasSubtests()
	return pt.Run(name, func(t *testing.T) {
		t.Parallel() //nolint:testifylint // parallelsuite intentionally supports parallel subtests
		fn(s.copySuite(t).(T))
	})
}

// Run discovers and runs all exported Test* methods on the given suite in parallel.
//
// Each method gets its own fresh suite instance initialized for the subtest's
// *testing.T. Both the suite-level test and each method subtest are marked as
// parallel. Any sequential setup must happen before calling Run.
//
// The suite must embed [Suite] and have no other fields.
func Run[T testingSuite](t *testing.T, s T, args ...any) {
	t.Helper()

	typ := reflect.TypeOf(s)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("parallelsuite.Run: suite must be a pointer to a struct, got %v", typ))
	}
	structType := typ.Elem()

	validateSuiteStruct(structType)

	methods := discoverTestMethods(typ, structType, args)
	if len(methods) == 0 {
		panic(fmt.Sprintf("parallelsuite.Run: suite %s has no Test* methods", structType.Name()))
	}

	argVals := make([]reflect.Value, len(args))
	for i, a := range args {
		argVals[i] = reflect.ValueOf(a)
	}

	t.Parallel()

	for _, method := range methods {
		t.Run(method.Name, func(t *testing.T) {
			t.Parallel()

			cpS := s.copySuite(t)
			callArgs := append([]reflect.Value{reflect.ValueOf(cpS)}, argVals...)
			method.Func.Call(callArgs)
		})
	}
}

var inheritedMethods map[string]bool

func init() {
	type ds struct{ Suite[*ds] }
	ptrType := reflect.TypeOf(&ds{})
	inheritedMethods = make(map[string]bool, ptrType.NumMethod())
	for i := 0; i < ptrType.NumMethod(); i++ {
		inheritedMethods[ptrType.Method(i).Name] = true
	}
}

func validateSuiteStruct(structType reflect.Type) {
	if !strings.HasSuffix(structType.Name(), "Suite") {
		panic(fmt.Sprintf("parallelsuite.Run: struct name %q must end with \"Suite\"", structType.Name()))
	}

	if structType.NumField() != 1 {
		panic(fmt.Sprintf(
			"parallelsuite.Run: suite %s must have no fields besides the embedded parallelsuite.Suite; "+
				"pass parameters as extra args to Run instead (got %d fields)",
			structType.Name(), structType.NumField(),
		))
	}
	f := structType.Field(0)
	if !f.Anonymous {
		panic(fmt.Sprintf(
			"parallelsuite.Run: suite %s must embed parallelsuite.Suite, found named field %q",
			structType.Name(), f.Name,
		))
	}
}

func discoverTestMethods(ptrType, structType reflect.Type, args []any) []reflect.Method {
	expectedNumIn := 1 + len(args)

	for i := 0; i < ptrType.NumMethod(); i++ {
		name := ptrType.Method(i).Name
		if !strings.HasPrefix(name, "Test") && !inheritedMethods[name] {
			panic(fmt.Sprintf(
				"parallelsuite.Run: suite %s has exported method %s that does not start with Test; "+
					"use a package-level function instead",
				structType.Name(), name,
			))
		}
	}

	var methods []reflect.Method
	for i := 0; i < ptrType.NumMethod(); i++ {
		method := ptrType.Method(i)
		if !strings.HasPrefix(method.Name, "Test") {
			continue
		}

		mt := method.Type
		if mt.NumOut() != 0 {
			panic(fmt.Sprintf(
				"parallelsuite.Run: method %s.%s must not have return values, got %v",
				structType.Name(), method.Name, mt,
			))
		}
		if mt.NumIn() != expectedNumIn {
			panic(fmt.Sprintf(
				"parallelsuite.Run: method %s.%s has wrong number of parameters: expected %d, got %d (%v)",
				structType.Name(), method.Name, expectedNumIn, mt.NumIn(), mt,
			))
		}

		for j, a := range args {
			paramType := mt.In(1 + j)
			argType := reflect.TypeOf(a)
			if !argType.AssignableTo(paramType) {
				panic(fmt.Sprintf(
					"parallelsuite.Run: method %s.%s parameter %d has type %v but Run arg has type %v",
					structType.Name(), method.Name, j+1, paramType, argType,
				))
			}
		}

		methods = append(methods, method)
	}
	return methods
}
