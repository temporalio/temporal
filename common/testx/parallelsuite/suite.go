package parallelsuite

import (
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	testifysuite "github.com/stretchr/testify/suite"
)

// TestingSuite is the constraint for suite types.
type TestingSuite interface {
	testifysuite.TestingSuite
	Scope
	copySuite(t *testing.T, parallel bool, assertT require.TestingT) TestingSuite
	initSuite(t *testing.T, parallel bool, assertT require.TestingT)
}

// Extension adds methods to a suite through an exported, embedded struct field.
// Init binds each fresh extension instance to its owning suite, after the base
// test handle and assertions are initialized. Extensions are initialized in field order.
type Extension[T TestingSuite] interface {
	Init(T)
}

// Scope provides the test handle and assertions for a suite instance.
type Scope interface {
	TB() testing.TB
	Require() *require.Assertions
	AssertionT() require.TestingT
}

// Suite provides parallel test execution with require-style (fail-fast) assertions.
//
// It enforces a strict rule: a test method (or subtest) must either use assertions
// directly OR create subtests via Run — not both.
type Suite[T TestingSuite] struct {
	testifyBase
	*require.Assertions

	guardT      guardT
	runParallel bool
	assertT     require.TestingT
}

// copySuite creates a fresh suite instance initialized for the given *testing.T.
// assertT overrides which TestingT assertions are bound to; nil means use the copy's own guardT.
func (s *Suite[T]) copySuite(t *testing.T, parallel bool, assertT require.TestingT) TestingSuite {
	cp := reflect.New(reflect.TypeFor[T]().Elem()).Interface().(T)
	initialize(t, cp, parallel, assertT)
	return cp
}

func (s *Suite[T]) initSuite(t *testing.T, parallel bool, assertT require.TestingT) {
	g := &s.guardT
	g.name = t.Name()
	g.T = t
	g.hasSubtests.Store(false)
	s.runParallel = parallel
	if s.runParallel {
		t.Parallel() //nolint:testifylint // parallelsuite intentionally supports parallel tests
	}
	if assertT == nil {
		assertT = g
	}
	s.assertT = assertT
	s.Assertions = require.New(assertT)
}

// T returns the *testing.T, panicking if the guard has been sealed.
func (s *Suite[T]) T() *testing.T {
	if s.guardT.hasSubtests.Load() {
		panic("parallelsuite: do not call T() after Run(); use the subtest callback's parameter instead")
	}
	return s.guardT.T
}

// TB returns the underlying test handle.
func (s *Suite[T]) TB() testing.TB {
	return s.T()
}

// Require returns assertions bound to the active test or await attempt.
func (s *Suite[T]) Require() *require.Assertions {
	return s.Assertions
}

// AssertionT returns the active assertion target.
func (s *Suite[T]) AssertionT() require.TestingT {
	return s.assertT
}

// Run creates a parallel subtest. The callback receives a fresh copy of the
// concrete suite type, initialized for the subtest's *testing.T.
func (s *Suite[T]) Run(name string, fn func(T)) bool {
	return s.run(name, s.runParallel, fn)
}

// RunSequential creates a serialized subtest. Use it when the subtests
// intentionally share state and/or ordering between tests matters.
func (s *Suite[T]) RunSequential(name string, fn func(T)) bool {
	return s.run(name, false, fn)
}

func (s *Suite[T]) run(name string, parallel bool, fn func(T)) bool {
	pt := s.guardT.T // grab T before sealing
	s.guardT.markHasSubtests()
	return pt.Run(name, func(t *testing.T) {
		fn(s.copySuite(t, parallel, nil).(T))
	})
}

// Run discovers and runs all exported Test* methods on the given suite in parallel.
//
// Each method gets its own fresh suite instance initialized for the subtest's
// *testing.T. Both the suite-level test and each method subtest are marked as
// parallel. Any sequential setup must happen before calling Run.
//
// The suite must embed [Suite]. Additional fields must be exported, embedded
// structs implementing [Extension]; other state must be passed as extra args to Run.
func Run[T TestingSuite](t *testing.T, s T, args ...any) {
	run(t, s, true, args...)
}

// RunLegacySequential behaves like [Run] but does not mark test methods as parallel.
//
// Deprecated: use [Run] for new tests. This only exists for backwards-compatibility
// with legacy behavior to ease migration.
func RunLegacySequential[T TestingSuite](t *testing.T, s T, args ...any) {
	run(t, s, false, args...)
}

func run[T TestingSuite](t *testing.T, s T, methodsParallel bool, args ...any) {
	t.Helper()

	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("parallelsuite.Run: suite must be a pointer to a struct, got %v", typ))
	}
	structType := typ.Elem()

	validateSuiteStruct[T](structType)

	methods := discoverTestMethods(typ, structType, args)
	if len(methods) == 0 {
		panic(fmt.Sprintf("parallelsuite.Run: suite %s has no Test* methods", structType.Name()))
	}

	methods = applyTestifyMFilter(methods)
	if len(methods) == 0 {
		return // all methods filtered by -testify.m; nothing to run
	}

	argVals := make([]reflect.Value, len(args))
	for i, a := range args {
		argVals[i] = reflect.ValueOf(a)
	}

	initialize(t, s, true, nil)

	for _, method := range methods {
		t.Run(method.Name, func(t *testing.T) {
			cpS := s.copySuite(t, methodsParallel, nil)
			callArgs := append([]reflect.Value{reflect.ValueOf(cpS)}, argVals...)
			method.Func.Call(callArgs)
		})
	}
}

// WithAssertions creates a fresh suite for the same test with assertions bound to target.
// Embedded extensions are initialized for the new instance, as with a subtest.
func WithAssertions[T TestingSuite](suite T, target require.TestingT) T {
	return suite.copySuite(suite.T(), false, target).(T)
}

func initialize[T TestingSuite](t *testing.T, suite T, parallel bool, target require.TestingT) {
	suite.initSuite(t, parallel, target)
	value := reflect.ValueOf(suite).Elem()
	for _, field := range value.Fields() {
		if extension, ok := field.Addr().Interface().(Extension[T]); ok {
			extension.Init(suite)
		}
	}
}

func validateSuiteStruct[T TestingSuite](structType reflect.Type) {
	if !strings.HasSuffix(structType.Name(), "Suite") {
		panic(fmt.Sprintf("parallelsuite.Run: struct name %q must end with \"Suite\"", structType.Name()))
	}
	for field := range structType.Fields() {
		if !field.Anonymous || field.PkgPath != "" || field.Type.Kind() != reflect.Struct {
			panic(fmt.Sprintf("parallelsuite.Run: suite %s field %s must be an exported, embedded struct", structType.Name(), field.Name))
		}
		fieldType := reflect.PointerTo(field.Type)
		if !fieldType.Implements(reflect.TypeFor[TestingSuite]()) && !fieldType.Implements(reflect.TypeFor[Extension[T]]()) {
			panic(fmt.Sprintf("parallelsuite.Run: suite %s field %s must embed Suite or implement Extension", structType.Name(), field.Name))
		}
	}
}

// applyTestifyMFilter filters methods by the -testify.m flag. This is helpful
// for editor integrations like VSCode that take this suite for a testify suite.
//
// The flag is registered by testify's suite package (imported above); we share
// that registration via flag.Lookup rather than registering it a second time.
func applyTestifyMFilter(methods []reflect.Method) []reflect.Method {
	f := flag.Lookup("testify.m")
	if f == nil {
		return methods
	}
	pattern := f.Value.String()
	if pattern == "" {
		return methods
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		panic(fmt.Sprintf("parallelsuite: invalid regexp for -testify.m: %s", err))
	}
	var filtered []reflect.Method
	for _, m := range methods {
		if re.MatchString(m.Name) {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

func discoverTestMethods(ptrType, structType reflect.Type, args []any) []reflect.Method {
	expectedNumIn := 1 + len(args)
	inheritedMethods := make(map[string]bool)
	for field := range structType.Fields() {
		fieldType := field.Type
		for method := range reflect.PointerTo(fieldType).Methods() {
			inheritedMethods[method.Name] = true
		}
	}

	for method := range ptrType.Methods() {
		name := method.Name
		if !strings.HasPrefix(name, "Test") && !inheritedMethods[name] {
			panic(fmt.Sprintf(
				"parallelsuite.Run: suite %s has exported method %s that does not start with Test; "+
					"use a package-level function instead",
				structType.Name(), name,
			))
		}
	}

	var methods []reflect.Method
	for method := range ptrType.Methods() {
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
