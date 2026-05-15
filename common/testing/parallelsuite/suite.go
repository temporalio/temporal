package parallelsuite

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	testifysuite "github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
)

// testingSuite is the constraint for suite types.
type testingSuite interface {
	testifysuite.TestingSuite
	//nolint:revive // ctx is last so callers can pass nil to mean "no override"; SA1012 forbids passing nil as the first ctx arg.
	copySuite(t *testing.T, assertT require.TestingT, ctx context.Context) testingSuite
	//nolint:revive // see copySuite above.
	initSuite(t *testing.T, assertT require.TestingT, ctx context.Context)
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

	guardT  guardT
	ctx     context.Context // override set in initSuite; lazy-filled by Context() under ctxOnce when nil
	ctxOnce sync.Once
}

// copySuite creates a fresh suite instance initialized for the given *testing.T.
// assertT overrides which TestingT assertions are bound to; nil means use the copy's own guardT.
// ctx overrides the suite's context; nil means use the default (lazy testcontext.New).
//
//nolint:revive // ctx is last so callers can pass nil to mean "no override"; SA1012 forbids passing nil as the first ctx arg.
func (s *Suite[T]) copySuite(t *testing.T, assertT require.TestingT, ctx context.Context) testingSuite {
	cp := reflect.New(reflect.TypeFor[T]().Elem()).Interface().(T)
	cp.initSuite(t, assertT, ctx)
	return cp
}

//nolint:revive // see copySuite above.
func (s *Suite[T]) initSuite(t *testing.T, assertT require.TestingT, ctx context.Context) {
	g := &s.guardT
	g.name = t.Name()
	g.T = t
	s.ctx = ctx
	if assertT == nil {
		assertT = g
	}
	s.Assertions = require.New(assertT)
	s.ProtoAssertions = protorequire.New(assertT)
	s.HistoryRequire = historyrequire.New(assertT)
}

// T returns the *testing.T, panicking if the guard has been sealed.
func (s *Suite[T]) T() *testing.T {
	if s.guardT.hasSubtests.Load() {
		panic("parallelsuite: do not call T() after Run(); use the subtest callback's parameter instead")
	}
	return s.guardT.T
}

// Context returns the test-scoped context (created from [testcontext]).
// Inside an [Await] callback, it returns the await-scoped context.
func (s *Suite[T]) Context() context.Context {
	s.ctxOnce.Do(func() {
		if s.ctx == nil {
			s.ctx = testcontext.New(s.T())
		}
	})
	return s.ctx
}

// Run creates a parallel subtest. The callback receives a fresh copy of the
// concrete suite type, initialized for the subtest's *testing.T.
func (s *Suite[T]) Run(name string, fn func(T)) bool {
	pt := s.guardT.T // grab T before sealing
	s.guardT.markHasSubtests()
	return pt.Run(name, func(t *testing.T) {
		t.Parallel() //nolint:testifylint // parallelsuite intentionally supports parallel subtests
		fn(s.copySuite(t, nil, nil).(T))
	})
}

// Await calls fn repeatedly until all assertions pass or timeout is reached.
func (s *Suite[T]) Await(fn func(T), timeout, interval time.Duration) {
	s.Awaitf(fn, timeout, interval, "")
}

// Awaitf is like [Await] but includes a format string appended to the failure message.
func (s *Suite[T]) Awaitf(fn func(T), timeout, interval time.Duration, msg string, args ...any) {
	t := s.T()
	await.Requiref(s.Context(), t, func(at *await.T) {
		fn(s.copySuite(t, at, at.Context()).(T))
	}, timeout, interval, msg, args...)
}

// AwaitTrue calls fn repeatedly until it returns true or timeout is reached.
//
// Use it for simple local predicates only. Do not use assertions or side effects; use [Await] instead.
func (s *Suite[T]) AwaitTrue(fn func() bool, timeout, interval time.Duration) {
	s.AwaitTruef(fn, timeout, interval, "")
}

// AwaitTruef is like [AwaitTrue] but includes a format string appended to the failure message.
func (s *Suite[T]) AwaitTruef(fn func() bool, timeout, interval time.Duration, msg string, args ...any) {
	await.RequireTruef(s.T(), fn, timeout, interval, msg, args...)
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

	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("parallelsuite.Run: suite must be a pointer to a struct, got %v", typ))
	}
	structType := typ.Elem()

	validateSuiteStruct(structType)

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

	t.Parallel()

	for _, method := range methods {
		t.Run(method.Name, func(t *testing.T) {
			t.Parallel()

			cpS := s.copySuite(t, nil, nil)
			callArgs := append([]reflect.Value{reflect.ValueOf(cpS)}, argVals...)
			method.Func.Call(callArgs)
		})
	}
}

var inheritedMethods map[string]bool

func init() {
	type ds struct{ Suite[*ds] }
	ptrType := reflect.TypeFor[*ds]()
	inheritedMethods = make(map[string]bool, ptrType.NumMethod())
	for method := range ptrType.Methods() {
		inheritedMethods[method.Name] = true
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
		method := method
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
