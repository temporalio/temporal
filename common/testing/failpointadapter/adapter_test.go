package failpointadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/faultinjection"
	"go.temporal.io/server/common/testing/failpointadapter"
	_ "go.temporal.io/server/common/testing/failpointerrors"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

const operation = "/test.Service/Method"

func newAdapter(registry *failpoint.Registry, namespaces failpointadapter.Namespaces) *failpointadapter.Adapter[any, any] {
	return failpointadapter.NewAdapter[any, any](registry, failpointadapter.Keys{
		Request:     serverfailpoints.GRPCKey,
		Response:    serverfailpoints.GRPCResponseKey,
		AnyRequest:  serverfailpoints.AnyGRPCKey,
		AnyResponse: serverfailpoints.AnyGRPCResponseKey,
	}, namespaces)
}

func setReturn(registry *failpoint.Registry, key failpoint.Key[error], err error) {
	fp := failpoint.NewFailpoint(key)
	fp.Return(err)
	registry.Set(fp)
}

func setBuggify(t *testing.T, registry *failpoint.Registry, config buggify.Config) *buggify.Buggify {
	t.Helper()
	b := buggify.New(registry)
	require.NoError(t, b.Set(config))
	return b
}

// generateRequest returns the error that adapter generates for a request, ignoring the response.
func generateRequest(adapter *failpointadapter.Adapter[any, any], operation string, req any) error {
	_, err := adapter.GenerateRequest(context.Background(), operation, req)
	return err
}

// generateResponse is generateRequest for the response of a handler.
func generateResponse(adapter *failpointadapter.Adapter[any, any], operation string, req, resp any, handlerErr error) error {
	_, err := adapter.GenerateResponse(context.Background(), operation, req, resp, handlerErr)
	return err
}

func TestAdapter_NoRegistry(t *testing.T) {
	t.Parallel()

	adapter := newAdapter(nil, failpointadapter.Namespaces{})
	require.NoError(t, generateRequest(adapter, operation, "request"))
	require.NoError(t, generateResponse(adapter, operation, "request", "response", nil))
}

func TestAdapter_Keys(t *testing.T) {
	t.Parallel()

	fromOperation := errors.New("operation")
	fromAny := errors.New("any")
	withID := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	withName := &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"}
	resolve := func(name namespace.Name) (namespace.ID, bool) {
		if name == "namespace-name" {
			return "namespace-id", true
		}
		return "", false
	}

	for _, stage := range []struct {
		name         string
		operationKey failpoint.Key[error]
		anyKey       failpoint.Key[error]
		generate     func(*failpointadapter.Adapter[any, any], any) error
	}{
		{
			name:         "request",
			operationKey: serverfailpoints.GRPCKey(operation),
			anyKey:       serverfailpoints.AnyGRPCKey,
			generate: func(a *failpointadapter.Adapter[any, any], req any) error {
				return generateRequest(a, operation, req)
			},
		},
		{
			name:         "response",
			operationKey: serverfailpoints.GRPCResponseKey(operation),
			anyKey:       serverfailpoints.AnyGRPCResponseKey,
			generate: func(a *failpointadapter.Adapter[any, any], req any) error {
				return generateResponse(a, operation, req, "response", nil)
			},
		},
	} {
		for _, tc := range []struct {
			name       string
			registered map[failpoint.Key[error]]error
			request    any
			namespaces failpointadapter.Namespaces
			want       error
		}{
			{
				name:       "other key",
				registered: map[failpoint.Key[error]]error{serverfailpoints.GRPCKey("/test.Service/Other"): fromOperation},
				request:    withID,
				want:       nil,
			},
			{
				name:       "operation scoped to namespace ID",
				registered: map[failpoint.Key[error]]error{stage.operationKey.ForScope("namespace-id"): fromOperation},
				request:    withID,
				want:       fromOperation,
			},
			{
				name:       "operation scoped to other namespace",
				registered: map[failpoint.Key[error]]error{stage.operationKey.ForScope("other"): fromOperation},
				request:    withID,
				want:       nil,
			},
			{
				name:       "unscoped operation",
				registered: map[failpoint.Key[error]]error{stage.operationKey: fromOperation},
				request:    withID,
				want:       fromOperation,
			},
			{
				name:       "any operation",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope("namespace-id"): fromAny},
				request:    withID,
				want:       fromAny,
			},
			{
				name: "operation before any operation",
				registered: map[failpoint.Key[error]]error{
					stage.operationKey.ForScope("namespace-id"): fromOperation,
					stage.anyKey.ForScope("namespace-id"):       fromAny,
				},
				request: withID,
				want:    fromOperation,
			},
			{
				name: "operation that fires without error skips any operation",
				registered: map[failpoint.Key[error]]error{
					stage.operationKey: nil,
					stage.anyKey:       fromAny,
				},
				request: withID,
				want:    nil,
			},
			{
				name:       "namespace name resolved",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope("namespace-id"): fromAny},
				request:    withName,
				namespaces: failpointadapter.Namespaces{ID: resolve},
				want:       fromAny,
			},
			{
				name:       "namespace name without resolver",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope("namespace-id"): fromAny},
				request:    withName,
				want:       nil,
			},
			{
				name:       "namespace name without resolver fires for the name scope",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope(failpointadapter.NameScope("namespace-name")): fromAny},
				request:    withName,
				want:       fromAny,
			},
			{
				name:       "unresolved namespace name fires for the name scope",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope(failpointadapter.NameScope("unknown")): fromAny},
				request:    &workflowservice.StartWorkflowExecutionRequest{Namespace: "unknown"},
				namespaces: failpointadapter.Namespaces{ID: resolve},
				want:       fromAny,
			},
			{
				name:       "resolved namespace name fires for the name scope",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope(failpointadapter.NameScope("namespace-name")): fromAny},
				request:    withName,
				namespaces: failpointadapter.Namespaces{ID: resolve},
				want:       fromAny,
			},
			{
				name: "the ID scope takes precedence over the name scope",
				registered: map[failpoint.Key[error]]error{
					stage.operationKey.ForScope("namespace-id"):                               fromOperation,
					stage.operationKey.ForScope(failpointadapter.NameScope("namespace-name")): fromAny,
				},
				request:    withName,
				namespaces: failpointadapter.Namespaces{ID: resolve},
				want:       fromOperation,
			},
			{
				name:       "the name scope fires for a request with the ID, if the name resolves",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope(failpointadapter.NameScope("namespace-name")): fromAny},
				request:    withID,
				namespaces: failpointadapter.Namespaces{Name: func(id namespace.ID) (namespace.Name, bool) { return "namespace-name", id == "namespace-id" }},
				want:       fromAny,
			},
			{
				name:       "the name scope doesn't fire for a request with the ID, without the name",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope(failpointadapter.NameScope("namespace-name")): fromAny},
				request:    withID,
				want:       nil,
			},
			{
				name:       "the name scope of another namespace doesn't fire",
				registered: map[failpoint.Key[error]]error{stage.anyKey.ForScope(failpointadapter.NameScope("other")): fromAny},
				request:    withName,
				namespaces: failpointadapter.Namespaces{ID: resolve},
				want:       nil,
			},
			{
				name:       "unresolved namespace name fires unscoped",
				registered: map[failpoint.Key[error]]error{stage.anyKey: fromAny},
				request:    &workflowservice.StartWorkflowExecutionRequest{Namespace: "unknown"},
				namespaces: failpointadapter.Namespaces{ID: resolve},
				want:       fromAny,
			},
		} {
			t.Run(stage.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				registry := failpoint.NewRegistry()
				for key, err := range tc.registered {
					setReturn(registry, key, err)
				}
				adapter := newAdapter(registry, tc.namespaces)

				require.Equal(t, tc.want, stage.generate(adapter, tc.request))
			})
		}
	}
}

func TestAdapter_CallbackArguments(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	handlerErr := errors.New("handler")
	var args []any
	for _, key := range []failpoint.Key[error]{serverfailpoints.AnyGRPCKey, serverfailpoints.AnyGRPCResponseKey} {
		fp := failpoint.NewFailpoint(key)
		fp.CallWithArg(func(arg any) error {
			args = append(args, arg)
			return nil
		})
		registry.Set(fp)
	}
	adapter := newAdapter(registry, failpointadapter.Namespaces{})

	require.NoError(t, generateRequest(adapter, operation, request))
	require.NoError(t, generateResponse(adapter, operation, request, "response", handlerErr))
	require.Equal(t, []any{
		request,
		&faultinjection.Result[any, any]{Operation: operation, Request: request, Response: "response", Error: handlerErr},
	}, args)
}

// replaceable is a request that a fault can answer with a response.
type replaceable struct {
	faultinjection.Replacement[any]
}

func TestAdapter_Replacement(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	for _, tc := range []struct {
		name string
		call func(adapter *failpointadapter.Adapter[any, any]) (any, error)
		key  failpoint.Key[error]
		// replace is the fault: it replaces the response, and returns err.
		replace func(arg any, err error)
	}{
		{
			name: "request",
			key:  serverfailpoints.GRPCKey(operation),
			call: func(adapter *failpointadapter.Adapter[any, any]) (any, error) {
				return adapter.GenerateRequest(context.Background(), operation, &replaceable{})
			},
			replace: func(arg any, _ error) { arg.(*replaceable).Replace("replacement") },
		},
		{
			name: "response",
			key:  serverfailpoints.GRPCResponseKey(operation),
			call: func(adapter *failpointadapter.Adapter[any, any]) (any, error) {
				return adapter.GenerateResponse(context.Background(), operation, "request", "response", nil)
			},
			replace: func(arg any, _ error) { arg.(*faultinjection.Result[any, any]).Replace("replacement") },
		},
	} {
		for _, returned := range []error{nil, injectedErr} {
			t.Run(tc.name+"/returning "+map[bool]string{false: "no error", true: "an error"}[returned != nil], func(t *testing.T) {
				t.Parallel()
				registry := failpoint.NewRegistry()
				fp := failpoint.NewFailpoint(tc.key)
				fp.CallWithArg(func(arg any) error {
					tc.replace(arg, returned)
					return returned
				})
				registry.Set(fp)

				resp, err := tc.call(newAdapter(registry, failpointadapter.Namespaces{}))

				if returned != nil {
					// The error fails the call; the replacement is ignored.
					require.ErrorIs(t, err, injectedErr)
					require.Nil(t, resp)
					return
				}
				require.NoError(t, err)
				require.Equal(t, "replacement", resp)
			})
		}
	}
}

func TestAdapter_NoReplacementWithoutIt(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint(serverfailpoints.GRPCKey(operation))
	fp.CallWithArg(func(any) error { return nil })
	registry.Set(fp)
	adapter := newAdapter(registry, failpointadapter.Namespaces{})

	resp, err := adapter.GenerateRequest(context.Background(), operation, &replaceable{})
	require.NoError(t, err)
	require.Nil(t, resp)

	resp, err = adapter.GenerateRequest(context.Background(), operation, "request")
	require.NoError(t, err)
	require.Nil(t, resp)
}

func TestAdapter_AnyOperationOnlyIfOperationDidNotFire(t *testing.T) {
	t.Parallel()

	fromAny := errors.New("any")
	for _, tc := range []struct {
		name      string
		configure func(*failpoint.Failpoint[error])
		want      error
	}{
		{name: "call returning nil", configure: func(fp *failpoint.Failpoint[error]) {
			fp.CallWithArg(func(any) error { return nil })
		}, want: nil},
		{name: "pause", configure: func(fp *failpoint.Failpoint[error]) { fp.Pause(time.Nanosecond) }, want: nil},
		{name: "on", configure: func(fp *failpoint.Failpoint[error]) { fp.On() }, want: nil},
		{name: "skipped by trigger", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Return(errors.New("operation"))
			_ = fp.SetTrigger(failpoint.Trigger{SkipFirst: 100})
		}, want: fromAny},
		{name: "disabled", configure: func(*failpoint.Failpoint[error]) {}, want: fromAny},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := failpoint.NewRegistry()
			fp := failpoint.NewFailpoint(serverfailpoints.GRPCKey(operation))
			tc.configure(fp)
			registry.Set(fp)
			setReturn(registry, serverfailpoints.AnyGRPCKey, fromAny)

			err := generateRequest(newAdapter(registry, failpointadapter.Namespaces{}), operation, "request")
			require.Equal(t, tc.want, err)
		})
	}
}

func TestAdapter_Buggify(t *testing.T) {
	t.Parallel()

	fromAny := errors.New("any")
	newRegistry := func(t *testing.T) *failpoint.Registry {
		registry := failpoint.NewRegistry()
		// Every execution of the operation's key fires, with an error from the server's catalog.
		setBuggify(t, registry, buggify.Config{Seed: 1, Keys: []buggify.Key{
			{Match: "grpc-response:*"},
			{Match: "*", ActivationProbability: 1, FireProbability: 1, Error: "Unavailable"},
		}})
		return registry
	}

	t.Run("injects an error for an operation with nothing registered", func(t *testing.T) {
		t.Parallel()
		adapter := newAdapter(newRegistry(t), failpointadapter.Namespaces{})
		err := generateRequest(adapter, operation, "request")
		var unavailable *serviceerror.Unavailable
		require.ErrorAs(t, err, &unavailable)
		require.NoError(t, generateResponse(adapter, operation, "request", "response", nil))
	})

	t.Run("a failpoint of any operation takes precedence", func(t *testing.T) {
		t.Parallel()
		registry := newRegistry(t)
		setReturn(registry, serverfailpoints.AnyGRPCKey, fromAny)
		require.Equal(t, fromAny, generateRequest(newAdapter(registry, failpointadapter.Namespaces{}), operation, "request"))
	})

	t.Run("a failpoint of any operation takes precedence even if it does not fire", func(t *testing.T) {
		t.Parallel()
		for name, configure := range map[string]func(*failpoint.Failpoint[error]){
			"disabled": func(*failpoint.Failpoint[error]) {},
			"skipped by trigger": func(fp *failpoint.Failpoint[error]) {
				fp.Return(fromAny)
				_ = fp.SetTrigger(failpoint.Trigger{SkipFirst: 100})
			},
		} {
			registry := newRegistry(t)
			fp := failpoint.NewFailpoint(serverfailpoints.AnyGRPCKey.ForScope("namespace-id"))
			configure(fp)
			registry.Set(fp)
			request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
			require.NoError(t, generateRequest(newAdapter(registry, failpointadapter.Namespaces{}), operation, request), name)
		}
	})

	t.Run("a failpoint of the operation takes precedence even if it does not fire", func(t *testing.T) {
		t.Parallel()
		registry := newRegistry(t)
		registry.Set(failpoint.NewFailpoint(serverfailpoints.GRPCKey(operation)))
		require.NoError(t, generateRequest(newAdapter(registry, failpointadapter.Namespaces{}), operation, "request"))
	})

	t.Run("explicit only keeps BUGGIFY away but not registered failpoints", func(t *testing.T) {
		t.Parallel()
		registry := newRegistry(t)
		adapter := failpointadapter.NewAdapter[any, any](registry, failpointadapter.Keys{
			Request:      serverfailpoints.GRPCKey,
			Response:     serverfailpoints.GRPCResponseKey,
			AnyRequest:   serverfailpoints.AnyGRPCKey,
			AnyResponse:  serverfailpoints.AnyGRPCResponseKey,
			ExplicitOnly: true,
		}, failpointadapter.Namespaces{})
		require.NoError(t, generateRequest(adapter, operation, "request"))

		setReturn(registry, serverfailpoints.AnyGRPCKey, fromAny)
		require.Equal(t, fromAny, generateRequest(adapter, operation, "request"))
	})

	t.Run("follows a new configuration", func(t *testing.T) {
		t.Parallel()
		registry := newRegistry(t)
		b, ok := registry.Policy().(*buggify.Buggify)
		require.True(t, ok)
		adapter := newAdapter(registry, failpointadapter.Namespaces{})
		require.Error(t, generateRequest(adapter, operation, "request"))
		require.NoError(t, b.Set(buggify.Config{Seed: 1, Keys: []buggify.Key{{Match: "*", ActivationProbability: 0, FireProbability: 1, Error: "Unavailable"}}}))
		require.NoError(t, generateRequest(adapter, operation, "request"))
		require.NoError(t, b.Set(buggify.Config{Seed: 2, Keys: []buggify.Key{{Match: "*", ActivationProbability: 1, FireProbability: 1, Error: "Unavailable"}}}))
		require.Error(t, generateRequest(adapter, operation, "request"))
	})
}

func TestAdapter_BuggifyScopeIsTheNamespaceName(t *testing.T) {
	t.Parallel()

	config := buggify.Config{Seed: 3, Keys: []buggify.Key{{Match: "*", ActivationProbability: 1, FireProbability: 0.5, Error: "Unavailable"}}}
	names := map[namespace.ID]namespace.Name{"id-1": "ns", "id-2": "ns", "id-3": "other"}
	namespaces := failpointadapter.Namespaces{
		ID: func(name namespace.Name) (namespace.ID, bool) {
			for id, n := range names {
				if n == name {
					return id, true
				}
			}
			return "", false
		},
		Name: func(id namespace.ID) (namespace.Name, bool) {
			name, ok := names[id]
			return name, ok
		},
	}
	// decisions returns whether each of 40 requests fails, on a registry and adapter of its own.
	decisions := func(t *testing.T, namespaces failpointadapter.Namespaces, request any) []bool {
		registry := failpoint.NewRegistry()
		setBuggify(t, registry, config)
		adapter := newAdapter(registry, namespaces)
		var failed []bool
		for range 40 {
			failed = append(failed, generateRequest(adapter, operation, request) != nil)
		}
		return failed
	}
	byID := func(id string) any { return &matchingservice.AddWorkflowTaskRequest{NamespaceId: id} }

	byName := decisions(t, namespaces, byID("id-1"))
	require.Contains(t, byName, true)
	require.Contains(t, byName, false)

	t.Run("a recreated namespace with the same name decides the same", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, byName, decisions(t, namespaces, byID("id-2")))
		require.NotEqual(t, byName, decisions(t, namespaces, byID("id-3")))
	})

	t.Run("a request with the name decides in the same scope as one with the ID", func(t *testing.T) {
		t.Parallel()
		withName := &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns"}
		require.Equal(t, byName, decisions(t, namespaces, withName))
		// The same without a way to resolve the ID.
		require.Equal(t, byName, decisions(t, failpointadapter.Namespaces{}, withName))
	})

	t.Run("a request with the ID and the name decides in the scope of the name, also on a cache miss", func(t *testing.T) {
		t.Parallel()
		// The frontend sends both fields to matching, whose namespace cache may not know the ID.
		both := &matchingservice.ListTaskQueuePartitionsRequest{NamespaceId: "id-1", Namespace: "ns"}
		require.Equal(t, byName, decisions(t, namespaces, both))
		require.Equal(t, byName, decisions(t, failpointadapter.Namespaces{}, both))
		unresolvable := failpointadapter.Namespaces{Name: func(namespace.ID) (namespace.Name, bool) { return "", false }}
		require.Equal(t, byName, decisions(t, unresolvable, both))
	})

	t.Run("an unresolvable ID decides in the scope of the ID", func(t *testing.T) {
		t.Parallel()
		unresolvable := failpointadapter.Namespaces{Name: func(namespace.ID) (namespace.Name, bool) { return "", false }}
		require.Equal(t, decisions(t, failpointadapter.Namespaces{}, byID("id-1")), decisions(t, unresolvable, byID("id-1")))
	})

	t.Run("failpoints still match the ID, not the name", func(t *testing.T) {
		t.Parallel()
		fromOperation := errors.New("operation")
		registry := failpoint.NewRegistry()
		setBuggify(t, registry, config)
		setReturn(registry, serverfailpoints.GRPCKey(operation).ForScope("id-1"), fromOperation)
		adapter := newAdapter(registry, namespaces)
		require.Equal(t, fromOperation, generateRequest(adapter, operation, byID("id-1")))
		// A failpoint of the ID doesn't apply to a namespace with the same name and another ID, so
		// BUGGIFY decides for it, and a failpoint of the name doesn't apply to the ID.
		failed := 0
		for range 40 {
			err := generateRequest(adapter, operation, byID("id-2"))
			require.NotEqual(t, fromOperation, err)
			if err != nil {
				failed++
			}
		}
		require.NotZero(t, failed)
		setReturn(registry, serverfailpoints.GRPCKey(operation).ForScope("ns"), fromOperation)
		require.NotEqual(t, fromOperation, generateRequest(adapter, operation, byID("id-3")))
	})

	t.Run("the name is only looked up while the registry may fire", func(t *testing.T) {
		t.Parallel()
		lookups := 0
		counting := failpointadapter.Namespaces{Name: func(namespace.ID) (namespace.Name, bool) {
			lookups++
			return "ns", true
		}}
		// No registry, and a registry that may not fire: no lookups.
		require.NoError(t, generateRequest(newAdapter(nil, counting), operation, byID("id-1")))
		require.NoError(t, generateRequest(newAdapter(failpoint.NewRegistry(), counting), operation, byID("id-1")))
		require.Zero(t, lookups)

		registry := failpoint.NewRegistry()
		setBuggify(t, registry, config)
		_ = generateRequest(newAdapter(registry, counting), operation, byID("id-1"))
		require.Positive(t, lookups)
	})
}

// A rule update that replaces the failpoint of the ID of a namespace by one of its name, or the
// other way round, doesn't let a request miss both and reach the failpoint without a scope.
func TestAdapter_ReplacedScopeIsNotMissed(t *testing.T) {
	t.Parallel()

	injected, unscoped := errors.New("scoped"), errors.New("unscoped")
	namespaces := failpointadapter.Namespaces{
		ID:   func(namespace.Name) (namespace.ID, bool) { return "id-1", true },
		Name: func(namespace.ID) (namespace.Name, bool) { return "ns", true },
	}
	key := serverfailpoints.GRPCKey(operation)
	byID, byName := key.ForScope("id-1"), key.ForScope(failpointadapter.NameScope("ns"))
	registry := failpoint.NewRegistry()
	setReturn(registry, key, unscoped)
	setReturn(registry, byID, injected)
	adapter := newAdapter(registry, namespaces)

	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// The new failpoint is registered before the old one is removed.
			registry.Atomically(func() {
				setReturn(registry, byName, injected)
				registry.Delete(byID)
			})
			registry.Atomically(func() {
				setReturn(registry, byID, injected)
				registry.Delete(byName)
			})
		}
	}()
	for range 2000 {
		require.Equal(t, injected, generateRequest(adapter, operation, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns"}))
		require.Equal(t, injected, generateRequest(adapter, operation, &matchingservice.AddWorkflowTaskRequest{NamespaceId: "id-1"}))
	}
	close(stop)
	<-done
}
