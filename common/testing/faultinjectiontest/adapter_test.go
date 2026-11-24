package faultinjectiontest_test

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
	_ "go.temporal.io/server/common/testing/failpointerrors"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

const operation = "/test.Service/Method"

type registrySource struct {
	registry failpoint.Registry
}

func (s registrySource) Failpoints() failpoint.Registry { return s.registry }

func newAdapter(registry failpoint.Registry, namespaces faultinjectiontest.NamespaceIDs) *faultinjectiontest.Adapter[any, any] {
	return faultinjectiontest.NewAdapter[any, any](registrySource{registry: registry}, faultinjectiontest.Keys{
		Request:     failpoint.GRPCKey,
		Response:    failpoint.GRPCResponseKey,
		AnyRequest:  failpoint.AnyGRPCKey,
		AnyResponse: failpoint.AnyGRPCResponseKey,
	}, namespaces)
}

func setReturn(registry failpoint.Registry, key failpoint.Key, err error) {
	fp := failpoint.NewFailpoint[error](key)
	fp.Return(err, failpoint.Always)
	registry.Set(key, fp)
}

func TestAdapter_NoRegistry(t *testing.T) {
	t.Parallel()

	adapter := newAdapter(nil, nil)
	require.NoError(t, adapter.GenerateRequest(context.Background(), operation, "request"))
	require.NoError(t, adapter.GenerateResponse(context.Background(), operation, "request", "response", nil))
}

func TestAdapter_Keys(t *testing.T) {
	t.Parallel()

	fromOperation := errors.New("operation")
	fromAny := errors.New("any")
	withID := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	withName := &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"}
	resolve := func(name namespace.Name) namespace.ID {
		if name == "namespace-name" {
			return "namespace-id"
		}
		return ""
	}

	for _, stage := range []struct {
		name         string
		operationKey failpoint.Key
		anyKey       failpoint.Key
		generate     func(*faultinjectiontest.Adapter[any, any], any) error
	}{
		{
			name:         "request",
			operationKey: failpoint.GRPCKey(operation),
			anyKey:       failpoint.AnyGRPCKey,
			generate: func(a *faultinjectiontest.Adapter[any, any], req any) error {
				return a.GenerateRequest(context.Background(), operation, req)
			},
		},
		{
			name:         "response",
			operationKey: failpoint.GRPCResponseKey(operation),
			anyKey:       failpoint.AnyGRPCResponseKey,
			generate: func(a *faultinjectiontest.Adapter[any, any], req any) error {
				return a.GenerateResponse(context.Background(), operation, req, "response", nil)
			},
		},
	} {
		for _, tc := range []struct {
			name       string
			registered map[failpoint.Key]error
			request    any
			namespaces faultinjectiontest.NamespaceIDs
			want       error
		}{
			{
				name:       "other key",
				registered: map[failpoint.Key]error{failpoint.GRPCKey("/test.Service/Other"): fromOperation},
				request:    withID,
				want:       nil,
			},
			{
				name:       "operation scoped to namespace ID",
				registered: map[failpoint.Key]error{stage.operationKey.ForScope("namespace-id"): fromOperation},
				request:    withID,
				want:       fromOperation,
			},
			{
				name:       "operation scoped to other namespace",
				registered: map[failpoint.Key]error{stage.operationKey.ForScope("other"): fromOperation},
				request:    withID,
				want:       nil,
			},
			{
				name:       "unscoped operation",
				registered: map[failpoint.Key]error{stage.operationKey: fromOperation},
				request:    withID,
				want:       fromOperation,
			},
			{
				name:       "any operation",
				registered: map[failpoint.Key]error{stage.anyKey.ForScope("namespace-id"): fromAny},
				request:    withID,
				want:       fromAny,
			},
			{
				name: "operation before any operation",
				registered: map[failpoint.Key]error{
					stage.operationKey.ForScope("namespace-id"): fromOperation,
					stage.anyKey.ForScope("namespace-id"):       fromAny,
				},
				request: withID,
				want:    fromOperation,
			},
			{
				name: "operation that fires without error skips any operation",
				registered: map[failpoint.Key]error{
					stage.operationKey: nil,
					stage.anyKey:       fromAny,
				},
				request: withID,
				want:    nil,
			},
			{
				name:       "namespace name resolved",
				registered: map[failpoint.Key]error{stage.anyKey.ForScope("namespace-id"): fromAny},
				request:    withName,
				namespaces: resolve,
				want:       fromAny,
			},
			{
				name:       "namespace name without resolver",
				registered: map[failpoint.Key]error{stage.anyKey.ForScope("namespace-id"): fromAny},
				request:    withName,
				want:       nil,
			},
			{
				name:       "unresolved namespace name fires unscoped",
				registered: map[failpoint.Key]error{stage.anyKey: fromAny},
				request:    &workflowservice.StartWorkflowExecutionRequest{Namespace: "unknown"},
				namespaces: resolve,
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

				// Twice, to use the cached keys of the operation.
				for range 2 {
					require.Equal(t, tc.want, stage.generate(adapter, tc.request))
				}
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
	for _, key := range []failpoint.Key{failpoint.AnyGRPCKey, failpoint.AnyGRPCResponseKey} {
		fp := failpoint.NewFailpoint[error](key)
		fp.CallWith(func(arg any) error {
			args = append(args, arg)
			return nil
		}, failpoint.Always)
		registry.Set(key, fp)
	}
	adapter := newAdapter(registry, nil)

	require.NoError(t, adapter.GenerateRequest(context.Background(), operation, request))
	require.NoError(t, adapter.GenerateResponse(context.Background(), operation, request, "response", handlerErr))
	require.Equal(t, []any{
		request,
		faultinjection.Result[any, any]{Operation: operation, Request: request, Response: "response", Error: handlerErr},
	}, args)
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
			fp.CallWith(func(any) error { return nil }, failpoint.Always)
		}, want: nil},
		{name: "pause", configure: func(fp *failpoint.Failpoint[error]) { fp.Pause(time.Nanosecond, failpoint.Always) }, want: nil},
		{name: "on", configure: func(fp *failpoint.Failpoint[error]) { fp.On(failpoint.Always) }, want: nil},
		{name: "skipped by trigger", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Return(errors.New("operation"), failpoint.Always)
			fp.SetTrigger(failpoint.Trigger{After: 100})
		}, want: fromAny},
		{name: "disabled", configure: func(*failpoint.Failpoint[error]) {}, want: fromAny},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := failpoint.NewRegistry()
			fp := failpoint.NewFailpoint[error](failpoint.GRPCKey(operation))
			tc.configure(fp)
			registry.Set(failpoint.GRPCKey(operation), fp)
			setReturn(registry, failpoint.AnyGRPCKey, fromAny)

			err := newAdapter(registry, nil).GenerateRequest(context.Background(), operation, "request")
			require.Equal(t, tc.want, err)
		})
	}
}

func TestAdapter_Exempt(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	injected := errors.New("injected")
	for _, key := range []failpoint.Key{failpoint.GRPCKey(operation), failpoint.AnyGRPCKey, failpoint.GRPCResponseKey(operation), failpoint.AnyGRPCResponseKey} {
		setReturn(registry, key, injected)
	}
	adapter := faultinjectiontest.NewAdapter[any, any](registrySource{registry: registry}, faultinjectiontest.Keys{
		Request:     failpoint.GRPCKey,
		Response:    failpoint.GRPCResponseKey,
		AnyRequest:  failpoint.AnyGRPCKey,
		AnyResponse: failpoint.AnyGRPCResponseKey,
		Exempt:      func(op string) bool { return op == operation },
	}, nil)

	require.NoError(t, adapter.GenerateRequest(context.Background(), operation, "request"))
	require.NoError(t, adapter.GenerateResponse(context.Background(), operation, "request", "response", nil))
	require.Equal(t, injected, adapter.GenerateRequest(context.Background(), "/test.Service/Other", "request"))
}

func TestAdapter_Buggify(t *testing.T) {
	t.Parallel()

	fromAny := errors.New("any")
	newRegistry := func(t *testing.T) failpoint.Registry {
		registry := failpoint.NewRegistry()
		// Every execution of the operation's key fires, with an error from the server's catalog.
		require.NoError(t, buggify.For(registry).Set(buggify.Config{
			Seed: 1, Activation: 1, Fire: 1, Error: "Unavailable",
			Exclude: []string{"grpc-response:*"},
		}))
		return registry
	}

	t.Run("injects an error for an operation with nothing registered", func(t *testing.T) {
		t.Parallel()
		adapter := newAdapter(newRegistry(t), nil)
		err := adapter.GenerateRequest(context.Background(), operation, "request")
		var unavailable *serviceerror.Unavailable
		require.ErrorAs(t, err, &unavailable)
		require.NoError(t, adapter.GenerateResponse(context.Background(), operation, "request", "response", nil))
	})

	t.Run("a failpoint of any operation takes precedence", func(t *testing.T) {
		t.Parallel()
		registry := newRegistry(t)
		setReturn(registry, failpoint.AnyGRPCKey, fromAny)
		require.Equal(t, fromAny, newAdapter(registry, nil).GenerateRequest(context.Background(), operation, "request"))
	})

	t.Run("a failpoint of any operation takes precedence even if it does not fire", func(t *testing.T) {
		t.Parallel()
		for name, configure := range map[string]func(*failpoint.Failpoint[error]){
			"disabled": func(*failpoint.Failpoint[error]) {},
			"suspended": func(fp *failpoint.Failpoint[error]) {
				fp.Return(fromAny, failpoint.Always)
				fp.SetEnabled(false)
			},
			"skipped by trigger": func(fp *failpoint.Failpoint[error]) {
				fp.Return(fromAny, failpoint.Always)
				fp.SetTrigger(failpoint.Trigger{After: 100})
			},
		} {
			registry := newRegistry(t)
			fp := failpoint.NewFailpoint[error](failpoint.AnyGRPCKey)
			configure(fp)
			registry.Set(failpoint.AnyGRPCKey.ForScope("namespace-id"), fp)
			request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
			require.NoError(t, newAdapter(registry, nil).GenerateRequest(context.Background(), operation, request), name)
		}
	})

	t.Run("a failpoint of the operation takes precedence even if it does not fire", func(t *testing.T) {
		t.Parallel()
		registry := newRegistry(t)
		registry.Set(failpoint.GRPCKey(operation), failpoint.NewFailpoint[error](failpoint.GRPCKey(operation)))
		require.NoError(t, newAdapter(registry, nil).GenerateRequest(context.Background(), operation, "request"))
	})

	t.Run("exempt operations are never buggified", func(t *testing.T) {
		t.Parallel()
		adapter := faultinjectiontest.NewAdapter[any, any](registrySource{registry: newRegistry(t)}, faultinjectiontest.Keys{
			Request:     failpoint.GRPCKey,
			Response:    failpoint.GRPCResponseKey,
			AnyRequest:  failpoint.AnyGRPCKey,
			AnyResponse: failpoint.AnyGRPCResponseKey,
			Exempt:      func(op string) bool { return op == operation },
		}, nil)
		require.NoError(t, adapter.GenerateRequest(context.Background(), operation, "request"))
	})
}
