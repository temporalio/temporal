package frontend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/rpc/interceptor"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryableNotFoundError is a gRPC NotFound error that also implements Retryable() bool.
type retryableNotFoundError struct {
	msg string
}

func (e *retryableNotFoundError) Error() string   { return e.msg }
func (e *retryableNotFoundError) Retryable() bool { return true }
func (e *retryableNotFoundError) GRPCStatus() *status.Status {
	return status.New(codes.NotFound, e.msg)
}

// fakeNamespaceRegistry implements namespace.Registry with just GetNamespaceName.
// All other methods panic.
type fakeNamespaceRegistry struct {
	namespace.Registry
	getNamespaceName func(id namespace.ID) (namespace.Name, error)
}

func (f *fakeNamespaceRegistry) GetNamespaceName(id namespace.ID) (namespace.Name, error) {
	return f.getNamespaceName(id)
}

func newTestNexusOperationHTTPHandler(
	endpointRegistry commonnexus.EndpointRegistry,
	namespaceRegistry namespace.Registry,
) (*mux.Router, *metricstest.Capture) {
	logger := log.NewTestLogger()
	metricsHandler := metricstest.NewCaptureHandler()
	h := &NexusOperationHTTPHandler{
		base: nexusrpc.BaseHTTPHandler{
			Logger:           log.NewSlogLogger(logger),
			FailureConverter: nexusrpc.DefaultFailureConverter(),
		},
		logger:                 logger,
		enpointRegistry:        endpointRegistry,
		namespaceRegistry:      namespaceRegistry,
		preprocessErrorCounter: metricsHandler.Counter(metrics.NexusRequestPreProcessErrors.Name()).Record,
	}
	router := mux.NewRouter()
	h.RegisterRoutes(router)
	return router, metricsHandler.StartCapture()
}

func doNexusHTTPRequest(t *testing.T, router *mux.Router, endpointID string) *httptest.ResponseRecorder {
	t.Helper()
	path := "/" + commonnexus.RouteDispatchNexusTaskByEndpoint.Path(endpointID) + "/test-service/test-operation"
	req := httptest.NewRequest(http.MethodPost, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestDispatchNexusTaskByEndpoint_NotFound_NonRetryable(t *testing.T) {
	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return nil, serviceerror.NewNotFound("endpoint not found")
		},
	}
	router, capture := newTestNexusOperationHTTPHandler(reg, nil)

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "false", rec.Header().Get("nexus-request-retryable"))

	var failure nexus.Failure
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&failure))
	require.Equal(t, "nexus endpoint not found", failure.Message)
	require.Equal(t,
		[]*metricstest.CapturedRecording{{Value: int64(1), Tags: map[string]string{"reason": "endpoint_not_found"}}},
		capture.SnapshotMetric(metrics.NexusRequestPreProcessErrors.Name()))
}

func TestDispatchNexusTaskByEndpoint_NotFound_Retryable(t *testing.T) {
	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return nil, &retryableNotFoundError{msg: "endpoint temporarily unavailable"}
		},
	}
	router, capture := newTestNexusOperationHTTPHandler(reg, nil)

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "true", rec.Header().Get("nexus-request-retryable"))

	var failure nexus.Failure
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&failure))
	require.Equal(t, "nexus endpoint not found", failure.Message)
	require.Equal(t,
		[]*metricstest.CapturedRecording{{Value: int64(1), Tags: map[string]string{"reason": "endpoint_not_found"}}},
		capture.SnapshotMetric(metrics.NexusRequestPreProcessErrors.Name()))
}

func TestDispatchNexusTaskByEndpoint_NamespaceNotFound_Retryable(t *testing.T) {
	endpointEntry := &persistencespb.NexusEndpointEntry{
		Id: "test-endpoint-id",
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Name: "test-endpoint",
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_Worker_{
						Worker: &persistencespb.NexusEndpointTarget_Worker{
							NamespaceId: "test-ns-id",
							TaskQueue:   "test-task-queue",
						},
					},
				},
			},
		},
	}

	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return endpointEntry, nil
		},
	}
	nsReg := &fakeNamespaceRegistry{
		getNamespaceName: func(id namespace.ID) (namespace.Name, error) {
			return "", serviceerror.NewNamespaceNotFound("test-ns-id")
		},
	}

	router, capture := newTestNexusOperationHTTPHandler(reg, nsReg)

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "true", rec.Header().Get("nexus-request-retryable"))

	var failure nexus.Failure
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&failure))
	require.Equal(t, "invalid endpoint target", failure.Message)
	require.Equal(t,
		[]*metricstest.CapturedRecording{{Value: int64(1), Tags: map[string]string{"reason": "target_namespace_not_found"}}},
		capture.SnapshotMetric(metrics.NexusRequestPreProcessErrors.Name()))
}

func TestNexusRequestErrorReporting(t *testing.T) {
	for _, tc := range []struct {
		name       string
		source     string
		completion bool
		skip       bool
		report     bool
	}{
		{name: "frontend failure", source: "frontend", report: true},
		{name: "worker failure", source: commonnexus.FailureSourceWorker},
		{name: "no failure source"},
		{name: "completion failure", completion: true, report: true},
		{name: "operation reporting explicitly skipped", source: "frontend", skip: true},
		{name: "completion reporting explicitly skipped", completion: true, skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oc := testOperationContext()
			errorHandler := interceptor.NewMockErrorHandler(gomock.NewController(t))
			originalErr := serviceerror.NewInternal("internal details")
			if tc.report {
				errorHandler.EXPECT().HandleError(nil, "", gomock.Any(), gomock.Any(), originalErr, oc.namespace.Name())
			}
			err := &interceptornexus.InterceptorError{Err: originalErr, SkipServiceErrorReporting: tc.skip}
			metricsHandler := metricstest.NewCaptureHandler()
			if tc.completion {
				h := &nexusCompletionHandler{MetricsHandler: metricsHandler, RequestErrorHandler: errorHandler}
				rCtx := &requestContext{nexusCompletionHandler: h, namespace: oc.namespace,
					metricsHandlerForInterceptors: metricsHandler}
				rCtx.handleRequestError(err)
			} else {
				oc.responseHeaders[commonnexus.FailureSourceHeaderName] = tc.source
				if tc.source == "" {
					delete(oc.responseHeaders, commonnexus.FailureSourceHeaderName)
				}
				oc.metricsHandlerForInterceptors = metricsHandler
				oc.requestErrorHandler = errorHandler
				oc.handleRequestError(err)
			}
		})
	}
}

func TestNexusOperationFinalizesErrors(t *testing.T) {
	for _, operation := range []string{"start", "cancel"} {
		for _, exposeDetails := range []bool{false, true} {
			t.Run(operation, func(t *testing.T) {
				oc := testOperationContext()
				registry := namespace.NewMockRegistry(gomock.NewController(t))
				registry.EXPECT().GetNamespace(namespace.Name(oc.namespaceName)).Return(oc.namespace, nil)
				errorHandler := interceptor.NewMockErrorHandler(gomock.NewController(t))
				originalErr := serviceerror.NewInternal("private details")
				errorHandler.EXPECT().HandleError(nil, "", gomock.Any(), gomock.Any(), originalErr, oc.namespace.Name())
				h := &nexusHandler{logger: log.NewNoopLogger(), namespaceRegistry: registry, metricsHandler: metricstest.NewCaptureHandler(), requestErrorHandler: errorHandler}
				h.chainedHandler = func(ctx context.Context, _ interceptornexus.InterceptorInput) (any, error) {
					current, ok := operationContextFromContext(ctx)
					require.True(t, ok)
					current.setFailureSource("frontend")
					return nil, &interceptornexus.InterceptorError{Err: originalErr, ExposeDetails: exposeDetails}
				}
				ctx := context.WithValue(context.Background(), nexusContextKey{}, oc.nexusContext)
				var err error
				if operation == "start" {
					_, err = h.StartOperation(ctx, "svc", "op", nil, nexus.StartOperationOptions{})
				} else {
					err = h.CancelOperation(ctx, "svc", "op", "token", nexus.CancelOperationOptions{})
				}
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				if exposeDetails {
					require.ErrorContains(t, err, "private details")
				} else {
					require.NotContains(t, err.Error(), "private details")
				}
			})
		}
	}
}

func TestNexusStartRecoversPreparationPanic(t *testing.T) {
	oc := testOperationContext()
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespace(namespace.Name(oc.namespaceName)).Return(oc.namespace, nil)
	h := &nexusHandler{logger: log.NewNoopLogger(), namespaceRegistry: registry, metricsHandler: metricstest.NewCaptureHandler(),
		chainedHandler: func(context.Context, interceptornexus.InterceptorInput) (any, error) {
			t.Fatal("preparation failed before the chain")
			return nil, nil
		},
	}
	ctx := context.WithValue(context.Background(), nexusContextKey{}, oc.nexusContext)
	_, err := h.StartOperation(ctx, "svc", "op", nil, nexus.StartOperationOptions{Links: []nexus.Link{{URL: nil}}})
	require.Error(t, err)
}

func TestNexusCompletionRecoversPreparationPanic(t *testing.T) {
	oc := testOperationContext()
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceByID(oc.namespace.ID()).Return(oc.namespace, nil)
	errorHandler := interceptor.NewMockErrorHandler(gomock.NewController(t))
	errorHandler.EXPECT().HandleError(nil, "", gomock.Any(), gomock.Any(), gomock.Any(), oc.namespace.Name())
	generator := commonnexus.NewCallbackTokenGenerator()
	completion := hsmCompletionToken()
	completion.NamespaceId = oc.namespace.ID().String()
	token, err := generator.Tokenize(completion)
	require.NoError(t, err)
	h := &nexusCompletionHandler{Logger: log.NewNoopLogger(), NamespaceRegistry: registry,
		MetricsHandler: metricstest.NewCaptureHandler(), RequestErrorHandler: errorHandler, CallbackTokenGenerator: generator,
		chainedHandler: func(context.Context, interceptornexus.InterceptorInput) (any, error) {
			t.Fatal("preparation failed before the chain")
			return nil, nil
		},
	}
	err = h.CompleteOperation(context.Background(), &nexusrpc.CompletionRequest{
		HTTPRequest: &http.Request{Header: http.Header{commonnexus.CallbackTokenHeader: []string{token}}},
	})
	require.Error(t, err)
}

func TestNexusCancelRecoversChainPanic(t *testing.T) {
	for _, panicValue := range []any{"private details", serviceerror.NewInternal("private details")} {
		t.Run("panic", func(t *testing.T) {
			oc := testOperationContext()
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			registry.EXPECT().GetNamespace(namespace.Name(oc.namespaceName)).Return(oc.namespace, nil)
			h := &nexusHandler{logger: log.NewNoopLogger(), namespaceRegistry: registry, metricsHandler: metricstest.NewCaptureHandler(),
				chainedHandler: func(context.Context, interceptornexus.InterceptorInput) (any, error) {
					panic(panicValue)
				},
			}
			ctx := context.WithValue(context.Background(), nexusContextKey{}, oc.nexusContext)
			err := h.CancelOperation(ctx, "svc", "op", "token", nexus.CancelOperationOptions{})
			if _, ok := panicValue.(error); ok {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.NotContains(t, err.Error(), "private details")
			} else {
				require.EqualError(t, err, "panic: private details")
			}
		})
	}
}
