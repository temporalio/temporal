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
) (*NexusOperationHTTPHandler, *mux.Router) {
	logger := log.NewTestLogger()
	h := &NexusOperationHTTPHandler{
		base: nexusrpc.BaseHTTPHandler{
			Logger:           log.NewSlogLogger(logger),
			FailureConverter: nexusrpc.DefaultFailureConverter(),
		},
		logger:                 logger,
		enpointRegistry:        endpointRegistry,
		namespaceRegistry:      namespaceRegistry,
		preprocessErrorCounter: metrics.CounterFunc(func(int64, ...metrics.Tag) {}),
	}
	router := mux.NewRouter()
	h.RegisterRoutes(router)
	return h, router
}

// capturePreprocessErrors wires the handler's preprocess error counter to a capture handler, the same way
// NewNexusOperationHTTPHandler wires it to the real metrics handler.
func capturePreprocessErrors(h *NexusOperationHTTPHandler) *metricstest.Capture {
	metricsHandler := metricstest.NewCaptureHandler()
	h.preprocessErrorCounter = metricsHandler.Counter(metrics.NexusRequestPreProcessErrors.Name()).Record
	return metricsHandler.StartCapture()
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
	h, router := newTestNexusOperationHTTPHandler(reg, nil)
	capture := capturePreprocessErrors(h)

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
	h, router := newTestNexusOperationHTTPHandler(reg, nil)
	capture := capturePreprocessErrors(h)

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

	h, router := newTestNexusOperationHTTPHandler(reg, nsReg)
	capture := capturePreprocessErrors(h)

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "true", rec.Header().Get("nexus-request-retryable"))

	var failure nexus.Failure
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&failure))
	require.Equal(t, "invalid endpoint target", failure.Message)
	require.Equal(t,
		[]*metricstest.CapturedRecording{{Value: int64(1), Tags: map[string]string{"reason": "invalid_endpoint_target"}}},
		capture.SnapshotMetric(metrics.NexusRequestPreProcessErrors.Name()))
}
