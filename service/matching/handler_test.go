package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// nexusMetricsCaptureEngine is a stub Engine that captures the opMetrics handler
// passed to Nexus handler methods. It embeds the Engine interface so only the
// methods under test need to be implemented.
type nexusMetricsCaptureEngine struct {
	Engine // embedded to satisfy all other interface methods (will panic if called)
	capturedMetrics metrics.Handler
}

func (e *nexusMetricsCaptureEngine) Start() {}
func (e *nexusMetricsCaptureEngine) Stop()  {}

func (e *nexusMetricsCaptureEngine) PollNexusTaskQueue(
	_ context.Context,
	_ *matchingservice.PollNexusTaskQueueRequest,
	opMetrics metrics.Handler,
) (*matchingservice.PollNexusTaskQueueResponse, error) {
	e.capturedMetrics = opMetrics
	return &matchingservice.PollNexusTaskQueueResponse{}, nil
}

func (e *nexusMetricsCaptureEngine) RespondNexusTaskCompleted(
	_ context.Context,
	_ *matchingservice.RespondNexusTaskCompletedRequest,
	opMetrics metrics.Handler,
) (*matchingservice.RespondNexusTaskCompletedResponse, error) {
	e.capturedMetrics = opMetrics
	return &matchingservice.RespondNexusTaskCompletedResponse{}, nil
}

func (e *nexusMetricsCaptureEngine) RespondNexusTaskFailed(
	_ context.Context,
	_ *matchingservice.RespondNexusTaskFailedRequest,
	opMetrics metrics.Handler,
) (*matchingservice.RespondNexusTaskFailedResponse, error) {
	e.capturedMetrics = opMetrics
	return &matchingservice.RespondNexusTaskFailedResponse{}, nil
}

// ctxWithClientName creates a context with the given client-name set in incoming
// gRPC metadata and a deadline (required by PollNexusTaskQueue).
func ctxWithClientName(clientName string) context.Context {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, 10*time.Second)
	md := metadata.New(map[string]string{
		headers.ClientNameHeaderName: clientName,
	})
	return metadata.NewIncomingContext(ctx, md)
}

// newTestHandler creates a Handler wired with the given capture handler, a stub
// engine, and a mock namespace registry that returns a fixed name.
func newTestHandler(t *testing.T, captureHandler *metricstest.CaptureHandler) (*Handler, *nexusMetricsCaptureEngine) {
	t.Helper()
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, assert.AnError).AnyTimes()

	stubEngine := &nexusMetricsCaptureEngine{}

	h := &Handler{
		engine:            stubEngine,
		config:            NewConfig(dynamicconfig.NewNoopCollection()),
		metricsHandler:    captureHandler,
		logger:            log.NewNoopLogger(),
		throttledLogger:   log.NewNoopLogger(),
		namespaceRegistry: nsRegistry,
	}
	// Mark handler as started so requests are served.
	h.startWG.Add(1)
	h.startWG.Done()

	return h, stubEngine
}

// hasTag checks whether any recording for the given metric name contains the
// specified tag key/value pair.
func hasTag(snap metricstest.CaptureSnapshot, tagKey, tagValue string) bool {
	for _, recordings := range snap {
		for _, rec := range recordings {
			if v, ok := rec.Tags[tagKey]; ok && v == tagValue {
				return true
			}
		}
	}
	return false
}

func TestWithClientNameTag(t *testing.T) {
	t.Run("adds tag when client name is present", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h := &Handler{}
		ctx := ctxWithClientName("temporal-go")
		tagged := h.withClientNameTag(ctx, captureHandler)

		// Emit a counter on the tagged handler and verify the tag is present.
		tagged.Counter("test_counter").Record(1)
		snap := capture.Snapshot()
		require.True(t, hasTag(snap, "client_name", "temporal-go"),
			"expected client_name=temporal-go tag, got snapshot: %v", snap)
	})

	t.Run("does not add tag when client name is absent", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h := &Handler{}
		ctx := context.Background()
		tagged := h.withClientNameTag(ctx, captureHandler)

		tagged.Counter("test_counter").Record(1)
		snap := capture.Snapshot()
		for _, recordings := range snap {
			for _, rec := range recordings {
				_, exists := rec.Tags["client_name"]
				assert.False(t, exists, "expected no client_name tag when header is absent")
			}
		}
	})
}

func TestNexusHandlersIncludeClientNameTag(t *testing.T) {
	const expectedClientName = "temporal-go"
	taskQueue := &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	t.Run("PollNexusTaskQueue", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, engine := newTestHandler(t, captureHandler)
		ctx := ctxWithClientName(expectedClientName)

		_, err := h.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: "test-ns-id",
			Request: &workflowservice.PollNexusTaskQueueRequest{
				TaskQueue: taskQueue,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, engine.capturedMetrics)

		// Emit a metric through the captured handler to verify tags.
		engine.capturedMetrics.Counter("poll_test").Record(1)
		snap := capture.Snapshot()
		require.True(t, hasTag(snap, "client_name", expectedClientName),
			"PollNexusTaskQueue should propagate client_name tag, got: %v", snap)
	})

	t.Run("RespondNexusTaskCompleted", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, engine := newTestHandler(t, captureHandler)
		ctx := ctxWithClientName(expectedClientName)

		_, err := h.RespondNexusTaskCompleted(ctx, &matchingservice.RespondNexusTaskCompletedRequest{
			NamespaceId: "test-ns-id",
			TaskQueue:   taskQueue,
		})
		require.NoError(t, err)
		require.NotNil(t, engine.capturedMetrics)

		engine.capturedMetrics.Counter("respond_completed_test").Record(1)
		snap := capture.Snapshot()
		require.True(t, hasTag(snap, "client_name", expectedClientName),
			"RespondNexusTaskCompleted should propagate client_name tag, got: %v", snap)
	})

	t.Run("RespondNexusTaskFailed", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, engine := newTestHandler(t, captureHandler)
		ctx := ctxWithClientName(expectedClientName)

		_, err := h.RespondNexusTaskFailed(ctx, &matchingservice.RespondNexusTaskFailedRequest{
			NamespaceId: "test-ns-id",
			TaskQueue:   taskQueue,
		})
		require.NoError(t, err)
		require.NotNil(t, engine.capturedMetrics)

		engine.capturedMetrics.Counter("respond_failed_test").Record(1)
		snap := capture.Snapshot()
		require.True(t, hasTag(snap, "client_name", expectedClientName),
			"RespondNexusTaskFailed should propagate client_name tag, got: %v", snap)
	})

	t.Run("no client_name tag when header is absent", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, engine := newTestHandler(t, captureHandler)
		ctx := context.Background()
		ctx, _ = context.WithTimeout(ctx, 10*time.Second)

		_, err := h.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: "test-ns-id",
			Request: &workflowservice.PollNexusTaskQueueRequest{
				TaskQueue: taskQueue,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, engine.capturedMetrics)

		engine.capturedMetrics.Counter("poll_no_client_test").Record(1)
		snap := capture.Snapshot()
		assert.False(t, hasTag(snap, "client_name", expectedClientName),
			"PollNexusTaskQueue should not have client_name tag when header is absent, got: %v", snap)
	})
}

func TestWorkerHeartbeatToListInfo_AllFieldsSet(t *testing.T) {
	hb := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "instance-key-1",
		WorkerIdentity:    "worker-identity-1",
		HostInfo: &workerpb.WorkerHostInfo{
			HostName:          "host-1",
			WorkerGroupingKey: "grouping-key-1",
			ProcessId:         "pid-123",
		},
		TaskQueue:         "my-task-queue",
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "dep-1", BuildId: "build-1"},
		SdkName:           "temporal-go",
		SdkVersion:        "1.0.0",
		Status:            enumspb.WORKER_STATUS_RUNNING,
		StartTime:         timestamppb.Now(),
		Plugins:           []*workerpb.PluginInfo{{Name: "plugin-1"}},
		Drivers:           []*workerpb.StorageDriverInfo{{Type: "driver-1"}},
	}

	result := workerHeartbeatToListInfo(hb)
	require.NotNil(t, result)

	resultReflect := result.ProtoReflect()
	fields := resultReflect.Descriptor().Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		val := resultReflect.Get(fd)
		if fd.IsList() {
			assert.NotEqualf(t, 0, val.List().Len(),
				"WorkerListInfo repeated field %q is empty — "+
					"if you added a new field to WorkerListInfo, make sure to populate it from WorkerHeartbeat",
				fd.Name(),
			)
		} else {
			assert.Truef(t, resultReflect.Has(fd),
				"WorkerListInfo field %q is not set by workerHeartbeatToListInfo — "+
					"if you added a new field to WorkerListInfo, make sure to populate it from WorkerHeartbeat",
				fd.Name(),
			)
		}
	}
}
