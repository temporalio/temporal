package frontend

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc/interceptor"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func testOperationContext() *operationContext {
	oc := &operationContext{
		nexusContext: &nexusContext{},
	}
	oc.logger = log.NewTestLogger()
	oc.apiName = "/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask"
	oc.responseHeaders = make(map[string]string)

	oc.namespaceName = "test-namespace"
	oc.namespace = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:    uuid.NewString(),
			Name:  oc.namespaceName,
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
		&persistencespb.NamespaceConfig{
			Retention:                    timestamp.DurationFromDays(1),
			CustomSearchAttributeAliases: make(map[string]string),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)

	return oc
}

func TestNexusDispatchRestoresRequestHeaders(t *testing.T) {
	for _, operation := range []string{"start", "cancel"} {
		for _, outcome := range []string{"success", "error", "panic"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				oc := testOperationContext()
				client := matchingservicemock.NewMockMatchingServiceClient(gomock.NewController(t))
				h := &nexusHandler{matchingClient: client,
					headersBlacklist: dynamicconfig.GetTypedPropertyFn(regexp.MustCompile("^secret$")),
					payloadSizeLimit: dynamicconfig.GetIntPropertyFnFilteredByNamespace(1024),
				}
				originalHeaders := map[string]string{"secret": "private", "public": "visible"}
				request := &matchingservice.DispatchNexusTaskRequest{Request: &nexuspb.Request{Header: originalHeaders}}
				options := nexus.StartOperationOptions{}
				var input interceptornexus.InterceptorInput
				if operation == "start" {
					request.Request.Variant = &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{}}
					lazy := nexus.NewLazyValue(commonnexus.PayloadSerializer, &nexus.Reader{ReadCloser: io.NopCloser(strings.NewReader("payload"))})
					input = interceptornexus.NewStartOpInput("svc", "op", time.Now(), options, lazy, interceptornexus.ForwardingInfo{}, interceptornexus.RequestMetadata{NamespaceEntry: oc.namespace, Request: request})
				} else {
					request.Request.Variant = &nexuspb.Request_CancelOperation{CancelOperation: &nexuspb.CancelOperationRequest{}}
					input = interceptornexus.NewCancelOpInput("svc", "op", time.Now(), nexus.CancelOperationOptions{}, "token", interceptornexus.ForwardingInfo{}, interceptornexus.RequestMetadata{NamespaceEntry: oc.namespace, Request: request})
				}
				client.EXPECT().DispatchNexusTask(gomock.Any(), request).DoAndReturn(
					func(context.Context, *matchingservice.DispatchNexusTaskRequest, ...grpc.CallOption) (*matchingservice.DispatchNexusTaskResponse, error) {
						require.Equal(t, map[string]string{"public": "visible"}, request.Request.Header)
						switch outcome {
						case "panic":
							panic("matching panic")
						case "error":
							return nil, errors.New("matching error")
						default:
						}
						if operation == "start" {
							return startOperationResponse(&nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: &nexuspb.StartOperationResponse_Sync{}}}), nil
						}
						return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{Response: &nexuspb.Response{Variant: &nexuspb.Response_CancelOperation{CancelOperation: &nexuspb.CancelOperationResponse{}}}}}, nil
					})
				invoke := func() {
					_, err := h.finalHandler(withOperationContext(nexus.WithHandlerContext(context.Background(), nexus.HandlerInfo{}), oc), input)
					if outcome == "success" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				}
				if outcome == "panic" {
					require.Panics(t, invoke)
				} else {
					invoke()
				}
				require.Equal(t, originalHeaders, request.Request.Header)
				request.Request.Header["restored"] = "same map"
				require.Equal(t, "same map", originalHeaders["restored"])
			})
		}
	}
}

func TestNexusTelemetryTagsUseOriginalHeaders(t *testing.T) {
	oc := testOperationContext()
	headers := nexus.Header{"tenant": "original", "outcome": "untrusted"}
	input := interceptornexus.NewStartOpInput("service", "operation", time.Now(),
		nexus.StartOperationOptions{Header: headers}, nil, interceptornexus.ForwardingInfo{},
		interceptornexus.RequestMetadata{NamespaceEntry: oc.namespace},
	)
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	telemetry := interceptor.NewTelemetryInterceptor(nil, handler, log.NewNoopLogger(), nil, nil, func(in interceptornexus.InterceptorInput) []metrics.Tag {
		return nexusMetricTags(chasmnexus.NexusMetricTagConfig{
			IncludeServiceTag: true, IncludeOperationTag: true,
			HeaderTagMappings: []chasmnexus.NexusHeaderTagMapping{
				{SourceHeader: "tenant", TargetTag: "tenant"},
				{SourceHeader: "outcome", TargetTag: "outcome"},
			},
		}, in)
	})
	_, err := telemetry.InterceptNexusOutermost(context.Background(), input,
		func(context.Context, interceptornexus.InterceptorInput) (any, error) {
			headers["tenant"] = "changed"
			return &nexus.HandlerStartOperationResultSync[any]{}, nil
		})
	require.NoError(t, err)
	recordings := capture.Snapshot()[metrics.NexusRequests.Name()]
	require.Len(t, recordings, 1)
	tags := recordings[0].Tags
	require.Equal(t, "original", tags["tenant"])
	require.Equal(t, "service", tags[metrics.NexusServiceTag("service").Key])
	require.Equal(t, "operation", tags[metrics.NexusOperationTag("operation").Key])
	require.Equal(t, "sync_success", tags[metrics.OutcomeTag("sync_success").Key])
}
