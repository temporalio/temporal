package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestDraftFieldsHTTPBoundary(t *testing.T) {
	for _, early := range []bool{false, true} {
		handlerCalls := 0
		dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
			dynamicconfig.FrontendEnabledDraftFeatureAPIs.Key(): []string{"feature-a"},
		}, log.NewNoopLogger())
		source := &workflowservice.GetWorkflowExecutionHistoryResponse{History: draftUpdateHistory("bob")}
		var roots []any
		var selections []*draft.Selection
		spy := func(root any, selection *draft.Selection) (bool, error) {
			roots = append(roots, root)
			selections = append(selections, selection)
			require.True(t, selection.Enabled("feature-a"))
			require.False(t, selection.Enabled(draftUpdateFeature))
			return draft.Omit(root, selection)
		}
		custom := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if early {
				return source, nil
			}
			return handler(ctx, req)
		}
		registry := namespace.NewMockRegistry(gomock.NewController(t))
		registry.EXPECT().GetNamespace(namespace.Name("namespace")).Return(namespace.NewLocalNamespaceForTest(nil, nil, "local"), nil).AnyTimes()
		options := draftBoundaryOptions(t, dc, registry, nil, custom, spy)
		svc := &draftHTTPService{response: source, calls: &handlerCalls}
		conn := newInlineClientConn(map[string]any{"temporal.api.workflowservice.v1.WorkflowService": svc}, options.UnaryInterceptors, metrics.NoopMetricsHandler, registry)
		req := &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "namespace"}
		reply := &workflowservice.GetWorkflowExecutionHistoryResponse{}
		err := conn.Invoke(metadata.NewOutgoingContext(t.Context(), metadata.MD{}), draftHistoryMethod, req, reply)
		require.NoError(t, err)
		require.Len(t, roots, 2)
		require.Same(t, req, roots[0])
		require.NotSame(t, source, roots[1])
		require.Same(t, selections[0], selections[1])
		require.Equal(t, []string{"bob", "bob"}, draftUpdateUsers(source.GetHistory()))
		protorequire.ProtoEqual(t, &workflowservice.GetWorkflowExecutionHistoryResponse{History: draftUpdateHistory("")}, reply)
		_, marshaler := newTemporalProtoMarshaler("", false)
		encoded, err := marshaler.Marshal(reply)
		require.NoError(t, err)
		require.JSONEq(t, `{"history":{"events":[
			{"eventId":"1","workflowExecutionUpdateAcceptedEventAttributes":{"acceptedRequest":{"meta":{"updateId":"u"}}}},
			{"eventId":"2","workflowExecutionUpdateAdmittedEventAttributes":{"request":{}}}
		]}}`, string(encoded))
		if early {
			require.Zero(t, handlerCalls)
		} else {
			require.Equal(t, 1, handlerCalls)
		}
	}
}

type draftHTTPService struct {
	response *workflowservice.GetWorkflowExecutionHistoryResponse
	calls    *int
}

func (s *draftHTTPService) GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	*s.calls++
	return s.response, nil
}
