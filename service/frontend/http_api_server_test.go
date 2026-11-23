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
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestDraftFieldsHTTPBoundary(t *testing.T) {
	for _, early := range []bool{false, true} {
		handlerCalls := 0
		dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
			dynamicconfig.FrontendEnabledDraftFeatureAPIs.Key(): []string{"feature-a"},
		}, log.NewNoopLogger())
		source := &workflowservice.GetSystemInfoResponse{ServerVersion: "stable-version"}
		var roots []any
		var selections []*draft.Selection
		spy := func(root any, selection *draft.Selection) (bool, error) {
			roots = append(roots, root)
			selections = append(selections, selection)
			require.True(t, selection.Enabled("feature-a"))
			require.False(t, selection.Enabled("feature-b"))
			return false, nil
		}
		custom := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if early {
				return source, nil
			}
			return handler(ctx, req)
		}
		options := draftBoundaryOptions(t, dc, nil, nil, custom, spy)
		svc := &draftHTTPService{response: source, calls: &handlerCalls}
		conn := newInlineClientConn(map[string]any{"temporal.api.workflowservice.v1.WorkflowService": svc}, options.UnaryInterceptors, metrics.NoopMetricsHandler, nil)
		req := &workflowservice.GetSystemInfoRequest{}
		reply := &workflowservice.GetSystemInfoResponse{}
		err := conn.Invoke(metadata.NewOutgoingContext(t.Context(), metadata.MD{}), draftFrontendMethod, req, reply)
		require.NoError(t, err)
		require.Len(t, roots, 2)
		require.Same(t, req, roots[0])
		require.Same(t, source, roots[1])
		require.Same(t, selections[0], selections[1])
		require.True(t, proto.Equal(source, reply))
		_, marshaler := newTemporalProtoMarshaler("", false)
		encoded, err := marshaler.Marshal(reply)
		require.NoError(t, err)
		require.JSONEq(t, `{"serverVersion":"stable-version"}`, string(encoded))
		if early {
			require.Zero(t, handlerCalls)
		} else {
			require.Equal(t, 1, handlerCalls)
		}
	}
}

type draftHTTPService struct {
	response *workflowservice.GetSystemInfoResponse
	calls    *int
}

func (s *draftHTTPService) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	*s.calls++
	return s.response, nil
}
