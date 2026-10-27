package queryworkflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/metrics"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestSingleSessionQueryDoesNotFallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "worker unavailable", err: &serviceerrors.StickyWorkerUnavailable{}},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "cancelled", err: context.Canceled},
		{name: "internal", err: serviceerror.NewInternal("failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			client := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).DoAndReturn(
				func(callCtx context.Context, req *matchingservice.QueryWorkflowRequest, _ ...grpc.CallOption) (*matchingservice.QueryWorkflowResponse, error) {
					require.NoError(t, callCtx.Err())
					require.Equal(t, "S", req.GetTaskQueue().GetName())
					require.Equal(t, enumspb.TASK_QUEUE_KIND_STICKY, req.GetTaskQueue().GetKind())
					return &matchingservice.QueryWorkflowResponse{}, tc.err
				})
			response, err := queryDirectlyThroughMatching(ctx, &historyservice.GetMutableStateResponse{
				SingleSessionBound: true,
				TaskQueue:          &taskqueuepb.TaskQueue{Name: "foo"},
				StickyTaskQueue:    &taskqueuepb.TaskQueue{Name: "S", Kind: enumspb.TASK_QUEUE_KIND_STICKY},
			}, tests.LocalNamespaceEntry, tests.NamespaceID.String(), &workflowservice.QueryWorkflowRequest{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "A", RunId: tests.RunID},
			}, nil, nil, client, nil, metrics.NoopMetricsHandler, nil)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.NotNil(t, response)
			}
		})
	}
}
