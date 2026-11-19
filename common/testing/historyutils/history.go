package historyutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
)

type HistoryUtils struct {
	t      testing.TB
	client workflowservice.WorkflowServiceClient
	nsName namespace.Name
}

func New(t testing.TB, client workflowservice.WorkflowServiceClient, nsName namespace.Name) HistoryUtils {
	return HistoryUtils{t: t, client: client, nsName: nsName}
}

func (u HistoryUtils) RequireHistoryFunc(ctx context.Context, execution *commonpb.WorkflowExecution) func() []*historypb.HistoryEvent {
	return func() []*historypb.HistoryEvent {
		u.t.Helper()
		return u.RequireHistory(ctx, execution)
	}
}

func (u HistoryUtils) RequireHistory(ctx context.Context, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	u.t.Helper()
	events, err := u.readHistory(ctx, execution)
	require.NoError(u.t, err)
	return events
}

// readHistory reads every page of workflow history using ctx.
func (u HistoryUtils) readHistory(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) ([]*historypb.HistoryEvent, error) {
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       u.nsName.String(),
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	}
	var events []*historypb.HistoryEvent
	for {
		response, err := u.client.GetWorkflowExecutionHistory(ctx, request)
		if err != nil {
			return nil, err
		}
		events = append(events, response.GetHistory().GetEvents()...)
		request.NextPageToken = response.GetNextPageToken()
		if len(request.NextPageToken) == 0 {
			return events, nil
		}
		request.MaximumPageSize = 0
	}
}
