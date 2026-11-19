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

type Reader struct {
	t      testing.TB
	client workflowservice.WorkflowServiceClient
}

func NewReader(t testing.TB, client workflowservice.WorkflowServiceClient) Reader {
	return Reader{t: t, client: client}
}

func (r Reader) GetHistoryFunc(ctx context.Context, nsName namespace.Name, execution *commonpb.WorkflowExecution) func() []*historypb.HistoryEvent {
	return func() []*historypb.HistoryEvent {
		r.t.Helper()
		return r.GetHistory(ctx, nsName, execution)
	}
}

func (r Reader) GetHistory(ctx context.Context, nsName namespace.Name, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	r.t.Helper()
	events, err := ReadHistory(ctx, r.client, nsName, execution)
	require.NoError(r.t, err)
	return events
}

// ReadHistory reads every page of workflow history using ctx.
func ReadHistory(
	ctx context.Context,
	client workflowservice.WorkflowServiceClient,
	nsName namespace.Name,
	execution *commonpb.WorkflowExecution,
) ([]*historypb.HistoryEvent, error) {
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       nsName.String(),
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	}
	var events []*historypb.HistoryEvent
	for {
		response, err := client.GetWorkflowExecutionHistory(ctx, request)
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
