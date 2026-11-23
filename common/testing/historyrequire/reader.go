package historyrequire

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testcontext"
)

type Reader struct {
	client workflowservice.WorkflowServiceClient
	nsName namespace.Name
}

func NewReader(client workflowservice.WorkflowServiceClient, nsName namespace.Name) Reader {
	return Reader{client: client, nsName: nsName}
}

type scope interface {
	Context() context.Context
	AssertionT() require.TestingT
}

func (u Reader) RequireHistoryFunc(s scope, execution *commonpb.WorkflowExecution) HistoryEventsReader {
	return func() []*historypb.HistoryEvent {
		if helper, ok := s.AssertionT().(helper); ok {
			helper.Helper()
		}
		return u.RequireHistory(s, execution)
	}
}

func (u Reader) RequireHistory(s scope, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	t := s.AssertionT()
	if helper, ok := t.(helper); ok {
		helper.Helper()
	}
	return u.requireHistory(s.Context(), t, execution)
}

// RequireHistoryWithT reads history using t's test context.
func (u Reader) RequireHistoryWithT(t *testing.T, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	t.Helper()
	return u.requireHistory(testcontext.For(t), t, execution)
}

func (u Reader) requireHistory(ctx context.Context, t require.TestingT, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	if helper, ok := t.(helper); ok {
		helper.Helper()
	}
	events, err := u.readHistory(ctx, execution)
	require.NoError(t, err)
	return events
}

// readHistory reads every page of workflow history using ctx.
func (u Reader) readHistory(ctx context.Context, execution *commonpb.WorkflowExecution) ([]*historypb.HistoryEvent, error) {
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
