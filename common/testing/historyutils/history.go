package historyutils

import (
	"context"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
)

type HistoryUtils struct {
	client workflowservice.WorkflowServiceClient
	nsName namespace.Name
}

func New(client workflowservice.WorkflowServiceClient, nsName namespace.Name) HistoryUtils {
	return HistoryUtils{client: client, nsName: nsName}
}

type Scope interface {
	Context() context.Context
	AssertionT() require.TestingT
}

type contextScope struct {
	ctx context.Context
	t   require.TestingT
}

// WithContext binds a context to a test's assertion handle.
func WithContext(ctx context.Context, t require.TestingT) Scope {
	return contextScope{ctx: ctx, t: t}
}

func (s contextScope) Context() context.Context {
	return s.ctx
}

func (s contextScope) AssertionT() require.TestingT {
	return s.t
}

func (u HistoryUtils) RequireHistoryFunc(scope Scope, execution *commonpb.WorkflowExecution) func() []*historypb.HistoryEvent {
	return func() []*historypb.HistoryEvent {
		if helper, ok := scope.AssertionT().(interface{ Helper() }); ok {
			helper.Helper()
		}
		return u.RequireHistory(scope, execution)
	}
}

func (u HistoryUtils) RequireHistory(scope Scope, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	t := scope.AssertionT()
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	events, err := u.readHistory(scope.Context(), execution)
	require.NoError(t, err)
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
