package testcore

import (
	"context"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyutils"
)

func (e *TestEnv) GetHistoryFunc(ctx context.Context, nsName namespace.Name, execution *commonpb.WorkflowExecution) func() []*historypb.HistoryEvent {
	return func() []*historypb.HistoryEvent {
		e.t.Helper()
		return e.GetHistory(ctx, nsName, execution)
	}
}

func (e *TestEnv) GetHistory(ctx context.Context, nsName namespace.Name, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	e.t.Helper()
	events, err := historyutils.ReadHistory(ctx, e.FrontendClient(), nsName, execution)
	require.NoError(e.t, err)
	return events
}
