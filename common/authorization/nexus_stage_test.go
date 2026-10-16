package authorization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

func TestNexusStageAuthorizationFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ method, stage string }{
		{api.WorkflowServicePrefix + "PollNexusTaskQueue", "handler-task-delivery"},
		{api.NexusServicePrefix + "DispatchByEndpoint", "handler-inbound"},
		{api.NexusServicePrefix + "CompleteNexusOperation", "caller-inbound"},
		{api.WorkflowServicePrefix + "StartNexusOperationExecution", "caller-outbound"},
		{api.WorkflowServicePrefix + "StartWorkflowExecution", ""},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, "Authorization error")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			authorizer := NewMockAuthorizer(gomock.NewController(t))
			authorizer.EXPECT().Authorize(gomock.Any(), gomock.Any(), gomock.Any()).Return(Result{}, errors.New("broken authorizer"))
			i := &Interceptor{authorizer: authorizer, logger: logger, metricsHandler: metrics.NoopMetricsHandler, exposeAuthorizerErrors: func() bool { return false }}
			_, err := i.Authorize(context.Background(), nil, &CallTarget{APIName: tc.method})
			require.Error(t, err)
			records := capture.Snapshot()
			require.Len(t, records, 1)
			tags := map[string]any{}
			for _, tag := range records[0].Tags {
				tags[tag.Key()] = tag.Value()
			}
			if tc.stage == "" {
				require.NotContains(t, tags, "nexus-stage")
			} else {
				require.Equal(t, tc.stage, tags["nexus-stage"])
			}
		})
	}
}
