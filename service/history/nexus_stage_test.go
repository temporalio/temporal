package history

import (
	"context"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/testing/testlogger"
)

type systemNexusResultHandler struct {
	nexus.UnimplementedHandler
	result nexus.HandlerStartOperationResult[any]
}

func (h systemNexusResultHandler) StartOperation(context.Context, string, string, *nexus.LazyValue, nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return h.result, nil
}

func TestNexusStageSystemHandlerErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, message string
		result        nexus.HandlerStartOperationResult[any]
	}{
		{"payload encoding", "failed to encode payload", &nexus.HandlerStartOperationResultSync[any]{Value: func() {}}},
		{"invalid result", "invalid result type: <nil>", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, tc.message)
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			h := &Handler{logger: logger, nexusHandler: systemNexusResultHandler{result: tc.result}}
			_, err := h.StartNexusOperation(t.Context(), &historyservice.StartNexusOperationRequest{Request: &nexuspb.StartOperationRequest{RequestId: "request-id"}})
			require.Error(t, err)
			capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: tc.message, Tags: map[string]any{"nexus-stage": "handler-inbound", "request-id": "request-id"}})
		})
	}
}
