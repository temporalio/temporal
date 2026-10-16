package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestNexusStagePollCancellationPanic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		taskType enumspb.TaskQueueType
		stage    string
	}{
		{enumspb.TASK_QUEUE_TYPE_NEXUS, "handler-task-delivery"},
		{enumspb.TASK_QUEUE_TYPE_WORKFLOW, ""},
	} {
		t.Run(tc.taskType.String(), func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, "Panic is captured")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			h := &Handler{logger: logger, nexusTaskLogger: log.With(logger, tag.NexusStageHandlerTaskDelivery)}
			_, err := h.CancelOutstandingPoll(t.Context(), &matchingservice.CancelOutstandingPollRequest{TaskQueueType: tc.taskType})
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
