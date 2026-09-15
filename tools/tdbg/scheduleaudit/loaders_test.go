package scheduleaudit

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/payload"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type executionWorkflowClient struct {
	workflowservice.WorkflowServiceClient
	responses []*workflowservice.ListWorkflowExecutionsResponse
	requests  []*workflowservice.ListWorkflowExecutionsRequest
}

type scheduleWorkflowClient struct {
	workflowservice.WorkflowServiceClient
	response *workflowservice.ListSchedulesResponse
}

func (c *scheduleWorkflowClient) ListSchedules(
	context.Context,
	*workflowservice.ListSchedulesRequest,
	...grpc.CallOption,
) (*workflowservice.ListSchedulesResponse, error) {
	return c.response, nil
}

func TestGRPCRetrierRetriesTransientErrors(t *testing.T) {
	attempts := 0
	retrier := &grpcRetrier{policy: backoff.NewExponentialRetryPolicy(time.Nanosecond).WithMaximumAttempts(2)}
	err := retrier.do(t.Context(), "test", func() error {
		attempts++
		if attempts == 1 {
			return serviceerror.NewUnavailable("try again")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

func TestGRPCScheduleLoaderIncludesEntryWithoutInfo(t *testing.T) {
	client := &scheduleWorkflowClient{response: &workflowservice.ListSchedulesResponse{
		Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "missing-info"}},
	}}
	loader := NewGRPCScheduleLoader(client, io.Discard, nil, nil)
	var ids []string
	require.NoError(t, loader.ListScheduleIDs(t.Context(), "ns", func(id string) error {
		ids = append(ids, id)
		return nil
	}))
	require.Equal(t, []string{"missing-info"}, ids)
}

func (c *executionWorkflowClient) ListWorkflowExecutions(
	_ context.Context,
	req *workflowservice.ListWorkflowExecutionsRequest,
	_ ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	c.requests = append(c.requests, req)
	response := c.responses[0]
	c.responses = c.responses[1:]
	return response, nil
}

func TestGRPCLoaderRateLimiters(t *testing.T) {
	describeLimiter := NewNamespaceRateLimiter(10)
	listLimiter := NewNamespaceRateLimiter(2)

	schedules := NewGRPCScheduleLoader(nil, io.Discard, describeLimiter, listLimiter).(*grpcScheduleLoader)
	require.Same(t, describeLimiter, schedules.limiter)
	require.Same(t, listLimiter, schedules.listLimiter)

	executions := NewGRPCExecutionLoader(nil, io.Discard, listLimiter).(*grpcExecutionLoader)
	require.Same(t, listLimiter, executions.limiter)
}

func TestGRPCExecutionLoaderBatchesAndPaginates(t *testing.T) {
	start := mustParseTime("2026-05-19T18:00:00Z")
	end := mustParseTime("2026-05-19T22:00:00Z")
	row := func(scheduleID, workflowID, nominal string) *workflowpb.WorkflowExecutionInfo {
		t := mustParseTime(nominal)
		return &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			StartTime: timestamppb.New(t),
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"TemporalScheduledById":      payload.EncodeString(scheduleID),
				"TemporalScheduledStartTime": payload.EncodeString(nominal),
			}},
		}
	}
	client := &executionWorkflowClient{responses: []*workflowservice.ListWorkflowExecutionsResponse{
		{Executions: []*workflowpb.WorkflowExecutionInfo{row("s2", "w2", "2026-05-19T20:00:00Z")}, NextPageToken: []byte("next")},
		{Executions: []*workflowpb.WorkflowExecutionInfo{
			row("s1", "w1", "2026-05-19T19:00:00Z"),
			row("not-requested", "other", "2026-05-19T19:00:00Z"),
		}},
	}}
	loader := NewGRPCExecutionLoader(client, io.Discard, nil)

	got, err := loader.ListExecutions(t.Context(), "ns", []string{"s1", "s2"}, start, end)
	require.NoError(t, err)
	require.Len(t, got["s1"], 1)
	require.Equal(t, "w1", got["s1"][0].WorkflowID)
	require.Len(t, got["s2"], 1)
	require.Equal(t, "w2", got["s2"][0].WorkflowID)
	require.NotContains(t, got, "not-requested")
	require.Len(t, client.requests, 2)
	require.Equal(t, []byte("next"), client.requests[1].GetNextPageToken())
	require.Equal(t, int32(visibilityPageSize), client.requests[0].GetPageSize())
	require.Equal(t,
		`TemporalScheduledById IN ("s1", "s2") AND ((TemporalScheduledStartTime >= "2026-05-19T18:00:00Z" AND TemporalScheduledStartTime <= "2026-05-19T22:00:00Z") OR (TemporalScheduledStartTime <= "2026-05-19T18:00:00Z" AND (CloseTime >= "2026-05-19T18:00:00Z" OR CloseTime IS NULL)))`,
		client.requests[0].GetQuery())
}

func TestExecutionQueryEscapesScheduleIDs(t *testing.T) {
	query := executionQuery(
		[]string{`quote"and\\slash`},
		time.Date(2026, 5, 19, 18, 0, 0, 0, time.FixedZone("offset", -5*60*60)),
		mustParseTime("2026-05-20T00:00:00Z"),
	)
	require.Contains(t, query, `TemporalScheduledById IN ("quote\"and\\\\slash")`)
	require.Contains(t, query, `TemporalScheduledStartTime >= "2026-05-19T23:00:00Z"`)
}

func TestGRPCExecutionLoaderTwentyFiveSchedulesOneRequest(t *testing.T) {
	scheduleIDs := make([]string, executionBatchSize)
	rows := make([]*workflowpb.WorkflowExecutionInfo, 0, executionBatchSize*5)
	for i := range executionBatchSize {
		scheduleID := fmt.Sprintf("schedule-%02d", i)
		scheduleIDs[i] = scheduleID
		for j := range 5 {
			nominal := fmt.Sprintf("2026-05-19T%02d:00:00Z", j+1)
			rows = append(rows, &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: fmt.Sprintf("workflow-%02d-%d", i, j)},
				StartTime: timestamppb.New(mustParseTime(nominal)),
				SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
					"TemporalScheduledById":      payload.EncodeString(scheduleID),
					"TemporalScheduledStartTime": payload.EncodeString(nominal),
				}},
			})
		}
	}
	client := &executionWorkflowClient{responses: []*workflowservice.ListWorkflowExecutionsResponse{{Executions: rows}}}
	loader := NewGRPCExecutionLoader(client, io.Discard, nil)

	got, err := loader.ListExecutions(
		t.Context(), "ns", scheduleIDs,
		mustParseTime("2026-05-19T00:00:00Z"), mustParseTime("2026-05-20T00:00:00Z"),
	)
	require.NoError(t, err)
	require.Len(t, client.requests, 1)
	for _, scheduleID := range scheduleIDs {
		require.Len(t, got[scheduleID], 5)
	}
}

// TestDecodePayloads covers the skip-on-failure contract of decodeScheduledByID and decodeNominalStartTime so the
// caller can drop visibility rows with malformed search-attribute payloads instead of grouping them under empty keys.
func TestDecodePayloads(t *testing.T) {
	makePayload := func(encoding string, data []byte) *commonpb.Payload {
		return &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte(encoding)},
			Data:     data,
		}
	}

	t.Run("decodeScheduledByID nil payload returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(nil)
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID wrong encoding returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(makePayload("proto/binary", []byte("anything")))
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID malformed json returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(makePayload("json/plain", []byte("not-json-quoted")))
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID valid json returns the string", func(t *testing.T) {
		actual, ok := decodeScheduledByID(makePayload("json/plain", []byte(`"my-schedule-id"`)))
		require.True(t, ok)
		require.Equal(t, "my-schedule-id", actual)
	})

	t.Run("decodeNominalStartTime nil payload returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(nil)
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime wrong encoding returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(makePayload("proto/binary", []byte("anything")))
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime non-RFC3339 returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(makePayload("json/plain", []byte(`"not-a-timestamp"`)))
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime valid RFC3339 parses", func(t *testing.T) {
		actual, ok := decodeNominalStartTime(makePayload("json/plain", []byte(`"2026-05-19T18:00:00Z"`)))
		require.True(t, ok)
		require.Equal(t, mustParseTime("2026-05-19T18:00:00Z"), actual)
	})
}
