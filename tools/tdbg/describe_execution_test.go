package tdbg_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type describeAdminClient struct {
	adminservice.AdminServiceClient
	resp *adminservice.DescribeMutableStateResponse
	req  *adminservice.DescribeMutableStateRequest
}

func (c *describeAdminClient) DescribeMutableState(
	_ context.Context,
	req *adminservice.DescribeMutableStateRequest,
	_ ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	c.req = req
	return c.resp, nil
}

// TestDescribeExecution_DumpsTimeSkippingInfo verifies that `tdbg execution describe` dumps the
// whole TimeSkippingInfo carried on the mutable state. This is what makes a dedicated tdbg command
// for TimeSkippingInfo unnecessary: the full struct (config, accumulated skip, fast-forward, and the
// circuit-breaker disabled reason/window counter) is already visible in the mutable-state dump.
func TestDescribeExecution_DumpsTimeSkippingInfo(t *testing.T) {
	dbMS := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			WorkflowId: "w",
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				// Enabled defaults to false: a run the circuit breaker has tripped.
				Config:                     &commonpb.TimeSkippingConfig{FastForward: durationpb.New(1500 * time.Hour)},
				AccumulatedSkippedDuration: durationpb.New(72 * time.Hour),
				DisabledReason:             persistencespb.TIME_SKIPPING_DISABLED_REASON_CIRCUIT_BREAKER,
				SkipWindowCount:            25,
				FastForwardInfo: &persistencespb.FastForwardInfo{
					TargetTime: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
				},
			},
			// Real mutable state always carries version histories; the describe command reads the
			// current one, so provide a minimal (single, empty) history.
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{{}},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "r"},
	}
	admin := &describeAdminClient{resp: &adminservice.DescribeMutableStateResponse{
		DatabaseMutableState: dbMS,
		ShardId:              "1",
		HistoryAddr:          "1.2.3.4:1234",
	}}
	factory := migrateClientFactory{admin: admin, workflow: &migrateWorkflowClient{}}

	stdout, _, err := runMigrate(t, factory,
		"-n", "my-ns", "execution", "describe", "--business-id", "w", "--run-id", "r")
	require.NoError(t, err)

	// The request was routed with our identifiers.
	require.NotNil(t, admin.req)
	require.Equal(t, "my-ns", admin.req.Namespace)
	require.Equal(t, "w", admin.req.Execution.GetWorkflowId())
	require.Equal(t, "r", admin.req.Execution.GetRunId())

	// The whole TimeSkippingInfo is present in the dump, including the circuit-breaker fields.
	require.Contains(t, stdout, "timeSkippingInfo")
	require.Contains(t, stdout, "accumulatedSkippedDuration")
	require.Contains(t, stdout, "fastForwardInfo")
	require.Contains(t, stdout, "skipWindowCount")
	require.Contains(t, stdout, "TIME_SKIPPING_DISABLED_REASON_CIRCUIT_BREAKER")
}
