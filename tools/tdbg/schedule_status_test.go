package tdbg_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
)

type statusWorkflowClient struct {
	workflowservice.WorkflowServiceClient
	counts map[string]int64 // query -> count
	err    error

	requests []*workflowservice.CountWorkflowExecutionsRequest
}

func (c *statusWorkflowClient) CountWorkflowExecutions(
	_ context.Context,
	req *workflowservice.CountWorkflowExecutionsRequest,
	_ ...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	c.requests = append(c.requests, req)
	if c.err != nil {
		return nil, c.err
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: c.counts[req.Query]}, nil
}

func runScheduleStatus(t *testing.T, wf workflowservice.WorkflowServiceClient, args ...string) (stdoutStr, stderrStr string, err error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: wf}
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = factory
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	runArgs := append([]string{"tdbg"}, args...)
	err = app.Run(runArgs)
	return stdout.String(), stderr.String(), err
}

func TestScheduleStatus_Basic(t *testing.T) {
	v1Query := fmt.Sprintf("TemporalNamespaceDivision = '%s' AND ExecutionStatus = 'Running'", scheduler.NamespaceDivision)
	v2Query := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running'", chasm.SchedulerArchetypeID)
	wf := &statusWorkflowClient{counts: map[string]int64{
		v1Query: 42,
		v2Query: 7,
	}}

	stdout, _, err := runScheduleStatus(t, wf, "-n", "my-ns", "schedule", "migrate", "status")
	require.NoError(t, err)

	require.Len(t, wf.requests, 2)
	for _, req := range wf.requests {
		require.Equal(t, "my-ns", req.Namespace)
	}
	gotQueries := []string{wf.requests[0].Query, wf.requests[1].Query}
	require.ElementsMatch(t, []string{v1Query, v2Query}, gotQueries)

	require.Contains(t, stdout, "Namespace: my-ns")
	require.Contains(t, stdout, "V1 (workflow-backed): 42")
	require.Contains(t, stdout, "V2 (CHASM):           7")
	require.Contains(t, stdout, "Total:                49")
}

func TestScheduleStatus_DefaultsToDefaultNamespace(t *testing.T) {
	// --namespace/-n defaults to "default" (a global flag), mirroring `schedule migrate`'s
	// behavior of never requiring it explicitly.
	wf := &statusWorkflowClient{counts: map[string]int64{}}
	stdout, _, err := runScheduleStatus(t, wf, "schedule", "migrate", "status")
	require.NoError(t, err)

	require.Len(t, wf.requests, 2)
	for _, req := range wf.requests {
		require.Equal(t, "default", req.Namespace)
	}
	require.Contains(t, stdout, "Namespace: default")
}

func TestScheduleStatus_CountError(t *testing.T) {
	wf := &statusWorkflowClient{err: errors.New("boom")}
	_, _, err := runScheduleStatus(t, wf, "-n", "my-ns", "schedule", "migrate", "status")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to count")
	require.Contains(t, err.Error(), "boom")
}

// describeMutableStateAdminClient fakes AdminService.DescribeMutableState for the single-schedule
// `status --schedule-id` path, keyed by the requested WorkflowId. Any WorkflowId with no fixture
// registered returns NotFound, matching a real server's response for a nonexistent execution.
type describeMutableStateAdminClient struct {
	adminservice.AdminServiceClient
	responses map[string]*adminservice.DescribeMutableStateResponse // workflowID -> response

	requests []*adminservice.DescribeMutableStateRequest
}

func (c *describeMutableStateAdminClient) DescribeMutableState(
	_ context.Context,
	req *adminservice.DescribeMutableStateRequest,
	_ ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	c.requests = append(c.requests, req)
	if resp, ok := c.responses[req.GetExecution().GetWorkflowId()]; ok {
		return resp, nil
	}
	return nil, serviceerror.NewNotFound("not found")
}

// chasmSchedulerStateResponse builds a DescribeMutableState response for a CHASM Scheduler
// component at the tree root, as produced by the real server for a V2 schedule or CHASM-side
// sentinel.
func chasmSchedulerStateResponse(t *testing.T, sentinel bool) *adminservice.DescribeMutableStateResponse {
	t.Helper()
	blob, err := serialization.Encode(&schedulerpb.SchedulerState{Sentinel: sentinel})
	require.NoError(t, err)
	return &adminservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ChasmNodes: map[string]*persistencespb.ChasmNode{
				"": {Data: blob},
			},
		},
	}
}

// workflowTypeResponse builds a DescribeMutableState response for a plain workflow execution of
// the given type, as produced by the real server for a V1 scheduler workflow or a V1-side
// (dummy workflow) sentinel.
func workflowTypeResponse(typeName string) *adminservice.DescribeMutableStateResponse {
	return &adminservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{WorkflowTypeName: typeName},
		},
	}
}

func runScheduleStatusForSchedule(t *testing.T, admin adminservice.AdminServiceClient, args ...string) (stdoutStr, stderrStr string, err error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	factory := migrateClientFactory{admin: admin}
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = factory
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	runArgs := append([]string{"tdbg"}, args...)
	err = app.Run(runArgs)
	return stdout.String(), stderr.String(), err
}

func TestScheduleStatus_SingleSchedule_PrefixedInputStillDoesBothLookups(t *testing.T) {
	v1ID := primitives.ScheduleWorkflowIDPrefix + "foo"
	admin := &describeMutableStateAdminClient{responses: map[string]*adminservice.DescribeMutableStateResponse{
		v1ID: workflowTypeResponse(scheduler.WorkflowType),
	}}

	stdout, _, err := runScheduleStatusForSchedule(t, admin, "-n", "my-ns", "schedule", "migrate", "status", "--schedule-id", v1ID)
	require.NoError(t, err)

	require.Len(t, admin.requests, 2)
	gotIDs := []string{admin.requests[0].GetExecution().GetWorkflowId(), admin.requests[1].GetExecution().GetWorkflowId()}
	require.ElementsMatch(t, []string{v1ID, "foo"}, gotIDs)

	require.Contains(t, stdout, fmt.Sprintf("Schedule %q is a V1 (workflow-backed) schedule.\n", v1ID))
	require.NotContains(t, stdout, "Additionally")
}

func TestScheduleStatus_SingleSchedule_GenuineV1(t *testing.T) {
	admin := &describeMutableStateAdminClient{responses: map[string]*adminservice.DescribeMutableStateResponse{
		primitives.ScheduleWorkflowIDPrefix + "foo": workflowTypeResponse(scheduler.WorkflowType),
	}}

	stdout, _, err := runScheduleStatusForSchedule(t, admin, "-n", "my-ns", "schedule", "migrate", "status", "--schedule-id", "foo")
	require.NoError(t, err)

	require.Contains(t, stdout, `Schedule "foo" is a V1 (workflow-backed) schedule.`+"\n")
	require.NotContains(t, stdout, "Additionally")
	require.Contains(t, stdout, "[workflow ID temporal-sys-scheduler:foo]: genuine")
	require.Contains(t, stdout, "[business ID foo]: not found")
}

func TestScheduleStatus_SingleSchedule_GenuineV2(t *testing.T) {
	admin := &describeMutableStateAdminClient{responses: map[string]*adminservice.DescribeMutableStateResponse{
		"foo": chasmSchedulerStateResponse(t, false),
	}}

	stdout, _, err := runScheduleStatusForSchedule(t, admin, "-n", "my-ns", "schedule", "migrate", "status", "--schedule-id", "foo")
	require.NoError(t, err)

	require.Contains(t, stdout, `Schedule "foo" is a V2 (CHASM) schedule.`+"\n")
	require.NotContains(t, stdout, "Additionally")
}

func TestScheduleStatus_SingleSchedule_V1GenuineWithV2Sentinel(t *testing.T) {
	admin := &describeMutableStateAdminClient{responses: map[string]*adminservice.DescribeMutableStateResponse{
		primitives.ScheduleWorkflowIDPrefix + "foo": workflowTypeResponse(scheduler.WorkflowType),
		"foo": chasmSchedulerStateResponse(t, true),
	}}

	stdout, _, err := runScheduleStatusForSchedule(t, admin, "-n", "my-ns", "schedule", "migrate", "status", "--schedule-id", "foo")
	require.NoError(t, err)

	require.Contains(t, stdout, `Schedule "foo" is a V1 (workflow-backed) schedule.`+"\n")
	require.Contains(t, stdout, "Additionally, a placeholder (\"sentinel\") V2 entity exists")
	require.Contains(t, stdout, "V1→V2 migration is in progress")
}

func TestScheduleStatus_SingleSchedule_V1SentinelWithV2Genuine(t *testing.T) {
	admin := &describeMutableStateAdminClient{responses: map[string]*adminservice.DescribeMutableStateResponse{
		primitives.ScheduleWorkflowIDPrefix + "foo": workflowTypeResponse(dummy.DummyWFTypeName),
		"foo": chasmSchedulerStateResponse(t, false),
	}}

	stdout, _, err := runScheduleStatusForSchedule(t, admin, "-n", "my-ns", "schedule", "migrate", "status", "--schedule-id", "foo")
	require.NoError(t, err)

	require.Contains(t, stdout, `Schedule "foo" is a V2 (CHASM) schedule.`+"\n")
	require.Contains(t, stdout, "Additionally, a placeholder (\"sentinel\") V1 workflow exists")
	require.Contains(t, stdout, "V2→V1 rollback is in progress")
}

func TestScheduleStatus_SingleSchedule_NotFoundBothSides(t *testing.T) {
	admin := &describeMutableStateAdminClient{}

	stdout, _, err := runScheduleStatusForSchedule(t, admin, "-n", "my-ns", "schedule", "migrate", "status", "--schedule-id", "foo")
	require.NoError(t, err)

	require.Contains(t, stdout, `Schedule "foo" was not found as either a V1 (workflow-backed) or V2 (CHASM) schedule.`)
}

func TestScheduleStatus_SingleSchedule_UnexpectedBothGenuine(t *testing.T) {
	admin := &describeMutableStateAdminClient{responses: map[string]*adminservice.DescribeMutableStateResponse{
		primitives.ScheduleWorkflowIDPrefix + "foo": workflowTypeResponse(scheduler.WorkflowType),
		"foo": chasmSchedulerStateResponse(t, false),
	}}

	stdout, _, err := runScheduleStatusForSchedule(t, admin, "-n", "my-ns", "schedule", "migrate", "status", "--schedule-id", "foo")
	require.NoError(t, err)

	require.Contains(t, stdout, `Schedule "foo" is in an unexpected state`)
}
