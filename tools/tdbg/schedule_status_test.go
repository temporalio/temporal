package tdbg_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
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
