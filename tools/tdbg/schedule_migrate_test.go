package tdbg_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
)

type migrateAdminClient struct {
	adminservice.AdminServiceClient
	err     error
	failIDs map[string]string // scheduleID -> error message

	mu            sync.Mutex
	requests      []*adminservice.MigrateScheduleRequest
	batchRequests []*adminservice.StartAdminBatchOperationRequest
}

func (c *migrateAdminClient) StartAdminBatchOperation(
	_ context.Context,
	req *adminservice.StartAdminBatchOperationRequest,
	_ ...grpc.CallOption,
) (*adminservice.StartAdminBatchOperationResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batchRequests = append(c.batchRequests, req)
	return &adminservice.StartAdminBatchOperationResponse{}, c.err
}

func (c *migrateAdminClient) MigrateSchedule(
	_ context.Context,
	req *adminservice.MigrateScheduleRequest,
	_ ...grpc.CallOption,
) (*adminservice.MigrateScheduleResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	if msg, ok := c.failIDs[req.ScheduleId]; ok {
		return nil, errors.New(msg)
	}
	return &adminservice.MigrateScheduleResponse{}, nil
}

type migrateWorkflowClient struct {
	workflowservice.WorkflowServiceClient
	pages         []*workflowservice.ListWorkflowExecutionsResponse
	next          int
	requests      []*workflowservice.ListWorkflowExecutionsRequest
	countRequests []*workflowservice.CountWorkflowExecutionsRequest
	count         int64
}

func (c *migrateWorkflowClient) CountWorkflowExecutions(
	_ context.Context,
	req *workflowservice.CountWorkflowExecutionsRequest,
	_ ...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	c.countRequests = append(c.countRequests, req)
	return &workflowservice.CountWorkflowExecutionsResponse{Count: c.count}, nil
}

func (c *migrateWorkflowClient) ListWorkflowExecutions(
	_ context.Context,
	req *workflowservice.ListWorkflowExecutionsRequest,
	_ ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	c.requests = append(c.requests, req)
	if c.next >= len(c.pages) {
		return &workflowservice.ListWorkflowExecutionsResponse{}, nil
	}
	resp := c.pages[c.next]
	c.next++
	return resp, nil
}

type migrateClientFactory struct {
	admin    adminservice.AdminServiceClient
	workflow workflowservice.WorkflowServiceClient
}

func (f migrateClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return f.admin
}

func (f migrateClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return f.workflow
}

func scheduleExecution(workflowID string) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	}
}

func runMigrate(t *testing.T, factory tdbg.ClientFactory, args ...string) (stdoutStr, stderrStr string, err error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = factory
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	runArgs := append([]string{"tdbg"}, args...)
	err = app.Run(runArgs)
	return stdout.String(), stderr.String(), err
}

func TestMigrateSchedule_FromVisibility_StartsAdminBatch(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{count: 3}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	stdout, _, err := runMigrate(t, factory,
		"-n", "payments", "schedule", "migrate",
		"--target", "workflow",
		"--from-visibility",
		"--query", "WorkflowId STARTS_WITH 'critical-'",
		"--execute",
		"--reason", "rollback",
		"--job-id", "rollback-payments")
	require.NoError(t, err)
	require.Contains(t, stdout, "rollback-payments")

	require.Len(t, wf.countRequests, 1)
	require.Equal(t, "payments", wf.countRequests[0].Namespace)
	require.Equal(t,
		fmt.Sprintf("(TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running') AND (WorkflowId STARTS_WITH 'critical-')", chasm.SchedulerArchetypeID),
		wf.countRequests[0].Query,
	)

	require.Len(t, admin.batchRequests, 1)
	req := admin.batchRequests[0]
	require.Equal(t, "payments", req.Namespace)
	require.Equal(t, "rollback-payments", req.JobId)
	require.Equal(t, "rollback", req.Reason)
	require.Equal(t, "WorkflowId STARTS_WITH 'critical-'", req.VisibilityQuery)
	op := req.GetMigrateSchedulesOperation()
	require.NotNil(t, op)
	require.Equal(t, adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW, op.Target)
	require.Empty(t, admin.requests)
}

func TestMigrateSchedule_FromVisibility_DryRun(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{count: 2}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	stdout, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility")
	require.NoError(t, err)

	require.Empty(t, admin.requests)
	require.Empty(t, admin.batchRequests)
	require.Len(t, wf.countRequests, 1)
	expectedQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running'", chasm.SchedulerArchetypeID)
	require.Equal(t, expectedQuery, wf.countRequests[0].Query)
	require.Equal(t, "my-ns", wf.countRequests[0].Namespace)
	require.Contains(t, stdout, "Dry-run: 2 schedule(s)")
}

func TestMigrateSchedule_FromVisibility_Execute(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{count: 2}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility", "--execute",
		"--reason", "rollback", "--job-id", "rollback-job")
	require.NoError(t, err)

	require.Empty(t, admin.requests)
	require.Len(t, admin.batchRequests, 1)
	req := admin.batchRequests[0]
	require.Equal(t, "my-ns", req.Namespace)
	require.Equal(t, "rollback-job", req.JobId)
	require.Equal(t, "rollback", req.Reason)
	require.Equal(t, adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW, req.GetMigrateSchedulesOperation().GetTarget())
}

func TestMigrateSchedule_FromVisibility_RejectsWorkers(t *testing.T) {
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: &migrateWorkflowClient{}}

	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility",
		"--execute", "--workers", "4")
	require.ErrorContains(t, err, "only valid when piping")
}

func TestMigrateSchedule_FromVisibility_RejectsOutputLog(t *testing.T) {
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: &migrateWorkflowClient{}}

	logPath := filepath.Join(t.TempDir(), "migrations.jsonl")
	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility",
		"--execute", "--output-log", logPath)
	require.ErrorContains(t, err, "only valid when piping")
}

func TestMigrateSchedule_FromVisibility_CustomQuery(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	customQuery := "WorkflowId = 'only-this'"
	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility",
		"--query", customQuery)
	require.NoError(t, err)

	require.Len(t, wf.countRequests, 1)
	require.Equal(t,
		fmt.Sprintf("(TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running') AND (%s)", chasm.SchedulerArchetypeID, customQuery),
		wf.countRequests[0].Query,
	)
}

func TestMigrateSchedule_FromVisibility_DefaultQueryToChasm(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	// Migrating to chasm (V1 -> V2) defaults to selecting running V1 (workflow-backed) schedules.
	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "chasm", "--from-visibility")
	require.NoError(t, err)

	require.Len(t, wf.countRequests, 1)
	expectedQuery := fmt.Sprintf("TemporalNamespaceDivision = '%s' AND ExecutionStatus = 'Running'", scheduler.NamespaceDivision)
	require.Equal(t, expectedQuery, wf.countRequests[0].Query)
}

func TestMigrateSchedule_RejectsQueryWithoutFromVisibility(t *testing.T) {
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: &migrateWorkflowClient{}}
	_, _, err := runMigrate(t, factory,
		"schedule", "migrate", "--target", "workflow", "--schedule-id", "x", "--query", "WorkflowId = 'x'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "query")
	require.Contains(t, err.Error(), "from-visibility")
}

func TestMigrateSchedule_RejectsWorkersWithScheduleID(t *testing.T) {
	// --workers controls client-side stdin fan-out and is meaningless for a single schedule.
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: &migrateWorkflowClient{}}
	_, _, err := runMigrate(t, factory,
		"schedule", "migrate", "--target", "workflow", "--schedule-id", "x", "--workers", "4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "workers")
}

func TestMigrateSchedule_FromVisibility_RejectsScheduleID(t *testing.T) {
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: &migrateWorkflowClient{}}
	_, _, err := runMigrate(t, factory,
		"schedule", "migrate", "--target", "workflow", "--from-visibility", "--schedule-id", "x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "from-visibility")
	require.Contains(t, err.Error(), "schedule-id")
}

func TestMigrateSchedule_Stdin_Execute(t *testing.T) {
	admin := &migrateAdminClient{}
	factory := migrateClientFactory{admin: admin, workflow: &migrateWorkflowClient{}}

	stdin := strings.Join([]string{
		`{"namespace":"ns-1","schedule_id":"sched-1"}`,
		``, // blank lines are skipped
		`{"namespace":"ns-2","schedule_id":"sched-2"}`,
	}, "\n")

	withStdin(t, stdin, func() {
		_, _, err := runMigrate(t, factory,
			"schedule", "migrate", "--target", "workflow", "--execute")
		require.NoError(t, err)
	})

	require.Len(t, admin.requests, 2)
	// The default worker count is >1, so migrations run concurrently and the order
	// in which they are recorded is not deterministic. Assert on the set of
	// (namespace, schedule_id) pairs rather than their positions.
	type migratePair struct{ namespace, scheduleID string }
	got := make([]migratePair, len(admin.requests))
	for i, req := range admin.requests {
		got[i] = migratePair{namespace: req.Namespace, scheduleID: req.ScheduleId}
		require.Equal(t, adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW, req.Target)
	}
	require.ElementsMatch(t, []migratePair{
		{namespace: "ns-1", scheduleID: "sched-1"},
		{namespace: "ns-2", scheduleID: "sched-2"},
	}, got)
}

func TestMigrateSchedule_Stdin_Workers(t *testing.T) {
	admin := &migrateAdminClient{}
	factory := migrateClientFactory{admin: admin, workflow: &migrateWorkflowClient{}}

	const n = 12
	lines := make([]string, n)
	want := make([]string, n)
	for i := range n {
		id := fmt.Sprintf("sched-%d", i)
		lines[i] = fmt.Sprintf(`{"namespace":"my-ns","schedule_id":%q}`, id)
		want[i] = id
	}

	withStdin(t, strings.Join(lines, "\n"), func() {
		_, _, err := runMigrate(t, factory,
			"schedule", "migrate", "--target", "workflow", "--execute", "--workers", "4")
		require.NoError(t, err)
	})

	require.Len(t, admin.requests, n)
	got := make([]string, len(admin.requests))
	for i, req := range admin.requests {
		got[i] = req.ScheduleId
	}
	// Order is not guaranteed with multiple workers.
	require.ElementsMatch(t, want, got)
}

func TestMigrateSchedule_Stdin_DryRun(t *testing.T) {
	admin := &migrateAdminClient{}
	factory := migrateClientFactory{admin: admin, workflow: &migrateWorkflowClient{}}

	withStdin(t, `{"namespace":"ns-1","schedule_id":"sched-1"}`, func() {
		stdout, _, err := runMigrate(t, factory, "schedule", "migrate", "--target", "workflow")
		require.NoError(t, err)
		require.Contains(t, stdout, "[dry-run] would migrate ns-1/sched-1 -> workflow")
	})

	require.Empty(t, admin.requests)
}

func TestMigrateSchedule_Stdin_OutputLog(t *testing.T) {
	admin := &migrateAdminClient{}
	factory := migrateClientFactory{admin: admin, workflow: &migrateWorkflowClient{}}

	logPath := filepath.Join(t.TempDir(), "migrations.jsonl")
	withStdin(t, `{"namespace":"ns-1","schedule_id":"sched-1"}`, func() {
		_, _, err := runMigrate(t, factory,
			"schedule", "migrate", "--target", "workflow", "--execute", "--output-log", logPath)
		require.NoError(t, err)
	})

	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	var rec struct {
		Namespace  string `json:"namespace"`
		ScheduleID string `json:"schedule_id"`
		Target     string `json:"target"`
		Status     string `json:"status"`
	}
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(string(data))), &rec))
	require.Equal(t, "ns-1", rec.Namespace)
	require.Equal(t, "sched-1", rec.ScheduleID)
	require.Equal(t, "workflow", rec.Target)
	require.Equal(t, "migrated", rec.Status)
}

// withStdin redirects os.Stdin to a temp file containing content for the duration of fn.
// A regular file is not a character device, so the command's piped-stdin detection treats it
// as piped input.
//
// NOTE: os.Stdin is process-global, so tests using withStdin must NOT call t.Parallel().
// TODO: inject the input reader through tdbg.Params instead of mutating os.Stdin, which would
// remove this constraint (deferred to a follow-up PR to keep this change small).
func withStdin(t *testing.T, content string, fn func()) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdin")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	_, err = f.Seek(0, 0)
	require.NoError(t, err)

	orig := os.Stdin
	os.Stdin = f
	defer func() {
		os.Stdin = orig
		_ = f.Close()
	}()
	fn()
}
