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

	mu       sync.Mutex
	requests []*adminservice.MigrateScheduleRequest
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
	pages    []*workflowservice.ListWorkflowExecutionsResponse
	next     int
	requests []*workflowservice.ListWorkflowExecutionsRequest
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

func TestMigrateSchedule_FromVisibility_DryRun(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{
		pages: []*workflowservice.ListWorkflowExecutionsResponse{
			{
				Executions:    []*workflowpb.WorkflowExecutionInfo{scheduleExecution("sched-a")},
				NextPageToken: []byte("page2"),
			},
			{
				Executions: []*workflowpb.WorkflowExecutionInfo{scheduleExecution("sched-b")},
			},
		},
	}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	stdout, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility")
	require.NoError(t, err)

	// Dry-run performs no migrations.
	require.Empty(t, admin.requests)
	// Default query is built from the CHASM scheduler archetype ID and scoped to --namespace.
	require.NotEmpty(t, wf.requests)
	expectedQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running'", chasm.SchedulerArchetypeID)
	require.Equal(t, expectedQuery, wf.requests[0].Query)
	require.Equal(t, "my-ns", wf.requests[0].Namespace)
	// Both pages are listed and reported.
	require.Contains(t, stdout, "[dry-run] would migrate my-ns/sched-a -> workflow")
	require.Contains(t, stdout, "[dry-run] would migrate my-ns/sched-b -> workflow")
	require.Contains(t, stdout, "Dry-run: 2 schedule(s)")
}

func TestMigrateSchedule_FromVisibility_Execute(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{
		pages: []*workflowservice.ListWorkflowExecutionsResponse{
			{
				Executions: []*workflowpb.WorkflowExecutionInfo{
					// CHASM execution: workflow id is the schedule id directly.
					scheduleExecution("sched-v2"),
					// V1 execution: the scheduler workflow-id prefix is trimmed off.
					scheduleExecution("temporal-sys-scheduler:sched-v1"),
				},
			},
		},
	}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility", "--execute")
	require.NoError(t, err)

	require.Len(t, admin.requests, 2)
	ids := []string{admin.requests[0].ScheduleId, admin.requests[1].ScheduleId}
	require.ElementsMatch(t, []string{"sched-v2", "sched-v1"}, ids)
	for _, req := range admin.requests {
		require.Equal(t, "my-ns", req.Namespace)
		require.Equal(t, adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW, req.Target)
		require.NotEmpty(t, req.RequestId)
	}
}

func TestMigrateSchedule_FromVisibility_Workers(t *testing.T) {
	admin := &migrateAdminClient{}
	const n = 12
	execs := make([]*workflowpb.WorkflowExecutionInfo, n)
	want := make([]string, n)
	for i := range n {
		id := fmt.Sprintf("sched-%d", i)
		execs[i] = scheduleExecution(id)
		want[i] = id
	}
	wf := &migrateWorkflowClient{
		pages: []*workflowservice.ListWorkflowExecutionsResponse{{Executions: execs}},
	}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility",
		"--execute", "--workers", "4")
	require.NoError(t, err)

	require.Len(t, admin.requests, n)
	got := make([]string, len(admin.requests))
	for i, req := range admin.requests {
		got[i] = req.ScheduleId
	}
	require.ElementsMatch(t, want, got)
}

func TestMigrateSchedule_FromVisibility_OutputLog(t *testing.T) {
	admin := &migrateAdminClient{failIDs: map[string]string{"sched-bad": "boom"}}
	wf := &migrateWorkflowClient{
		pages: []*workflowservice.ListWorkflowExecutionsResponse{{
			Executions: []*workflowpb.WorkflowExecutionInfo{
				scheduleExecution("sched-ok"),
				scheduleExecution("sched-bad"),
			},
		}},
	}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	logPath := filepath.Join(t.TempDir(), "migrations.jsonl")
	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility",
		"--execute", "--output-log", logPath)
	require.NoError(t, err)

	type logRec struct {
		Timestamp  string `json:"timestamp"`
		Namespace  string `json:"namespace"`
		ScheduleID string `json:"schedule_id"`
		Target     string `json:"target"`
		Status     string `json:"status"`
		Error      string `json:"error"`
	}
	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	byID := map[string]logRec{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var rec logRec
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		byID[rec.ScheduleID] = rec
	}

	require.Len(t, byID, 2)

	ok := byID["sched-ok"]
	require.Equal(t, "migrated", ok.Status)
	require.Equal(t, "my-ns", ok.Namespace)
	require.Equal(t, "workflow", ok.Target)
	require.Empty(t, ok.Error)
	require.NotEmpty(t, ok.Timestamp)

	bad := byID["sched-bad"]
	require.Equal(t, "failed", bad.Status)
	require.Contains(t, bad.Error, "boom")
}

func TestMigrateSchedule_FromVisibility_CustomQuery(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	customQuery := "TemporalNamespaceDivision = '403648407' AND ScheduleId = 'only-this'"
	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "workflow", "--from-visibility",
		"--query", customQuery)
	require.NoError(t, err)

	require.NotEmpty(t, wf.requests)
	require.Equal(t, customQuery, wf.requests[0].Query)
}

func TestMigrateSchedule_FromVisibility_DefaultQueryToChasm(t *testing.T) {
	admin := &migrateAdminClient{}
	wf := &migrateWorkflowClient{}
	factory := migrateClientFactory{admin: admin, workflow: wf}

	// Migrating to chasm (V1 -> V2) defaults to selecting running V1 (workflow-backed) schedules.
	_, _, err := runMigrate(t, factory,
		"-n", "my-ns", "schedule", "migrate", "--target", "chasm", "--from-visibility")
	require.NoError(t, err)

	require.NotEmpty(t, wf.requests)
	expectedQuery := fmt.Sprintf("TemporalNamespaceDivision = '%s' AND ExecutionStatus = 'Running'", scheduler.NamespaceDivision)
	require.Equal(t, expectedQuery, wf.requests[0].Query)
}

func TestMigrateSchedule_RejectsQueryWithoutFromVisibility(t *testing.T) {
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: &migrateWorkflowClient{}}
	_, _, err := runMigrate(t, factory,
		"schedule", "migrate", "--target", "workflow", "--schedule-id", "x", "--query", "ScheduleId = 'x'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "query")
	require.Contains(t, err.Error(), "from-visibility")
}

func TestMigrateSchedule_RejectsWorkersWithScheduleID(t *testing.T) {
	// --workers applies to the bulk modes (--from-visibility and stdin) but is meaningless when
	// migrating a single --schedule-id, so it is rejected rather than silently ignored.
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
	// Order is not guaranteed: the default worker pool migrates lines concurrently, so match
	// each schedule id to its namespace as a pair rather than asserting by index.
	byID := map[string]string{}
	for _, req := range admin.requests {
		byID[req.ScheduleId] = req.Namespace
		require.Equal(t, adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW, req.Target)
	}
	require.Equal(t, map[string]string{"sched-1": "ns-1", "sched-2": "ns-2"}, byID)
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
