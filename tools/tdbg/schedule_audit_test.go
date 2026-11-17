package tdbg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tools/tdbg/scheduleaudit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuditInputs_Validate(t *testing.T) {
	now := mustParseTime("2026-05-19T21:00:00Z")
	base := func() *auditInputs {
		return &auditInputs{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
		}
	}

	t.Run("valid passes", func(t *testing.T) {
		require.NoError(t, base().validate(now))
	})

	t.Run("schedule-id without namespace is rejected", func(t *testing.T) {
		in := base()
		in.Namespace = ""
		in.ScheduleID = "sched1"
		require.ErrorContains(t, in.validate(now), "--schedule-id is only valid with --namespace")
	})

	t.Run("all-namespaces with namespace or file is rejected", func(t *testing.T) {
		in := base()
		in.AllNamespaces = true
		require.ErrorContains(t, in.validate(now), "--all-namespaces cannot be combined")
		in.Namespace = ""
		in.File = "targets.jsonl"
		require.ErrorContains(t, in.validate(now), "--all-namespaces cannot be combined")
		in.File = ""
		require.NoError(t, in.validate(now))
	})

	t.Run("exclude-namespace requires all-namespaces", func(t *testing.T) {
		in := base()
		in.ExcludeNamespaces = []string{"temporal-system"}
		require.ErrorContains(t, in.validate(now), "--exclude-namespace is only valid with --all-namespaces")
	})

	t.Run("end must be after start", func(t *testing.T) {
		in := base()
		in.WindowStart = mustParseTime("2026-05-19T20:00:00Z")
		in.WindowEnd = mustParseTime("2026-05-19T18:00:00Z")
		require.ErrorContains(t, in.validate(now), "must be after")
	})

	t.Run("end == start rejected", func(t *testing.T) {
		in := base()
		in.WindowEnd = in.WindowStart
		require.ErrorContains(t, in.validate(now), "must be after")
	})

	t.Run("future end is rejected", func(t *testing.T) {
		in := base()
		in.WindowEnd = now.Add(time.Second)
		require.ErrorContains(t, in.validate(now), "must not be in the future")
	})
}

func TestParseDuration(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{"24h", 24 * time.Hour, false},
		{"3d", 72 * time.Hour, false},
		{"1.5d", 36 * time.Hour, false},
		{"90m", 90 * time.Minute, false},
		{"0s", 0, false},
		{"2d12h", 60 * time.Hour, false},
		{"", 0, true},
		{"3w", 0, true}, // weeks unsupported, matching the CLI
		{"banana", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseDuration(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestResolveBound(t *testing.T) {
	now := mustParseTime("2026-07-06T12:00:00Z")

	t.Run("duration is interpreted as before now", func(t *testing.T) {
		got, ok, err := resolveBound("3d", "", now)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, mustParseTime("2026-07-03T12:00:00Z"), got)
	})

	t.Run("timestamp is absolute", func(t *testing.T) {
		got, ok, err := resolveBound("", "2026-07-01T00:00:00Z", now)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, mustParseTime("2026-07-01T00:00:00Z"), got)
	})

	t.Run("neither set reports not provided", func(t *testing.T) {
		_, ok, err := resolveBound("", "", now)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("both set is rejected", func(t *testing.T) {
		_, _, err := resolveBound("24h", "2026-07-01T00:00:00Z", now)
		require.ErrorContains(t, err, "not both")
	})

	t.Run("bad duration surfaces the error", func(t *testing.T) {
		_, ok, err := resolveBound("nope", "", now)
		require.True(t, ok)
		require.Error(t, err)
	})

	t.Run("bad timestamp surfaces the error", func(t *testing.T) {
		_, ok, err := resolveBound("", "not-a-time", now)
		require.True(t, ok)
		require.Error(t, err)
	})
}

func TestStreamJSONLTargets(t *testing.T) {
	collect := func(input string) ([]scheduleaudit.Target, error) {
		var got []scheduleaudit.Target
		resolve := func(id string) (string, error) {
			if id == "unknown-id" {
				return "", errors.New("namespace ID not found")
			}
			return "name-of-" + id, nil
		}
		err := streamJSONLTargets(strings.NewReader(input), resolve, func(t scheduleaudit.Target) error {
			got = append(got, t)
			return nil
		})
		return got, err
	}

	t.Run("mixed with and without schedule_id", func(t *testing.T) {
		got, err := collect(`{"namespace":"ns1"}` + "\n" + `{"namespace":"ns2","schedule_id":"s2"}` + "\n")
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{
			{Namespace: "ns1"},
			{Namespace: "ns2", ScheduleID: "s2"},
		}, got)
	})

	t.Run("surrounding whitespace and blank lines are tolerated", func(t *testing.T) {
		got, err := collect("\n  " + `{"namespace":"ns1"}` + "  \n\n" + `{"namespace":"ns2"}` + "\n")
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "ns1"}, {Namespace: "ns2"}}, got)
	})

	t.Run("empty input yields no targets", func(t *testing.T) {
		got, err := collect("")
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("missing namespace is rejected", func(t *testing.T) {
		_, err := collect(`{"schedule_id":"s1"}` + "\n")
		require.ErrorContains(t, err, "exactly one of namespace and namespace_id")
	})

	t.Run("namespace_id is resolved to a name", func(t *testing.T) {
		got, err := collect(`{"namespace_id":"id1","schedule_id":"s1"}`)
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "name-of-id1", ScheduleID: "s1"}}, got)
	})

	t.Run("namespace and namespace_id together are rejected", func(t *testing.T) {
		_, err := collect(`{"namespace":"ns1","namespace_id":"id1"}`)
		require.ErrorContains(t, err, "exactly one of namespace and namespace_id")
	})

	t.Run("unresolvable namespace_id is rejected", func(t *testing.T) {
		_, err := collect(`{"namespace_id":"unknown-id"}`)
		require.ErrorContains(t, err, "namespace ID not found")
	})

	t.Run("unknown field cannot expand scope", func(t *testing.T) {
		_, err := collect(`{"namespace":"prod","scheduleId":"daily"}`)
		require.ErrorContains(t, err, "unknown field")
	})

	t.Run("malformed json is rejected", func(t *testing.T) {
		_, err := collect(`{"namespace":` + "\n")
		require.ErrorContains(t, err, "decode target")
	})
}

func TestProduceTargets(t *testing.T) {
	produce := func(in *auditInputs) ([]scheduleaudit.Target, error) {
		out := make(chan scheduleaudit.Target, 64)
		err := in.produceTargets(context.Background(), out)
		close(out)
		var got []scheduleaudit.Target
		for t := range out {
			got = append(got, t)
		}
		return got, err
	}

	t.Run("--namespace matching the stream passes targets through", func(t *testing.T) {
		got, err := produce(&auditInputs{
			Namespace: "ns1",
			Stdin:     strings.NewReader(`{"namespace":"ns1"}` + "\n" + `{"namespace":"ns1","schedule_id":"s1"}` + "\n"),
		})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{
			{Namespace: "ns1"},
			{Namespace: "ns1", ScheduleID: "s1"},
		}, got)
	})

	t.Run("--namespace not matching a stream target errors", func(t *testing.T) {
		_, err := produce(&auditInputs{
			Namespace: "ns1",
			Stdin:     strings.NewReader(`{"namespace":"ns2"}` + "\n"),
		})
		require.ErrorContains(t, err, `does not match --namespace "ns1"`)
	})

	t.Run("--schedule-id not matching a stream target errors", func(t *testing.T) {
		_, err := produce(&auditInputs{
			Namespace:  "ns1",
			ScheduleID: "s1",
			Stdin:      strings.NewReader(`{"namespace":"ns1","schedule_id":"s2"}` + "\n"),
		})
		require.ErrorContains(t, err, `does not match --schedule-id "s1"`)
	})

	t.Run("--namespace + --schedule-id matching the stream passes through", func(t *testing.T) {
		got, err := produce(&auditInputs{
			Namespace:  "ns1",
			ScheduleID: "s1",
			Stdin:      strings.NewReader(`{"namespace":"ns1","schedule_id":"s1"}` + "\n"),
		})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "ns1", ScheduleID: "s1"}}, got)
	})

	t.Run("no stream: --namespace alone audits the whole namespace", func(t *testing.T) {
		got, err := produce(&auditInputs{Namespace: "ns1", Stdin: devNull(t)})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "ns1"}}, got)
	})

	t.Run("no stream: --namespace + --schedule-id audits one schedule", func(t *testing.T) {
		got, err := produce(&auditInputs{Namespace: "ns1", ScheduleID: "s1", Stdin: devNull(t)})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "ns1", ScheduleID: "s1"}}, got)
	})

	t.Run("no stream and no flags errors", func(t *testing.T) {
		_, err := produce(&auditInputs{Stdin: devNull(t)})
		require.ErrorContains(t, err, "no targets")
	})

	t.Run("no flags reads the stream from stdin", func(t *testing.T) {
		got, err := produce(&auditInputs{
			Stdin: strings.NewReader(`{"namespace":"ns1"}` + "\n" + `{"namespace":"ns2","schedule_id":"s2"}` + "\n"),
		})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{
			{Namespace: "ns1"},
			{Namespace: "ns2", ScheduleID: "s2"},
		}, got)
	})

	t.Run("stream from --file path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "targets.jsonl")
		require.NoError(t, os.WriteFile(path, []byte(`{"namespace":"ns1"}`+"\n"+`{"namespace":"ns2","schedule_id":"s2"}`+"\n"), 0o600))
		got, err := produce(&auditInputs{File: path})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{
			{Namespace: "ns1"},
			{Namespace: "ns2", ScheduleID: "s2"},
		}, got)
	})

	t.Run("--file that doesn't exist errors", func(t *testing.T) {
		_, err := produce(&auditInputs{File: filepath.Join(t.TempDir(), "missing.jsonl")})
		require.ErrorContains(t, err, "no such file or directory")
	})

	t.Run("--all-namespaces lists namespaces, skips exclusions by name or ID, and ignores stdin", func(t *testing.T) {
		got, err := produce(&auditInputs{
			AllNamespaces:     true,
			ExcludeNamespaces: []string{"temporal-system", "id-c"},
			Stdin:             strings.NewReader(`{"namespace":"from-stdin"}`),
			Namespaces: &fakeNamespaceDirectory{namespaces: [][2]string{
				{"temporal-system", "id-sys"}, {"a", "id-a"}, {"b", "id-b"}, {"c", "id-c"},
			}},
		})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "a"}, {Namespace: "b"}}, got)
	})

	t.Run("stream namespace_id is resolved once and checked against --namespace", func(t *testing.T) {
		dir := &fakeNamespaceDirectory{namespaces: [][2]string{{"ns1", "id1"}}}
		got, err := produce(&auditInputs{
			Namespace:  "ns1",
			Namespaces: dir,
			Stdin:      strings.NewReader(`{"namespace_id":"id1"}` + "\n" + `{"namespace_id":"id1","schedule_id":"s1"}` + "\n"),
		})
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "ns1"}, {Namespace: "ns1", ScheduleID: "s1"}}, got)
		require.Equal(t, 1, dir.lookups)
	})

}

func TestOpenStreamExplicitDashReadsInteractiveStdin(t *testing.T) {
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	if !isTerminal(f) {
		t.Skip("os.DevNull is not reported as a character device on this platform")
	}

	in := &auditInputs{File: "-", Stdin: f}
	_, _, hasStream, err := in.openStream()
	require.NoError(t, err)
	require.True(t, hasStream)

	in.File = ""
	_, _, hasStream, err = in.openStream()
	require.NoError(t, err)
	require.False(t, hasStream)
}

// devNull opens os.DevNull, a character device, so openStream treats stdin as an interactive terminal with no stream.
func devNull(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

type fakeNamespaceDirectory struct {
	namespaces [][2]string // name, ID
	lookups    int
}

func (d *fakeNamespaceDirectory) ListNamespaces(_ context.Context, yield func(name, id string) error) error {
	for _, ns := range d.namespaces {
		if err := yield(ns[0], ns[1]); err != nil {
			return err
		}
	}
	return nil
}

func (d *fakeNamespaceDirectory) NamespaceName(_ context.Context, id string) (string, error) {
	d.lookups++
	for _, ns := range d.namespaces {
		if ns[1] == id {
			return ns[0], nil
		}
	}
	return "", status.Error(codes.NotFound, "namespace not found")
}

type namespaceListClient struct {
	workflowservice.WorkflowServiceClient
	requests []*workflowservice.ListNamespacesRequest
}

func (c *namespaceListClient) ListNamespaces(
	_ context.Context,
	req *workflowservice.ListNamespacesRequest,
	_ ...grpc.CallOption,
) (*workflowservice.ListNamespacesResponse, error) {
	c.requests = append(c.requests, req)
	ns := func(name string, state enumspb.NamespaceState) *workflowservice.DescribeNamespaceResponse {
		return &workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Name: name, Id: name + "-id", State: state}}
	}
	if len(req.GetNextPageToken()) == 0 {
		return &workflowservice.ListNamespacesResponse{
			Namespaces:    []*workflowservice.DescribeNamespaceResponse{ns("a", enumspb.NAMESPACE_STATE_REGISTERED), ns("gone", enumspb.NAMESPACE_STATE_DELETED)},
			NextPageToken: []byte("page2"),
		}, nil
	}
	return &workflowservice.ListNamespacesResponse{
		Namespaces: []*workflowservice.DescribeNamespaceResponse{ns("old", enumspb.NAMESPACE_STATE_DEPRECATED), ns("b", enumspb.NAMESPACE_STATE_REGISTERED)},
	}, nil
}

func TestGRPCNamespaceDirectoryListsRegisteredNamespaces(t *testing.T) {
	client := &namespaceListClient{}
	var got [][2]string
	require.NoError(t, grpcNamespaceDirectory{client: client}.ListNamespaces(t.Context(), func(name, id string) error {
		got = append(got, [2]string{name, id})
		return nil
	}))
	require.Equal(t, [][2]string{{"a", "a-id"}, {"b", "b-id"}}, got)
	require.Len(t, client.requests, 2)
}

func TestClassifyScheduleBackend(t *testing.T) {
	genuineWorkflow := v1ScheduleStatus{found: true, running: true, workflowType: scheduler.WorkflowType}
	sentinelWorkflow := v1ScheduleStatus{found: true, running: true, workflowType: dummy.DummyWFTypeName}
	closedWorkflow := v1ScheduleStatus{found: true, workflowType: scheduler.WorkflowType}
	genuineChasm := v2ScheduleStatus{found: true, running: true}
	sentinelChasm := v2ScheduleStatus{found: true, running: true, sentinel: true}
	closedChasm := v2ScheduleStatus{found: true}
	for _, tc := range []struct {
		name     string
		workflow v1ScheduleStatus
		chasm    v2ScheduleStatus
		want     string
	}{
		{"genuine workflow", genuineWorkflow, v2ScheduleStatus{}, scheduleaudit.BackendWorkflow},
		{"genuine workflow with chasm sentinel", genuineWorkflow, sentinelChasm, scheduleaudit.BackendWorkflow},
		{"genuine workflow with closed chasm", genuineWorkflow, closedChasm, scheduleaudit.BackendWorkflow},
		{"genuine chasm", v1ScheduleStatus{}, genuineChasm, scheduleaudit.BackendChasm},
		{"genuine chasm with workflow sentinel", sentinelWorkflow, genuineChasm, scheduleaudit.BackendChasm},
		{"genuine chasm with closed workflow", closedWorkflow, genuineChasm, scheduleaudit.BackendChasm},
		{"nothing", v1ScheduleStatus{}, v2ScheduleStatus{}, scheduleaudit.BackendNotFound},
		{"only a chasm sentinel", v1ScheduleStatus{}, sentinelChasm, scheduleaudit.BackendUnexpected},
		{"both genuine", genuineWorkflow, genuineChasm, scheduleaudit.BackendUnexpected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyScheduleBackend(tc.workflow, tc.chasm))
		})
	}
}

type auditWorkflowClient struct {
	workflowservice.WorkflowServiceClient
}

func (c *auditWorkflowClient) DescribeNamespace(context.Context, *workflowservice.DescribeNamespaceRequest, ...grpc.CallOption) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Id: "ns-id"}}, nil
}
func (c *auditWorkflowClient) DescribeSchedule(context.Context, *workflowservice.DescribeScheduleRequest, ...grpc.CallOption) (*workflowservice.DescribeScheduleResponse, error) {
	return &workflowservice.DescribeScheduleResponse{Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{CronString: []string{"0 * * * *"}}}}, nil
}
func (c *auditWorkflowClient) ListWorkflowExecutions(context.Context, *workflowservice.ListWorkflowExecutionsRequest, ...grpc.CallOption) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return &workflowservice.ListWorkflowExecutionsResponse{}, nil
}

// auditAdminClient reports every schedule as absent on both sides, the way a real server answers a lookup.
type auditAdminClient struct {
	adminservice.AdminServiceClient
}

func (c *auditAdminClient) DescribeMutableState(context.Context, *adminservice.DescribeMutableStateRequest, ...grpc.CallOption) (*adminservice.DescribeMutableStateResponse, error) {
	return nil, status.Error(codes.NotFound, "not found")
}

type auditClientFactory struct{ ClientFactory }

func (f *auditClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return &auditWorkflowClient{}
}

func (f *auditClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return &auditAdminClient{}
}

func TestAuditCommandWriters(t *testing.T) {
	for _, quiet := range []bool{false, true} {
		t.Run(fmt.Sprint(quiet), func(t *testing.T) {
			var output, diagnostics bytes.Buffer
			app := NewCliApp(func(p *Params) {
				p.Writer = &output
				p.ErrWriter = &diagnostics
				p.ClientFactory = &auditClientFactory{}
			})
			app.ExitErrHandler = func(*cli.Context, error) {}
			targets := filepath.Join(t.TempDir(), "targets.jsonl")
			require.NoError(t, os.WriteFile(targets, []byte(`{"namespace":"ns","schedule_id":"s"}`), 0600))
			args := []string{"tdbg", "schedule", "audit", "--file", targets, "--lookback-start", "3h", "--lookback-end", "1h"}
			if quiet {
				args = append(args, "--quiet")
			}
			require.NoError(t, app.Run(args))
			require.Contains(t, output.String(), `"schedule_id":"s"`)
			require.Contains(t, output.String(), `"backend":"not_found"`)
			if quiet {
				require.Empty(t, diagnostics.String())
			} else {
				require.Contains(t, diagnostics.String(), "audit complete:")
			}
		})
	}
}
