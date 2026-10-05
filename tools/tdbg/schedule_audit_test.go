package tdbg

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	namespacepb "go.temporal.io/api/namespace/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tools/tdbg/scheduleaudit"
	"google.golang.org/grpc"
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
		err := streamJSONLTargets(strings.NewReader(input), func(t scheduleaudit.Target) error {
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
		require.ErrorContains(t, err, "namespace is empty")
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

type auditClientFactory struct{ ClientFactory }

func (f *auditClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return &auditWorkflowClient{}
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
			if quiet {
				require.Empty(t, diagnostics.String())
			} else {
				require.Contains(t, diagnostics.String(), "audit complete:")
			}
		})
	}
}
