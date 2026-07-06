package tdbg

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/tdbg/scheduleaudit"
)

func TestAuditInputs_Validate(t *testing.T) {
	base := func() *auditInputs {
		return &auditInputs{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
		}
	}

	t.Run("valid passes", func(t *testing.T) {
		require.NoError(t, base().validate())
	})

	t.Run("schedule-id without namespace is rejected", func(t *testing.T) {
		in := base()
		in.Namespace = ""
		in.ScheduleID = "sched1"
		require.ErrorContains(t, in.validate(), "--schedule-id is only valid with --namespace")
	})

	t.Run("end must be after start", func(t *testing.T) {
		in := base()
		in.WindowStart = mustParseTime("2026-05-19T20:00:00Z")
		in.WindowEnd = mustParseTime("2026-05-19T18:00:00Z")
		require.ErrorContains(t, in.validate(), "must be after")
	})

	t.Run("end == start rejected", func(t *testing.T) {
		in := base()
		in.WindowEnd = in.WindowStart
		require.ErrorContains(t, in.validate(), "must be after")
	})
}

func TestAuditCommandIsRegistered(t *testing.T) {
	app := NewCliApp()
	var found bool
	for _, top := range app.Commands {
		if top.Name != "schedule" {
			continue
		}
		for _, ss := range top.Subcommands {
			if ss.Name == "audit" {
				found = true
				break
			}
		}
	}
	require.True(t, found, "audit subcommand not registered under schedule")
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

	t.Run("namespace only, one object per line", func(t *testing.T) {
		got, err := collect(`{"namespace":"ns1"}` + "\n" + `{"namespace":"ns2"}` + "\n")
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "ns1"}, {Namespace: "ns2"}}, got)
	})

	t.Run("namespace + schedule_id", func(t *testing.T) {
		got, err := collect(`{"namespace":"ns1","schedule_id":"s1"}` + "\n" + `{"namespace":"ns2","schedule_id":"s2"}` + "\n")
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{
			{Namespace: "ns1", ScheduleID: "s1"},
			{Namespace: "ns2", ScheduleID: "s2"},
		}, got)
	})

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

	t.Run("no trailing newline is fine", func(t *testing.T) {
		got, err := collect(`{"namespace":"ns1","schedule_id":"s1"}`)
		require.NoError(t, err)
		require.Equal(t, []scheduleaudit.Target{{Namespace: "ns1", ScheduleID: "s1"}}, got)
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

	t.Run("malformed stream line surfaces the parser error", func(t *testing.T) {
		_, err := produce(&auditInputs{Stdin: strings.NewReader(`{"schedule_id":"s1"}` + "\n")})
		require.ErrorContains(t, err, "namespace is empty")
	})
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
