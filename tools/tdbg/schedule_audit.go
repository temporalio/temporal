package tdbg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tools/tdbg/scheduleaudit"
	"golang.org/x/sync/errgroup"
)

// AdminAuditSchedules is the entry point for `tdbg schedule audit`. It streams audit targets into the auditor and
// streams one JSONL row per flagged schedule to stdout as each schedule finishes -- input and output both stream so a
// namespace with millions of schedules can be processed with bounded memory.
func AdminAuditSchedules(c *cli.Context, factory ClientFactory) error {
	in, err := parseAuditInputs(c)
	if err != nil {
		return err
	}

	wfClient := factory.WorkflowClient(c)
	in.Namespaces = grpcNamespaceDirectory{client: wfClient}
	progress := c.App.ErrWriter
	if in.Quiet {
		progress = io.Discard
	}
	limiter := scheduleaudit.NewNamespaceRateLimiter(in.RPS)
	listLimiter := scheduleaudit.NewNamespaceRateLimiter(in.ListRPS)
	stats := scheduleaudit.NewStats()
	auditor := &scheduleaudit.Auditor{
		WindowStart:    in.WindowStart,
		WindowEnd:      in.WindowEnd,
		AsOf:           in.AsOf,
		Concurrency:    in.Concurrency,
		IncludePaused:  in.IncludePaused,
		DelayThreshold: in.DelayThreshold,
		ChasmOnly:      in.ChasmOnly,
		Progress:       progress,
		Schedules:      scheduleaudit.NewGRPCScheduleLoader(wfClient, progress, limiter, listLimiter),
		Executions:     scheduleaudit.NewGRPCExecutionLoader(wfClient, progress, listLimiter),
		Stats:          stats,
		Backends: scheduleaudit.NewRetryingBackendResolver(
			scheduleBackendResolver{adminClient: factory.AdminClient(c)}, progress, limiter),
	}
	if in.VerifyMisses {
		auditor.Verifier = scheduleaudit.NewGRPCMissVerifier(wfClient, progress, limiter)
	}

	auditStart := time.Now()
	rw := scheduleaudit.NewRowWriter(c.App.Writer, in.DelayThreshold)
	var flagged int

	g, ctx := errgroup.WithContext(c.Context)
	targets := make(chan scheduleaudit.Target)
	g.Go(func() error {
		defer close(targets)
		return in.produceTargets(ctx, targets)
	})
	g.Go(func() error {
		return auditor.Run(ctx, targets, func(r scheduleaudit.Result) error {
			wrote, err := rw.Write(r)
			if wrote {
				flagged++
			}
			return err
		})
	})
	if err := g.Wait(); err != nil {
		return err
	}
	summary := stats.Snapshot()
	_, _ = fmt.Fprintf(progress,
		"audit complete: %d listed, %d audited, %d skipped, %d flagged in %s\n",
		summary.Listed, summary.Audited, summary.Skipped, flagged, time.Since(auditStart).Round(time.Second))
	return nil
}

// auditInputs holds the parsed, validated CLI inputs. Targets are not materialized here -- they are streamed by
// produceTargets so processing can begin before the whole target stream is read.
type auditInputs struct {
	Namespace   string
	ScheduleID  string
	File        string
	Stdin       io.Reader
	WindowStart time.Time
	WindowEnd   time.Time
	AsOf        time.Time

	Concurrency    int
	RPS            int
	ListRPS        int
	DelayThreshold time.Duration
	IncludePaused  bool
	Quiet          bool

	AllNamespaces     bool
	ExcludeNamespaces []string
	ChasmOnly         bool
	VerifyMisses      bool
	// Namespaces lists namespaces for --all-namespaces and resolves namespace_id targets.
	Namespaces namespaceDirectory
}

// namespaceDirectory is the namespace lookup the target producer needs.
type namespaceDirectory interface {
	// ListNamespaces calls yield with the name and ID of every registered namespace.
	ListNamespaces(ctx context.Context, yield func(name, id string) error) error
	// NamespaceName returns the name of the namespace with this ID.
	NamespaceName(ctx context.Context, id string) (string, error)
}

type grpcNamespaceDirectory struct {
	client workflowservice.WorkflowServiceClient
}

func (d grpcNamespaceDirectory) ListNamespaces(ctx context.Context, yield func(name, id string) error) error {
	var pageToken []byte
	for {
		resp, err := d.client.ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{
			PageSize:      100,
			NextPageToken: pageToken,
		})
		if err != nil {
			return fmt.Errorf("list namespaces: %w", err)
		}
		for _, ns := range resp.GetNamespaces() {
			info := ns.GetNamespaceInfo()
			// Deprecated and deleted namespaces have nothing worth auditing.
			if info.GetState() != enumspb.NAMESPACE_STATE_REGISTERED {
				continue
			}
			if err := yield(info.GetName(), info.GetId()); err != nil {
				return err
			}
		}
		if len(resp.GetNextPageToken()) == 0 {
			return nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (d grpcNamespaceDirectory) NamespaceName(ctx context.Context, id string) (string, error) {
	resp, err := d.client.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Id: id})
	if err != nil {
		return "", fmt.Errorf("describe namespace ID %s: %w", id, err)
	}
	return resp.GetNamespaceInfo().GetName(), nil
}

func parseAuditInputs(c *cli.Context) (*auditInputs, error) {
	in := &auditInputs{
		Namespace:      c.String(FlagNamespace),
		ScheduleID:     c.String(FlagScheduleID),
		File:           c.String(FlagFile),
		Stdin:          os.Stdin,
		Concurrency:    c.Int(FlagConcurrency),
		RPS:            c.Int(FlagRPS),
		ListRPS:        c.Int(FlagListRPS),
		DelayThreshold: c.Duration(FlagDelayThreshold),
		IncludePaused:  c.Bool(FlagIncludePaused),
		Quiet:          c.Bool(FlagQuiet),

		AllNamespaces:     c.Bool(FlagAllNamespaces),
		ExcludeNamespaces: c.StringSlice(FlagExcludeNamespace),
		ChasmOnly:         c.Bool(FlagChasmOnly),
		VerifyMisses:      c.Bool(FlagVerifyMisses),
	}
	if in.Concurrency <= 0 {
		in.Concurrency = 1
	}
	if in.RPS <= 0 {
		in.RPS = 1
	}
	if in.ListRPS <= 0 {
		in.ListRPS = 1
	}

	now := time.Now()
	in.AsOf = now
	start, ok, err := resolveBound(c.String(FlagStart), c.String(FlagStartTime), now)
	if err != nil {
		return nil, fmt.Errorf("--lookback-start/--start-time: %w", err)
	}
	if !ok {
		return nil, errors.New("one of --lookback-start (duration before now) or --start-time (RFC3339) is required")
	}
	in.WindowStart = start

	end, ok, err := resolveBound(c.String(FlagEnd), c.String(FlagEndTime), now)
	if err != nil {
		return nil, fmt.Errorf("--lookback-end/--end-time: %w", err)
	}
	if !ok {
		end = now // default: audit up to now
	}
	in.WindowEnd = end

	if err := in.validate(now); err != nil {
		return nil, err
	}
	return in, nil
}

// resolveBound turns a window bound into an absolute time from either a duration before now (dur, e.g. "24h"/"3d") or an
// absolute RFC3339 timestamp (ts). At most one may be set. ok reports whether a value was provided at all, so the caller
// can distinguish "not set" (apply a default or require it) from a parse error.
func resolveBound(dur, ts string, now time.Time) (t time.Time, ok bool, err error) {
	switch {
	case dur != "" && ts != "":
		return time.Time{}, false, errors.New("specify a duration or a timestamp, not both")
	case ts != "":
		t, err = time.Parse(time.RFC3339, ts)
		return t, true, err
	case dur != "":
		d, derr := parseDuration(dur)
		if derr != nil {
			return time.Time{}, true, derr
		}
		return now.Add(-d), true, nil
	default:
		return time.Time{}, false, nil
	}
}

// reDurationDays matches a days component ("3d", "1.5d") within a duration string.
var reDurationDays = regexp.MustCompile(`(\d+(\.\d*)?|(\.\d+))d`)

// parseDuration is time.ParseDuration extended with a "d" (days = exactly 24h) unit, mirroring
// cliext.ParseFlagDuration in the Temporal CLI so "3d" works the same way here.
func parseDuration(s string) (time.Duration, error) {
	s = reDurationDays.ReplaceAllStringFunc(s, func(v string) string {
		f, err := strconv.ParseFloat(strings.TrimSuffix(v, "d"), 64)
		if err != nil {
			return v // leave it; time.ParseDuration will report the error
		}
		return fmt.Sprintf("%fh", 24*f)
	})
	return time.ParseDuration(s)
}

func (in *auditInputs) validate(now time.Time) error {
	if in.ScheduleID != "" && in.Namespace == "" {
		return errors.New("--schedule-id is only valid with --namespace")
	}
	if in.AllNamespaces && (in.Namespace != "" || in.File != "") {
		return errors.New("--all-namespaces cannot be combined with --namespace or --file")
	}
	if len(in.ExcludeNamespaces) > 0 && !in.AllNamespaces {
		return errors.New("--exclude-namespace is only valid with --all-namespaces")
	}
	if !in.WindowEnd.After(in.WindowStart) {
		return fmt.Errorf("--lookback-end (%s) must be after --lookback-start (%s)",
			in.WindowEnd.UTC().Format(time.RFC3339),
			in.WindowStart.UTC().Format(time.RFC3339))
	}
	if in.WindowEnd.After(now) {
		return fmt.Errorf("--lookback-end (%s) must not be in the future", in.WindowEnd.UTC().Format(time.RFC3339))
	}
	return nil
}

// produceTargets streams audit targets into out. When a JSONL stream is present (--file, or piped/redirected stdin),
// it is the source of targets and the --namespace / --schedule-id flags act as constraints: every streamed target
// must agree with any flag that is set, otherwise produceTargets errors. This lets a caller pass --namespace to
// guarantee a run stays within a single namespace. When no stream is present (an interactive terminal, no --file), the
// flags define a single target directly: --namespace alone audits that whole namespace, and --namespace +
// --schedule-id audits that one schedule.
//
// The stream is a sequence of JSON objects (one per line): {"namespace":"...","schedule_id":"..."} with schedule_id
// optional (omit to audit the whole namespace).
func (in *auditInputs) produceTargets(ctx context.Context, out chan<- scheduleaudit.Target) error {
	send := func(t scheduleaudit.Target) error {
		select {
		case out <- t:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// --all-namespaces ignores stdin: a non-interactive caller can attach an empty stdin that isn't a terminal.
	if in.AllNamespaces {
		excluded := make(map[string]struct{}, len(in.ExcludeNamespaces))
		for _, ns := range in.ExcludeNamespaces {
			excluded[ns] = struct{}{}
		}
		return in.Namespaces.ListNamespaces(ctx, func(name, id string) error {
			_, byName := excluded[name]
			_, byID := excluded[id]
			if byName || byID {
				return nil
			}
			return send(scheduleaudit.Target{Namespace: name})
		})
	}

	r, closeFn, hasStream, err := in.openStream()
	if err != nil {
		return err
	}
	defer closeFn()

	// No stream: the flags define the single target. --schedule-id requires --namespace (enforced by validate).
	if !hasStream {
		if in.Namespace == "" {
			return errors.New("no targets: pass --namespace or --all-namespaces, or provide a JSONL stream via --file or stdin")
		}
		return send(scheduleaudit.Target{Namespace: in.Namespace, ScheduleID: in.ScheduleID})
	}

	// Stream present: the flags, when set, must match every streamed target.
	names := map[string]string{}
	resolve := func(id string) (string, error) {
		if name, ok := names[id]; ok {
			return name, nil
		}
		name, err := in.Namespaces.NamespaceName(ctx, id)
		if err != nil {
			return "", err
		}
		names[id] = name
		return name, nil
	}
	return streamJSONLTargets(r, resolve, func(t scheduleaudit.Target) error {
		if in.Namespace != "" && t.Namespace != in.Namespace {
			return fmt.Errorf("stream target namespace %q does not match --namespace %q", t.Namespace, in.Namespace)
		}
		if in.ScheduleID != "" && t.ScheduleID != in.ScheduleID {
			return fmt.Errorf("stream target schedule_id %q does not match --schedule-id %q", t.ScheduleID, in.ScheduleID)
		}
		return send(t)
	})
}

// openStream returns the reader for the JSONL target stream, a close function, and whether a stream is actually
// present. The source is --file (a path, or stdin when the value is "-"); when --file is unset it defaults to stdin.
// Stdin only counts as a stream when data is piped or redirected in -- an interactive terminal has no stream (and must
// not be read, or the command would block waiting for input that never comes).
func (in *auditInputs) openStream() (io.Reader, func(), bool, error) {
	if in.File != "" && in.File != "-" {
		f, err := os.Open(in.File)
		if err != nil {
			return nil, func() {}, false, err
		}
		return f, func() { _ = f.Close() }, true, nil
	}
	if in.File == "" {
		if f, ok := in.Stdin.(*os.File); ok && isTerminal(f) {
			return in.Stdin, func() {}, false, nil
		}
	}
	return in.Stdin, func() {}, true, nil
}

// isTerminal reports whether f is an interactive terminal (a character device) rather than a pipe or regular file.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// targetLine is the JSON shape of one input stream entry. Exactly one of namespace and namespace_id is set.
type targetLine struct {
	Namespace   string `json:"namespace"`
	NamespaceID string `json:"namespace_id"`
	ScheduleID  string `json:"schedule_id"`
}

// streamJSONLTargets decodes a stream of JSON objects (one per line) and calls emit for each, so targets flow to the
// auditor as they are read rather than being buffered. resolve maps a namespace_id to its name. A target must name
// exactly one of namespace and namespace_id.
func streamJSONLTargets(r io.Reader, resolve func(id string) (string, error), emit func(scheduleaudit.Target) error) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	for dec.More() {
		var tl targetLine
		if err := dec.Decode(&tl); err != nil {
			return fmt.Errorf("decode target: %w", err)
		}
		if (tl.Namespace == "") == (tl.NamespaceID == "") {
			return fmt.Errorf("target %+v: exactly one of namespace and namespace_id is required", tl)
		}
		namespace := tl.Namespace
		if namespace == "" {
			var err error
			if namespace, err = resolve(tl.NamespaceID); err != nil {
				return fmt.Errorf("target %+v: %w", tl, err)
			}
		}
		if err := emit(scheduleaudit.Target{Namespace: namespace, ScheduleID: tl.ScheduleID}); err != nil {
			return err
		}
	}
	return nil
}
