package tdbg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/urfave/cli/v2"
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
	limiter := scheduleaudit.NewNamespaceRateLimiter(in.RPS)
	auditor := &scheduleaudit.Auditor{
		WindowStart: in.WindowStart,
		WindowEnd:   in.WindowEnd,
		Concurrency: in.Concurrency,
		RPS:         in.RPS,
		Progress:    os.Stderr,
		Schedules:   scheduleaudit.NewGRPCScheduleLoader(wfClient, os.Stderr, limiter),
		Executions:  scheduleaudit.NewGRPCExecutionLoader(wfClient, os.Stderr, limiter),
	}

	auditStart := time.Now()
	rw := scheduleaudit.NewRowWriter(os.Stdout, in.DelayThreshold)
	var flagged int

	g, ctx := errgroup.WithContext(c.Context)
	targets := make(chan scheduleaudit.Target)
	g.Go(func() error {
		defer close(targets)
		return in.produceTargets(ctx, targets)
	})
	g.Go(func() error {
		return auditor.Run(ctx, targets, func(r scheduleaudit.Result) error {
			flagged++
			return rw.Write(r)
		})
	})
	if err := g.Wait(); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stderr, "audit complete: %d schedule(s) flagged in %s\n",
		flagged, time.Since(auditStart).Round(time.Second))
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

	Concurrency    int
	RPS            int
	DelayThreshold time.Duration
}

func parseAuditInputs(c *cli.Context) (*auditInputs, error) {
	in := &auditInputs{
		Namespace:      c.String(FlagNamespace),
		ScheduleID:     c.String(FlagScheduleID),
		File:           c.String(FlagFile),
		Stdin:          os.Stdin,
		Concurrency:    c.Int(FlagConcurrency),
		RPS:            c.Int(FlagRPS),
		DelayThreshold: c.Duration(FlagDelayThreshold),
	}
	if in.Concurrency <= 0 {
		in.Concurrency = 1
	}
	if in.RPS <= 0 {
		in.RPS = 1
	}

	var err error
	if in.WindowStart, err = time.Parse(time.RFC3339, c.String(FlagStart)); err != nil {
		return nil, fmt.Errorf("--start: %w", err)
	}
	if in.WindowEnd, err = time.Parse(time.RFC3339, c.String(FlagEnd)); err != nil {
		return nil, fmt.Errorf("--end: %w", err)
	}

	if err := in.validate(); err != nil {
		return nil, err
	}
	return in, nil
}

func (in *auditInputs) validate() error {
	if in.ScheduleID != "" && in.Namespace == "" {
		return errors.New("--schedule-id is only valid with --namespace")
	}
	if !in.WindowEnd.After(in.WindowStart) {
		return fmt.Errorf("--end (%s) must be after --start (%s)",
			in.WindowEnd.UTC().Format(time.RFC3339),
			in.WindowStart.UTC().Format(time.RFC3339))
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

	r, closeFn, hasStream, err := in.openStream()
	if err != nil {
		return err
	}
	defer closeFn()

	// No stream: the flags define the single target. --schedule-id requires --namespace (enforced by validate).
	if !hasStream {
		if in.Namespace == "" {
			return errors.New("no targets: pass --namespace, or provide a JSONL stream via --file or stdin")
		}
		return send(scheduleaudit.Target{Namespace: in.Namespace, ScheduleID: in.ScheduleID})
	}

	// Stream present: the flags, when set, must match every streamed target.
	return streamJSONLTargets(r, func(t scheduleaudit.Target) error {
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
	if f, ok := in.Stdin.(*os.File); ok && isTerminal(f) {
		return in.Stdin, func() {}, false, nil
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

// targetLine is the JSON shape of one input stream entry.
type targetLine struct {
	Namespace  string `json:"namespace"`
	ScheduleID string `json:"schedule_id"`
}

// streamJSONLTargets decodes a stream of JSON objects (one per line) and calls emit for each, so targets flow to the
// auditor as they are read rather than being buffered. A missing namespace is a hard error.
func streamJSONLTargets(r io.Reader, emit func(scheduleaudit.Target) error) error {
	dec := json.NewDecoder(r)
	for dec.More() {
		var tl targetLine
		if err := dec.Decode(&tl); err != nil {
			return fmt.Errorf("decode target: %w", err)
		}
		if tl.Namespace == "" {
			return fmt.Errorf("target %+v: namespace is empty", tl)
		}
		if err := emit(scheduleaudit.Target{Namespace: tl.Namespace, ScheduleID: tl.ScheduleID}); err != nil {
			return err
		}
	}
	return nil
}
