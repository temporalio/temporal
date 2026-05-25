package tdbg

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdminAuditSchedules is the entry point for `tdbg schedule audit`. It parses CLI inputs once, then runs the audit per
// namespace and writes the combined output bundle.
func AdminAuditSchedules(c *cli.Context, factory ClientFactory) error {
	in, err := parseAuditInputs(c)
	if err != nil {
		return err
	}

	wfClient := factory.WorkflowClient(c)
	retrier := &grpcRetrier{log: os.Stderr}

	// Cross-namespace parallelism is bounded by --namespace-concurrency (default 8); RPCs within a single namespace are
	// sequential to keep concurrent frontend pressure low. Per-schedule CPU work (classification) fans out to NumCPU
	// goroutines within a namespace. If any namespace fails, queued (not-yet-started) namespaces bail; in-flight
	// namespaces continue until finish.
	auditStart := time.Now()
	total := len(in.Namespaces)
	sem := make(chan struct{}, in.NamespaceConcurrency)
	var (
		mu         sync.Mutex
		allResults []scheduleResult
		firstErr   error
		done       int
	)
	var wg sync.WaitGroup
	for _, ns := range in.Namespaces {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			// Bail early if a sibling already failed.
			mu.Lock()
			if firstErr != nil {
				mu.Unlock()
				return
			}
			mu.Unlock()

			nsStart := time.Now()
			_, _ = fmt.Fprintf(os.Stderr, "auditing %s...\n", ns)
			auditor := &scheduleAuditor{
				Namespace:   ns,
				ScheduleID:  in.ScheduleID,
				WindowStart: in.WindowStart,
				WindowEnd:   in.WindowEnd,
				Schedules:   &grpcScheduleLoader{client: wfClient, retrier: retrier},
				Executions:  &grpcExecutionLoader{client: wfClient, retrier: retrier, log: os.Stderr},
				Progress:    os.Stderr,
			}
			results, err := auditor.run(c.Context)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("audit %s: %w", ns, err)
				}
				return
			}
			done++
			_, _ = fmt.Fprintf(os.Stderr, "[%d/%d] %s: %d schedule(s) flagged in %s\n",
				done, total, ns, len(results), time.Since(nsStart).Round(time.Second))
			allResults = append(allResults, results...)
		})
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	_, _ = fmt.Fprintf(os.Stderr, "audit complete: %d total flagged across %d namespaces in %s\n",
		len(allResults), total, time.Since(auditStart).Round(time.Second))
	if in.OutputDir == "" {
		// Stdout mode: one CSV stream, all rows, single header, no summary file.
		// Sort across namespaces (per-namespace.run() only sorts within its own batch).
		sort.Slice(allResults, func(i, j int) bool {
			if allResults[i].Namespace != allResults[j].Namespace {
				return allResults[i].Namespace < allResults[j].Namespace
			}
			return allResults[i].ScheduleID < allResults[j].ScheduleID
		})
		return writeFlatCSV(os.Stdout, allResults)
	}
	return writeOutput(in.OutputDir, allResults)
}

type auditInputs struct {
	Namespaces           []string
	ScheduleID           string // optional; if set, len(Namespaces) == 1
	WindowStart          time.Time
	WindowEnd            time.Time
	OutputDir            string
	NamespaceConcurrency int
}

func parseAuditInputs(c *cli.Context) (*auditInputs, error) {
	in := &auditInputs{
		ScheduleID:           c.String(FlagScheduleID),
		OutputDir:            c.String(FlagOutputDir),
		NamespaceConcurrency: c.Int(FlagNamespaceConcurrency),
	}
	if in.NamespaceConcurrency <= 0 {
		in.NamespaceConcurrency = 1
	}

	ns, err := resolveNamespaces(c)
	if err != nil {
		return nil, err
	}
	in.Namespaces = ns

	if in.WindowStart, err = time.Parse(time.RFC3339, c.String(FlagAuditStart)); err != nil {
		return nil, fmt.Errorf("--start: %w", err)
	}
	if in.WindowEnd, err = time.Parse(time.RFC3339, c.String(FlagAuditEnd)); err != nil {
		return nil, fmt.Errorf("--end: %w", err)
	}

	if err := in.validate(); err != nil {
		return nil, err
	}
	return in, nil
}

// maxAuditWindow caps how wide a single audit window can be. Catches typos (e.g. wrong month in --end) and discourages
// expensive multi-day runs that should be chunked into separate invocations.
const maxAuditWindow = 7 * 24 * time.Hour

func (in *auditInputs) validate() error {
	if in.ScheduleID != "" && len(in.Namespaces) != 1 {
		return errors.New("--schedule-id requires exactly one namespace (use --namespace, not --namespace-file)")
	}
	if !in.WindowEnd.After(in.WindowStart) {
		return fmt.Errorf("--end (%s) must be after --start (%s)",
			in.WindowEnd.UTC().Format(time.RFC3339),
			in.WindowStart.UTC().Format(time.RFC3339))
	}
	if d := in.WindowEnd.Sub(in.WindowStart); d > maxAuditWindow {
		return fmt.Errorf("window is %s (--start %s to --end %s); max is %s -- run multiple shorter audits for longer spans",
			d.Round(time.Hour),
			in.WindowStart.UTC().Format(time.RFC3339),
			in.WindowEnd.UTC().Format(time.RFC3339),
			maxAuditWindow)
	}
	return nil
}

func resolveNamespaces(c *cli.Context) ([]string, error) {
	ns := c.String(FlagNamespace)
	file := c.String(FlagNamespaceFile)
	switch {
	case ns != "" && file != "":
		return nil, errors.New("--namespace and --namespace-file are mutually exclusive")
	case ns != "":
		return []string{ns}, nil
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("--namespace-file: %w", err)
		}
		var out []string
		for line := range strings.SplitSeq(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			out = append(out, line)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("--namespace-file %q has no namespace entries", file)
		}
		return out, nil
	default:
		return nil, errors.New("one of --namespace or --namespace-file is required")
	}
}

// expectedFireTimes returns the nominal (pre-jitter) fire times the spec would produce in (start, end]. Uses the
// server's own spec compiler so the semantics exactly match the live scheduler.
//
// jitterSeed is required by the underlying compiler but does not affect the returned nominal times. Pass the schedule
// ID so the seed is stable per schedule.
func expectedFireTimes(spec *schedulepb.ScheduleSpec, jitterSeed string, start, end time.Time) ([]time.Time, error) {
	if spec == nil {
		return nil, nil
	}
	builder := scheduler.NewSpecBuilder()
	compiled, err := builder.NewCompiledSpec(spec)
	if err != nil {
		return nil, fmt.Errorf("compile spec: %w", err)
	}
	var out []time.Time
	cursor := start
	for {
		res := compiled.GetNextTime(jitterSeed, cursor)
		if res.Nominal.IsZero() || res.Nominal.After(end) {
			return out, nil
		}
		out = append(out, res.Nominal)
		cursor = res.Nominal
		if len(out) > 100_000 {
			// Defensive cap -- sub-minute schedules over very large windows could otherwise OOM. Caller should chunk windows.
			return nil, errors.New("expected fire times exceeded cap (100k)")
		}
	}
}

// timelineEntry is a single workflow execution row observed in visibility.
type timelineEntry struct {
	WorkflowID  string
	RunID       string
	StartTime   time.Time
	CloseTime   *time.Time // nil if still running
	Status      enumspb.WorkflowExecutionStatus
	NominalTime time.Time // from TemporalScheduledStartTime search attribute
}

// workflowChain is one logical scheduled fire, possibly spanning many ContinueAsNew links that share the same
// WorkflowID. The chain's lifetime is the union of its links' lifetimes, used to answer "was this fire still active at
// time T?".
type workflowChain struct {
	WorkflowID  string
	NominalTime time.Time
	// Earliest start across the chain.
	ChainStart time.Time
	// Latest close across the chain. Only meaningful when StillRunning is false; activeAt ignores this field when any
	// link is RUNNING.
	ChainEnd time.Time
	// StillRunning is true if any link in the chain is currently running.
	StillRunning bool
}

func (c *workflowChain) activeAt(at time.Time) bool {
	if c.StillRunning {
		return !at.Before(c.ChainStart)
	}
	// Fully closed: active in [ChainStart, ChainEnd).
	return !at.Before(c.ChainStart) && at.Before(c.ChainEnd)
}

// timeline holds all observed executions for a single schedule, grouped into ContinueAsNew chains keyed by WorkflowID.
type timeline struct {
	byWorkflowID map[string]*workflowChain
}

// addExecution incorporates one visibility row into the timeline, building or updating the chain for its WorkflowID.
func (t *timeline) addExecution(e timelineEntry) {
	chain, ok := t.byWorkflowID[e.WorkflowID]
	if !ok {
		chain = &workflowChain{
			WorkflowID:  e.WorkflowID,
			NominalTime: e.NominalTime,
			ChainStart:  e.StartTime,
		}
		t.byWorkflowID[e.WorkflowID] = chain
	}
	if e.StartTime.Before(chain.ChainStart) {
		chain.ChainStart = e.StartTime
	}
	if e.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		chain.StillRunning = true
	}
	if e.CloseTime != nil && !chain.StillRunning {
		if chain.ChainEnd.Before(*e.CloseTime) {
			chain.ChainEnd = *e.CloseTime
		}
	}
}

func (t *timeline) activeAt(at time.Time) []*workflowChain {
	var out []*workflowChain
	for _, c := range t.byWorkflowID {
		if c.activeAt(at) {
			out = append(out, c)
		}
	}
	return out
}

// matchNominal finds a chain whose NominalTime equals the given expected time, or nil if none match. Match is strict
// equality because the server's spec compiler produces whole-second UTC nominal times and our local expectedFireTimes
// uses the same compiler; both sides are byte-identical when the spec hasn't changed.
func (t *timeline) matchNominal(expected time.Time) *workflowChain {
	for _, c := range t.byWorkflowID {
		if c.NominalTime.Equal(expected) {
			return c
		}
	}
	return nil
}

// classifyFirings diffs expected fire times against the timeline and populates the classification fields on r.
// Classification at this stage is binary: "skip_overlap" when some chain was active at the expected fire moment,
// "real_miss" otherwise. (Other buckets like inconclusive_schedule_changed and unsupported_policy are not assigned
// here as they're populated later by postProcess based on DescribeSchedule output.)
func classifyFirings(r *scheduleResult, expected []time.Time, tl *timeline) {
	r.Categories = map[time.Time]string{}
	r.Counts = map[string]int{}
	r.Expected = len(expected)
	r.Actual = len(tl.byWorkflowID)

	for _, et := range expected {
		if tl.matchNominal(et) != nil {
			r.Matched++
			continue
		}
		r.Missed = append(r.Missed, et)
		if len(tl.activeAt(et)) > 0 {
			r.Categories[et] = categorySkipOverlap
			r.Counts[categorySkipOverlap]++
			continue
		}
		r.Categories[et] = categoryRealMiss
		r.Counts[categoryRealMiss]++
	}
}

// delayedFireBuffer is how far past the audit's windowEnd the visibility query keeps looking for workflows. A
// BUFFER-policy schedule whose nominal fire time falls inside the audit window can have its workflow start much
// later than windowEnd (because the scheduler queued it behind a still-running prior fire). Without this buffer,
// we'd miss those workflows and falsely flag them as real_miss. 24h is generous for the patterns we've observed
// in practice; may need to raise if we encounter BUFFER chains that routinely run longer than a day.
const delayedFireBuffer = 24 * time.Hour

// retentionSafetyBuffer is how far inside the retention boundary windowStart must sit to be considered "safe to audit".
// Visibility purges workflows retention-from-CloseTime ago, so a windowStart that's exactly at the boundary risks
// missing workflows that closed near windowStart. One day of slack absorbs clock skew and retention-job lag.
const retentionSafetyBuffer = 24 * time.Hour

// scheduleLoader fetches schedule specs from the cluster.
type scheduleLoader interface {
	// ListSchedules pages through every schedule in the namespace and projects each ScheduleListEntry into a
	// scheduleEntry (ID, spec, workflow type, jitter, paused). Paginated under the hood; one logical call may issue
	// many RPCs.
	ListSchedules(ctx context.Context, namespace string) ([]scheduleEntry, error)
	// LookupSchedule fetches a single schedule entry via DescribeSchedule
	LookupSchedule(ctx context.Context, namespace, scheduleID string) (scheduleEntry, error)
	// DescribeSchedule fetches a schedule's full metadata (create/update times, exhausted state, policies, catchup
	// window) via a single DescribeSchedule RPC, projected into scheduleDescription.
	DescribeSchedule(ctx context.Context, namespace, scheduleID string) (scheduleDescription, error)
	// NamespaceRetention returns the workflow execution retention TTL for the namespace.
	NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error)
}

type scheduleDescription struct {
	CreateTime time.Time
	UpdateTime time.Time
	// Exhausted is true when the schedule has limited_actions enabled and remaining_actions == 0. It won't fire anymore
	// even though its spec still evaluates to fire times.
	Exhausted bool
	// UnsupportedReason is non-empty when the schedule uses a policy our algorithm doesn't currently handle correctly.
	// Multiple reasons are joined with ";". When set, real_miss entries are reclassified into the unsupported_policy
	// bucket so operators can see which schedules need manual review.
	UnsupportedReason string
	// CatchupWindow is the schedule's configured catchup window. Recorded to troubleshoot dropped firings if the
	// catchup window is shorter than the observed gap between the expected fire time and the actual workflow start time.
	CatchupWindow time.Duration
}

// scheduleEntry is a minimal projection of ScheduleListEntry - exactly what the audit needs.
type scheduleEntry struct {
	ID            string
	Spec          *schedulepb.ScheduleSpec
	WorkflowType  string
	JitterSeconds int64
	// Paused schedules never fire even though their spec evaluates to fire times. Analysis skips them entirely to avoid
	// flagging expected-but-never-fired runs as real_miss.
	Paused bool
}

// executionLoader fetches every scheduled workflow that was alive at some moment in [windowStart, queryEnd] -- where
// "alive" is the natural interval predicate StartTime <= queryEnd AND (CloseTime >= windowStart OR CloseTime IS NULL).
// One paginated query covers everything our analysis needs: in-window starts, still-running long-runners, closed
// long-runners, and heavily-delayed BUFFER fires. When scheduleIDFilter is set, the query restricts to that one
// schedule.
type executionLoader interface {
	ListExecutions(ctx context.Context, namespace, scheduleIDFilter string, windowStart, queryEnd time.Time) (map[string][]timelineEntry, error)
}

// --- auditor ---

// scheduleAuditor performs the audit for one namespace and time window.
type scheduleAuditor struct {
	Namespace string
	// ScheduleID is optional; if non-empty, only this schedule is audited.
	ScheduleID  string
	WindowStart time.Time
	WindowEnd   time.Time

	// Progress receives one-line progress updates (start/end of the namespace, rare events like retention skip or
	// list/describe race). Required -- callers that don't want logs should pass io.Discard.
	Progress io.Writer

	Schedules  scheduleLoader
	Executions executionLoader
}

// Classification bucket names. Used both as keys in scheduleResult.Categories/Counts and as CSV column headers.
// The duplication is intentional -- one source of truth keeps map keys and column names in lockstep.
const (
	categoryRealMiss            = "real_miss"
	categorySkipOverlap         = "skip_overlap"
	categoryInconclusiveChanged = "inconclusive_schedule_changed"
	categoryUnsupportedPolicy   = "unsupported_policy"
)

// scheduleResult is the per-schedule output for downstream writers.
type scheduleResult struct {
	Namespace     string
	ScheduleID    string
	WorkflowType  string
	JitterSeconds int64

	Expected   int
	Actual     int
	Matched    int
	Missed     []time.Time
	Categories map[time.Time]string
	Counts     map[string]int
	// UnsupportedReason, when non-empty, names the policy/state config that caused this row's real_miss entries to be
	// reclassified into the unsupported_policy bucket. Mirrored from scheduleDescription.
	UnsupportedReason string
	// CatchupWindowSeconds is the schedule's configured catchup window in seconds. Populated only when DescribeSchedule
	// was called (i.e. real_miss > 0); zero/unset otherwise. Lets operators correlate a real_miss with a known scheduler
	// outage duration: if catchup_window < outage gap, fires are dropped permanently.
	CatchupWindowSeconds int64
}

// run executes the audit and returns one result per analyzed schedule. Schedules with no expected fire times in the
// window are omitted.
//
// One visibility query per namespace (not per schedule) keeps the RPC count down to O(namespaces) instead of
// O(schedules); critical for namespaces with thousands of schedules and a per-namespace visibility rate limit.
func (a *scheduleAuditor) run(ctx context.Context) ([]scheduleResult, error) {
	skip, err := a.checkRetention(ctx)
	if err != nil {
		return nil, err
	}
	if skip {
		return nil, nil
	}

	scheds, err := a.loadSchedules(ctx)
	if err != nil {
		return nil, err
	}

	executions, err := a.fetchExecutions(ctx)
	if err != nil {
		return nil, err
	}

	results, err := a.analyzeAll(scheds, executions)
	if err != nil {
		return nil, err
	}

	results, err = a.postProcess(ctx, scheds, executions, results)
	if err != nil {
		return nil, err
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Namespace != results[j].Namespace {
			return results[i].Namespace < results[j].Namespace
		}
		return results[i].ScheduleID < results[j].ScheduleID
	})
	return results, nil
}

// checkRetention refuses to audit a window that crosses the retention boundary -- purged workflows would silently turn
// into false-positive real_miss. Returns (skip=true, nil) when the caller should treat the namespace as a no-op.
func (a *scheduleAuditor) checkRetention(ctx context.Context) (bool, error) {
	retention, err := a.Schedules.NamespaceRetention(ctx, a.Namespace)
	if err != nil {
		return false, fmt.Errorf("namespace retention %s: %w", a.Namespace, err)
	}
	if retention == 0 {
		return false, nil
	}
	safeStart := time.Now().Add(-retention + retentionSafetyBuffer)
	if !a.WindowStart.Before(safeStart) {
		return false, nil
	}
	_, _ = fmt.Fprintf(a.Progress,
		"    %s: SKIPPING - windowStart %s is past retention boundary (retention=%s, safe-start=%s)\n",
		a.Namespace,
		a.WindowStart.UTC().Format(time.RFC3339),
		retention,
		safeStart.UTC().Format(time.RFC3339))
	return true, nil
}

// loadSchedules returns the schedules to analyze: either the single --schedule-id target (one DescribeSchedule RPC) or
// every active schedule in the namespace (paginated ListSchedules). Paused schedules are dropped in both modes -- their
// specs still produce expected fire times but nothing fires, which would otherwise show up as false-positive real_miss.
func (a *scheduleAuditor) loadSchedules(ctx context.Context) ([]scheduleEntry, error) {
	var scheds []scheduleEntry
	if a.ScheduleID != "" {
		entry, err := a.Schedules.LookupSchedule(ctx, a.Namespace, a.ScheduleID)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil, fmt.Errorf("schedule %q not found in namespace %q", a.ScheduleID, a.Namespace)
			}
			return nil, fmt.Errorf("lookup schedule %s: %w", a.ScheduleID, err)
		}
		scheds = []scheduleEntry{entry}
	} else {
		var err error
		scheds, err = a.Schedules.ListSchedules(ctx, a.Namespace)
		if err != nil {
			return nil, fmt.Errorf("list schedules: %w", err)
		}
	}

	active := scheds[:0]
	for _, s := range scheds {
		if !s.Paused {
			active = append(active, s)
		}
	}
	return active, nil
}

// fetchExecutions runs the single alive-during-window visibility query and returns workflow timelines grouped by
// schedule ID. queryEnd extends past windowEnd by delayedFireBuffer to catch heavily-delayed BUFFER fires whose nominal
// time falls in the window but whose actual StartTime lands well after windowEnd (e.g. 5-min schedules whose workflows
// take 24h to run). When --schedule-id is set, the filter is pushed down to the server.
func (a *scheduleAuditor) fetchExecutions(ctx context.Context) (map[string][]timelineEntry, error) {
	queryEnd := a.WindowEnd.Add(delayedFireBuffer)
	executions, err := a.Executions.ListExecutions(ctx, a.Namespace, a.ScheduleID, a.WindowStart, queryEnd)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	return executions, nil
}

// analyzeAll fans out analyzeSchedule across worker goroutines and gathers the per-schedule results. Concurrency is
// sized to NumCPU since the work is pure CPU (spec compile + classification), with no RPCs.
func (a *scheduleAuditor) analyzeAll(scheds []scheduleEntry, executions map[string][]timelineEntry) ([]scheduleResult, error) {
	sem := make(chan struct{}, runtime.NumCPU())
	var (
		mu       sync.Mutex
		results  []scheduleResult
		firstErr error
	)
	var wg sync.WaitGroup
	for _, s := range scheds {
		entries := executions[s.ID]
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			res, err := a.analyzeSchedule(s, entries, a.WindowStart)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			if res != nil {
				results = append(results, *res)
			}
		})
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

// postProcess re-examines schedules flagged with real_miss > 0 by calling DescribeSchedule and applying the following
// reclassification rules (checked in order; first match wins):
//
//   - NotFound (deleted between list and describe)  -> drop the row.
//   - UpdateTime > windowStart (spec changed mid-window) -> reclassify real_miss to inconclusive_schedule_changed.
//   - Exhausted (limited_actions, remaining_actions == 0) -> drop the row.
//   - UnsupportedReason != "" -> reclassify real_miss to unsupported_policy and stamp the reason. These are
//     corner-case policy/state configs the algorithm does not model correctly today, so we move the count out of
//     the trusted real_miss bucket and surface the row for manual review. Reasons currently detected:
//   - keep_original_workflow_id  -- all fires share one WorkflowID, collapsing the chain-by-WorkflowID model.
//   - overlap_buffer_all         -- fires can be queued for arbitrary durations, beyond our 24h query buffer.
//   - overlap_allow_all          -- scheduler never skips on overlap, so skip_overlap labels may be wrong.
//   - overlap_cancel_other       -- new fire cancels prior; chain lifecycle differs (likely works, unverified).
//   - overlap_terminate_other    -- same as cancel_other; likely works, unverified.
//   - CreateTime >= windowEnd -> drop the row. The schedule was created after the audit window ended, so it could
//     not have fired during the window. The expected fires we computed are phantom -- the spec compiler is
//     stateless and doesn't know when the schedule was created, so it generates fires the schedule never had a
//     chance to run. Dropping the row removes the false-positive real_miss count entirely.
//   - CreateTime > windowStart (created mid-window) -> re-analyze with expectedStart = CreateTime. The schedule
//     existed for part of the window but not all of it. Fires before CreateTime are phantom (same reason as
//     above) but fires at-or-after CreateTime are real. Re-running classifyFirings with the lower bound advanced
//     to CreateTime drops the phantom expected fires while preserving any genuine misses that occurred after the
//     schedule came online.
//   - Otherwise: row stands as real_miss.
//
// Done sequentially to keep describe RPCs gentle on the frontend.
func (a *scheduleAuditor) postProcess(ctx context.Context, scheds []scheduleEntry, executions map[string][]timelineEntry, results []scheduleResult) ([]scheduleResult, error) {
	schedByID := make(map[string]scheduleEntry, len(scheds))
	for _, s := range scheds {
		schedByID[s.ID] = s
	}
	revised := results[:0]
	for _, r := range results {
		if r.Counts[categoryRealMiss] == 0 {
			revised = append(revised, r)
			continue
		}
		newR, err := a.reclassifyOne(ctx, r, schedByID, executions)
		if err != nil {
			return nil, err
		}
		if newR != nil {
			revised = append(revised, *newR)
		}
	}
	return revised, nil
}

// reclassifyOne applies the post-process rules to a single result with real_miss > 0. Returns (nil, nil) when the row
// should be dropped (NotFound race, exhausted, or created after windowEnd) and (newRow, nil) when it should be kept
// (possibly reclassified or re-analyzed against a truncated window).
func (a *scheduleAuditor) reclassifyOne(ctx context.Context, r scheduleResult, schedByID map[string]scheduleEntry, executions map[string][]timelineEntry) (*scheduleResult, error) {
	desc, err := a.Schedules.DescribeSchedule(ctx, a.Namespace, r.ScheduleID)
	if err != nil {
		// Race: ListSchedules saw this schedule but it was deleted before our post-process call. Drop the row -- we
		// can't classify real_miss for a schedule that no longer exists.
		if status.Code(err) == codes.NotFound {
			_, _ = fmt.Fprintf(a.Progress, "    %s: %s: deleted between list and describe, dropping row\n",
				a.Namespace, r.ScheduleID)
			return nil, nil
		}
		return nil, fmt.Errorf("describe %s: %w", r.ScheduleID, err)
	}
	// Record to help correlate real_miss, especially for short catch-ups
	r.CatchupWindowSeconds = int64(desc.CatchupWindow.Seconds())

	switch {
	// Spec modified during window: current spec doesn't describe what was firing pre-modification.
	case !desc.UpdateTime.IsZero() && desc.UpdateTime.After(a.WindowStart):
		recategorize(&r, categoryRealMiss, categoryInconclusiveChanged)
		return &r, nil
	// Exhausted (limited_actions with remaining_actions==0): won't fire anymore even though spec still produces fire
	// times. Drop.
	case desc.Exhausted:
		return nil, nil
	// Unsupported policy/state: algorithm doesn't fully model these (keepOriginalWorkflowId, BUFFER_ALL, etc). Surface
	// for manual review but don't claim the misses are real.
	case desc.UnsupportedReason != "":
		recategorize(&r, categoryRealMiss, categoryUnsupportedPolicy)
		r.UnsupportedReason = desc.UnsupportedReason
		return &r, nil
	// Created after windowEnd: couldn't have fired during the window.
	case !desc.CreateTime.IsZero() && !desc.CreateTime.Before(a.WindowEnd):
		return nil, nil
	// Created mid-window: re-analyze with truncated expected.
	case !desc.CreateTime.IsZero() && desc.CreateTime.After(a.WindowStart):
		return a.reanalyzeFromCreate(r, desc.CreateTime, schedByID, executions)
	}
	// Schedule unchanged and pre-existing: result stands.
	return &r, nil
}

// reanalyzeFromCreate handles the "created mid-window" branch: re-runs the per-schedule analysis with expectedStart
// advanced to the schedule's CreateTime, so phantom expected fires from before the schedule existed are dropped.
func (a *scheduleAuditor) reanalyzeFromCreate(r scheduleResult, createTime time.Time, schedByID map[string]scheduleEntry, executions map[string][]timelineEntry) (*scheduleResult, error) {
	s, ok := schedByID[r.ScheduleID]
	if !ok {
		return &r, nil
	}
	newR, err := a.analyzeSchedule(s, executions[r.ScheduleID], createTime)
	if err != nil {
		return nil, fmt.Errorf("re-analyze %s: %w", r.ScheduleID, err)
	}
	if newR == nil {
		return nil, nil
	}
	newR.CatchupWindowSeconds = r.CatchupWindowSeconds
	return newR, nil
}

// analyzeSchedule runs the audit for a single schedule entry against the pre-fetched timeline entries. expectedStart
// bounds the lower edge of expected fires; pass a.WindowStart for the normal case, or a schedule's createTime (when
// later than WindowStart) to suppress false-positive real_miss for schedules that didn't yet exist at the start of the
// audit window.
func (a *scheduleAuditor) analyzeSchedule(s scheduleEntry, entries []timelineEntry, expectedStart time.Time) (*scheduleResult, error) {
	expected, err := expectedFireTimes(s.Spec, s.ID, expectedStart, a.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("expected fires for %s: %w", s.ID, err)
	}
	if len(expected) == 0 {
		return nil, nil
	}
	tl := &timeline{byWorkflowID: map[string]*workflowChain{}}
	for _, e := range entries {
		tl.addExecution(e)
	}
	r := &scheduleResult{
		Namespace:     a.Namespace,
		ScheduleID:    s.ID,
		WorkflowType:  s.WorkflowType,
		JitterSeconds: s.JitterSeconds,
	}
	classifyFirings(r, expected, tl)
	return r, nil
}

// recategorize moves all entries currently classified as `from` into `to`, updating both the per-fire Categories map
// and the aggregate Counts. Used by the post-process to reclassify real_miss entries when we can't trust the spec (e.g.
// the schedule was modified during the audit window).
func recategorize(r *scheduleResult, from, to string) {
	if r.Counts[from] == 0 {
		return
	}
	for t, cat := range r.Categories {
		if cat == from {
			r.Categories[t] = to
		}
	}
	r.Counts[to] += r.Counts[from]
	delete(r.Counts, from)
}

// grpcRetrier wraps RPCs with retry-on-ResourceExhausted
type grpcRetrier struct {
	log io.Writer
}

// retrier parameters
const (
	retryMaxAttempts     = 10
	retryInitialBackoff  = time.Second
	retryMaxBackoff      = 10 * time.Second
	retryBackoffJitter   = 0.2
	retryLogAfterAttempt = 3
)

// do executes f with retry-on-ResourceExhausted. Backoff is exponential with jitter.
func (t *grpcRetrier) do(ctx context.Context, opName string, f func() error) error {
	var err error
	for attempt := range retryMaxAttempts {
		err = f()
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.ResourceExhausted {
			return err
		}
		if attempt == retryMaxAttempts-1 {
			return err
		}
		wait := backoffForAttempt(attempt)
		// attempt is 0-indexed; this is the (attempt+1)-th retry about to start.
		nextAttempt := attempt + 2
		if t.log != nil && nextAttempt > retryLogAfterAttempt {
			_, _ = fmt.Fprintf(t.log, "    rate limited on %s; sleeping %s before retry %d/%d\n",
				opName, wait, nextAttempt, retryMaxAttempts)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func backoffForAttempt(attempt int) time.Duration {
	wait := min(time.Duration(float64(retryInitialBackoff)*math.Pow(2, float64(attempt))), retryMaxBackoff)
	jitter := (rand.Float64()*2 - 1) * retryBackoffJitter
	return time.Duration(float64(wait) * (1 + jitter))
}

// grpcScheduleLoader is the production implementation of scheduleLoader, backed by the workflow-service frontend.
type grpcScheduleLoader struct {
	client  workflowservice.WorkflowServiceClient
	retrier *grpcRetrier
}

func (l *grpcScheduleLoader) ListSchedules(ctx context.Context, namespace string) ([]scheduleEntry, error) {
	var out []scheduleEntry
	var pageToken []byte
	for {
		var resp *workflowservice.ListSchedulesResponse
		err := l.retrier.do(ctx, fmt.Sprintf("ListSchedules(%s)", namespace), func() error {
			var rpcErr error
			resp, rpcErr = l.client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace:       namespace,
				MaximumPageSize: 1000,
				NextPageToken:   pageToken,
			})
			return rpcErr
		})
		if err != nil {
			return nil, err
		}
		for _, s := range resp.GetSchedules() {
			info := s.GetInfo()
			if info == nil {
				continue
			}
			var jitterSecs int64
			if j := info.GetSpec().GetJitter(); j != nil {
				jitterSecs = int64(j.AsDuration().Seconds())
			}
			out = append(out, scheduleEntry{
				ID:            s.GetScheduleId(),
				Spec:          info.GetSpec(),
				WorkflowType:  info.GetWorkflowType().GetName(),
				JitterSeconds: jitterSecs,
				Paused:        info.GetPaused(),
			})
		}
		if len(resp.GetNextPageToken()) == 0 {
			break
		}
		pageToken = resp.GetNextPageToken()
	}
	return out, nil
}

// LookupSchedule fetches a single schedule via DescribeSchedule and projects the response into a scheduleEntry
func (l *grpcScheduleLoader) LookupSchedule(ctx context.Context, namespace, scheduleID string) (scheduleEntry, error) {
	var resp *workflowservice.DescribeScheduleResponse
	err := l.retrier.do(ctx, fmt.Sprintf("DescribeSchedule(%s/%s)", namespace, scheduleID), func() error {
		var rpcErr error
		resp, rpcErr = l.client.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		})
		return rpcErr
	})
	if err != nil {
		return scheduleEntry{}, err
	}
	sched := resp.GetSchedule()
	spec := sched.GetSpec()
	var jitterSecs int64
	if j := spec.GetJitter(); j != nil {
		jitterSecs = int64(j.AsDuration().Seconds())
	}
	return scheduleEntry{
		ID:            scheduleID,
		Spec:          spec,
		WorkflowType:  sched.GetAction().GetStartWorkflow().GetWorkflowType().GetName(),
		JitterSeconds: jitterSecs,
		Paused:        sched.GetState().GetPaused(),
	}, nil
}

// NamespaceRetention returns the workflow execution retention TTL via DescribeNamespace. One RPC per namespace.
func (l *grpcScheduleLoader) NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error) {
	var resp *workflowservice.DescribeNamespaceResponse
	err := l.retrier.do(ctx, fmt.Sprintf("DescribeNamespace(%s)", namespace), func() error {
		var rpcErr error
		resp, rpcErr = l.client.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: namespace,
		})
		return rpcErr
	})
	if err != nil {
		return 0, err
	}
	if d := resp.GetConfig().GetWorkflowExecutionRetentionTtl(); d != nil {
		return d.AsDuration(), nil
	}
	return 0, nil
}

// DescribeSchedule fetches a schedule's metadata via one DescribeSchedule RPC, projected into scheduleDescription
func (l *grpcScheduleLoader) DescribeSchedule(ctx context.Context, namespace, scheduleID string) (scheduleDescription, error) {
	var resp *workflowservice.DescribeScheduleResponse
	err := l.retrier.do(ctx, fmt.Sprintf("DescribeSchedule(%s/%s)", namespace, scheduleID), func() error {
		var rpcErr error
		resp, rpcErr = l.client.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		})
		return rpcErr
	})
	if err != nil {
		return scheduleDescription{}, err
	}
	info := resp.GetInfo()
	sched := resp.GetSchedule()
	state := sched.GetState()
	policies := sched.GetPolicies()
	var desc scheduleDescription
	if ct := info.GetCreateTime(); ct != nil {
		desc.CreateTime = ct.AsTime()
	}
	if ut := info.GetUpdateTime(); ut != nil {
		desc.UpdateTime = ut.AsTime()
	}
	desc.Exhausted = state.GetLimitedActions() && state.GetRemainingActions() == 0
	desc.UnsupportedReason = unsupportedPolicyReason(policies)
	if cw := policies.GetCatchupWindow(); cw != nil {
		desc.CatchupWindow = cw.AsDuration()
	}
	return desc, nil
}

// unsupportedPolicyReason returns a non-empty string when a schedule's policies/state use a configuration our audit
// doesn't fully handle. Multiple reasons are joined with ";". Operators see this in the report and know those rows need
// manual review rather than being trusted as real_miss.
func unsupportedPolicyReason(policies *schedulepb.SchedulePolicies) string {
	if policies == nil {
		return ""
	}
	var reasons []string
	if policies.GetKeepOriginalWorkflowId() {
		// All fires share one WorkflowID, breaking our chain-by-WorkflowID model.
		reasons = append(reasons, "keep_original_workflow_id")
	}
	switch policies.GetOverlapPolicy() {
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
		// Fires can be buffered for arbitrary durations; our delayedFireBuffer of 24h may miss the workflow.
		reasons = append(reasons, "overlap_buffer_all")
	case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
		// Each new fire cancels the running one; chain lifecycle differs from the standard model. Likely works but
		// unverified.
		reasons = append(reasons, "overlap_cancel_other")
	case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		// Similar to CANCEL_OTHER. Likely works but unverified.
		reasons = append(reasons, "overlap_terminate_other")
	case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
		// ALLOW_ALL never skips on overlap, so classifyFirings can mis-label
		// real_miss as skip_overlap when a long-running prior fire is active
		// at the expected time. Surface for inspection; we don't reclassify
		// here because the skip_overlap labels were already computed before
		// post-process and DescribeSchedule isn't called for skip-only rows.
		reasons = append(reasons, "overlap_allow_all")
	default:
		// SKIP, BUFFER_ONE, and UNSPECIFIED are fully modeled; nothing to flag.
	}
	return strings.Join(reasons, ";")
}

// grpcExecutionLoader pages through ListWorkflowExecutions for an entire namespace in a single visibility query,
// returning entries grouped by schedule ID. The query uses `TemporalScheduledById IS NOT NULL` so the server can
// satisfy it with an index scan, and pagination is shared across all schedules.
type grpcExecutionLoader struct {
	client  workflowservice.WorkflowServiceClient
	retrier *grpcRetrier
	log     io.Writer
}

// visibilityPageSize is the per-call ListWorkflowExecutions page size. 1000 is a safe default; the server may
// silently clamp to a lower max. Larger values reduce RPC count but each call returns more bytes (4MB gRPC limit
// caps practical sizes around ~5000).
const visibilityPageSize = 1000

const pageProgressEvery = 5

// ListExecutions issues one paginated visibility query that returns every scheduled workflow alive at some moment in
// [windowStart, queryEnd]. The predicate `StartTime <= queryEnd AND (CloseTime >= windowStart OR CloseTime IS NULL)`
// captures in-window starts, still-running long-runners, closed long-runners that crossed windowStart, and
// heavily-delayed BUFFER fires in one shot -- replacing what used to be three separate queries.
func (l *grpcExecutionLoader) ListExecutions(ctx context.Context, namespace, scheduleIDFilter string, windowStart, queryEnd time.Time) (map[string][]timelineEntry, error) {
	scheduleClause := buildScheduleClause(scheduleIDFilter)
	query := fmt.Sprintf(
		`%s AND StartTime <= %q AND (CloseTime >= %q OR CloseTime IS NULL)`,
		scheduleClause,
		queryEnd.UTC().Format(time.RFC3339),
		windowStart.UTC().Format(time.RFC3339),
	)
	return l.paginate(ctx, namespace, scheduleIDFilter, query, "alive-during-window")
}

func buildScheduleClause(scheduleIDFilter string) string {
	if scheduleIDFilter != "" {
		return fmt.Sprintf(`TemporalScheduledById = %q`, scheduleIDFilter)
	}
	return `TemporalScheduledById IS NOT NULL`
}

// paginate runs a visibility query to completion, demuxing results by TemporalScheduledById. opTag is a short label
// included in retry/progress log lines so operators can identify the query when scanning stderr.
func (l *grpcExecutionLoader) paginate(ctx context.Context, namespace, scheduleIDFilter, query, opTag string) (map[string][]timelineEntry, error) {
	opLabel := fmt.Sprintf("ListWorkflowExecutions(%s,%s)", namespace, opTag)
	if scheduleIDFilter != "" {
		opLabel = fmt.Sprintf("ListWorkflowExecutions(%s/%s,%s)", namespace, scheduleIDFilter, opTag)
	}
	out := map[string][]timelineEntry{}
	var pageToken []byte
	var page, total, skipped int
	for {
		resp, err := l.fetchPage(ctx, namespace, query, pageToken, opLabel)
		if err != nil {
			return nil, err
		}
		page++
		added, skip := appendPageEntries(resp, out)
		total += added
		skipped += skip
		if l.log != nil && page%pageProgressEvery == 0 {
			_, _ = fmt.Fprintf(l.log, "      %s (%s): page %d, %d entries (%d skipped) across %d schedules\n",
				namespace, opTag, page, total, skipped, len(out))
		}
		if len(resp.GetNextPageToken()) == 0 {
			return out, nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (l *grpcExecutionLoader) fetchPage(ctx context.Context, namespace, query string, pageToken []byte, opLabel string) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListWorkflowExecutionsResponse
	err := l.retrier.do(ctx, opLabel, func() error {
		var rpcErr error
		resp, rpcErr = l.client.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     namespace,
			Query:         query,
			PageSize:      visibilityPageSize,
			NextPageToken: pageToken,
		})
		return rpcErr
	})
	return resp, err
}

// appendPageEntries parses each execution in resp into a timelineEntry and appends it under its TemporalScheduledById bucket
// in out. Returns (added, skipped) counts for the page.
func appendPageEntries(resp *workflowservice.ListWorkflowExecutionsResponse, out map[string][]timelineEntry) (added, skipped int) {
	for _, exec := range resp.GetExecutions() {
		sa := exec.GetSearchAttributes().GetIndexedFields()
		scheduleID, ok := decodeScheduledByID(sa["TemporalScheduledById"])
		if !ok || scheduleID == "" {
			skipped++
			continue
		}
		entry, ok := buildTimelineEntry(exec, sa)
		if !ok {
			skipped++
			continue
		}
		out[scheduleID] = append(out[scheduleID], entry)
		added++
	}
	return added, skipped
}

// buildTimelineEntry constructs a timelineEntry from one visibility execution. Returns ok=false if the row has neither
// nominal nor start time, since there's nothing to match against.
func buildTimelineEntry(exec *workflowpb.WorkflowExecutionInfo, sa map[string]*commonpb.Payload) (timelineEntry, bool) {
	entry := timelineEntry{
		WorkflowID: exec.GetExecution().GetWorkflowId(),
		RunID:      exec.GetExecution().GetRunId(),
		Status:     exec.GetStatus(),
	}
	if st := exec.GetStartTime(); st != nil {
		entry.StartTime = st.AsTime()
	}
	if ct := exec.GetCloseTime(); ct != nil {
		t := ct.AsTime()
		entry.CloseTime = &t
	}
	if nominal, ok := decodeNominalStartTime(sa["TemporalScheduledStartTime"]); ok {
		entry.NominalTime = nominal
	}
	if entry.NominalTime.IsZero() && entry.StartTime.IsZero() {
		return timelineEntry{}, false
	}
	if entry.NominalTime.IsZero() {
		entry.NominalTime = entry.StartTime
	}
	return entry, true
}

// decodeScheduledByID extracts the schedule ID from a TemporalScheduledById search-attribute payload. Returns false if
// the payload is missing or in an unexpected encoding so the caller can skip the row instead of grouping it under an
// empty key.
func decodeScheduledByID(payload *commonpb.Payload) (string, bool) {
	if payload == nil {
		return "", false
	}
	encoding := string(payload.GetMetadata()["encoding"])
	if !strings.HasPrefix(encoding, "json/plain") {
		return "", false
	}
	var raw string
	if err := json.Unmarshal(payload.GetData(), &raw); err != nil {
		return "", false
	}
	return raw, true
}

// decodeNominalStartTime extracts the nominal scheduled start time from a TemporalScheduledStartTime search-attribute
// payload. Returns zero time if the payload is missing or in an unexpected encoding.
func decodeNominalStartTime(payload *commonpb.Payload) (time.Time, bool) {
	if payload == nil {
		return time.Time{}, false
	}
	encoding := string(payload.GetMetadata()["encoding"])
	if !strings.HasPrefix(encoding, "json/plain") {
		return time.Time{}, false
	}
	// Visibility payloads carry a JSON-encoded scalar (string like "2026-05-19T18:00:00Z").
	var raw string
	if err := json.Unmarshal(payload.GetData(), &raw); err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

var flatCSVBaseHeader = []string{
	"namespace",
	"schedule_id",
	"workflow_type",
	"jitter_s",
	"expected",
	"actual",
	"matched",
	"missed",
	categoryRealMiss,
	categorySkipOverlap,
	categoryInconclusiveChanged,
	categoryUnsupportedPolicy,
	"unsupported_reason",
	"catchup_window_s",
	"real_miss_times",
}

func writeFlatCSV(w io.Writer, results []scheduleResult) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()
	if err := cw.Write(flatCSVBaseHeader); err != nil {
		return err
	}
	for _, r := range results {
		if len(r.Missed) == 0 {
			continue
		}
		if err := cw.Write(flatRow(r)); err != nil {
			return err
		}
	}
	return cw.Error()
}

func flatRow(r scheduleResult) []string {
	// Only emit the real_miss times -- the other buckets (skip_overlap, inconclusive_schedule_changed, unsupported_policy)
	// all have valid reasons, so their timestamps add noise to the output column. Categories may have been mutated by
	// recategorize during post-process, so filter on the final classification.
	var realMissTimes []time.Time
	for _, t := range r.Missed {
		if r.Categories[t] == categoryRealMiss {
			realMissTimes = append(realMissTimes, t)
		}
	}
	return []string{
		r.Namespace,
		r.ScheduleID,
		strings.ReplaceAll(r.WorkflowType, ",", ";"),
		fmt.Sprintf("%d", r.JitterSeconds),
		fmt.Sprintf("%d", r.Expected),
		fmt.Sprintf("%d", r.Actual),
		fmt.Sprintf("%d", r.Matched),
		fmt.Sprintf("%d", len(r.Missed)),
		fmt.Sprintf("%d", r.Counts[categoryRealMiss]),
		fmt.Sprintf("%d", r.Counts[categorySkipOverlap]),
		fmt.Sprintf("%d", r.Counts[categoryInconclusiveChanged]),
		fmt.Sprintf("%d", r.Counts[categoryUnsupportedPolicy]),
		strings.ReplaceAll(r.UnsupportedReason, ",", ";"),
		fmt.Sprintf("%d", r.CatchupWindowSeconds),
		joinTimes(realMissTimes, 20),
	}
}

// joinTimes joins up to maxItems timestamps with `;` separators, truncating with a `...+N more` suffix if there are
// more.
func joinTimes(times []time.Time, maxItems int) string {
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	if len(times) > maxItems {
		s := make([]string, 0, maxItems+1)
		for _, t := range times[:maxItems] {
			s = append(s, t.UTC().Format(time.RFC3339))
		}
		s = append(s, fmt.Sprintf("...+%d more", len(times)-maxItems))
		return strings.Join(s, ";")
	}
	s := make([]string, 0, len(times))
	for _, t := range times {
		s = append(s, t.UTC().Format(time.RFC3339))
	}
	return strings.Join(s, ";")
}

// writeOutput writes a per-namespace bundle directory:
//
//	<dir>/summary.csv
//	<dir>/per-namespace/<ns>.csv
func writeOutput(dir string, results []scheduleResult) error {
	perNSDir := filepath.Join(dir, "per-namespace")
	if err := os.MkdirAll(perNSDir, 0o755); err != nil {
		return err
	}

	byNS := map[string][]scheduleResult{}
	for _, r := range results {
		if len(r.Missed) == 0 {
			continue
		}
		byNS[r.Namespace] = append(byNS[r.Namespace], r)
	}

	for ns, rs := range byNS {
		// Sanitize "/" -> "_"
		safeName := strings.NewReplacer("/", "_", string(filepath.Separator), "_").Replace(ns)
		path := filepath.Join(perNSDir, safeName+".csv")
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		if err := writeFlatCSV(f, rs); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}

	return writeSummaryCSV(filepath.Join(dir, "summary.csv"), byNS)
}

// writeSummaryCSV emits one row per namespace with aggregated counts across the namespace's flagged schedules.
func writeSummaryCSV(path string, byNS map[string][]scheduleResult) (retErr error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = cerr
		}
	}()
	cw := csv.NewWriter(f)
	defer cw.Flush()
	if err := cw.Write([]string{
		"namespace", "schedules_flagged",
		categoryRealMiss, categorySkipOverlap, categoryInconclusiveChanged,
		categoryUnsupportedPolicy,
	}); err != nil {
		return err
	}
	nss := make([]string, 0, len(byNS))
	for ns := range byNS {
		nss = append(nss, ns)
	}
	sort.Strings(nss)
	for _, ns := range nss {
		rs := byNS[ns]
		var realMisses, skips, specChanged, unsupported int
		for _, r := range rs {
			realMisses += r.Counts[categoryRealMiss]
			skips += r.Counts[categorySkipOverlap]
			specChanged += r.Counts[categoryInconclusiveChanged]
			unsupported += r.Counts[categoryUnsupportedPolicy]
		}
		if err := cw.Write([]string{
			ns,
			fmt.Sprintf("%d", len(rs)),
			fmt.Sprintf("%d", realMisses),
			fmt.Sprintf("%d", skips),
			fmt.Sprintf("%d", specChanged),
			fmt.Sprintf("%d", unsupported),
		}); err != nil {
			return err
		}
	}
	return cw.Error()
}
