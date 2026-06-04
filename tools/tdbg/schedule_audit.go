package tdbg

import (
	"bufio"
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
	total := len(in.Targets)
	sem := make(chan struct{}, in.NamespaceConcurrency)
	var (
		mu         sync.Mutex
		allResults []scheduleResult
		firstErr   error
		done       int
	)
	var wg sync.WaitGroup
	for _, t := range in.Targets {
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

			label := t.Namespace
			if t.ScheduleID != "" {
				label = fmt.Sprintf("%s/%s", t.Namespace, t.ScheduleID)
			}
			nsStart := time.Now()
			_, _ = fmt.Fprintf(os.Stderr, "auditing %s...\n", label)
			auditor := &scheduleAuditor{
				Namespace:   t.Namespace,
				ScheduleID:  t.ScheduleID,
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
					firstErr = fmt.Errorf("audit %s: %w", label, err)
				}
				return
			}
			done++
			_, _ = fmt.Fprintf(os.Stderr, "[%d/%d] %s: %d schedule(s) flagged in %s\n",
				done, total, label, len(results), time.Since(nsStart).Round(time.Second))
			allResults = append(allResults, results...)
		})
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	_, _ = fmt.Fprintf(os.Stderr, "audit complete: %d total flagged across %d target(s) in %s\n",
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

type auditTarget struct {
	Namespace  string
	ScheduleID string
}

type auditInputs struct {
	Targets              []auditTarget
	WindowStart          time.Time
	WindowEnd            time.Time
	OutputDir            string
	NamespaceConcurrency int
}

func parseAuditInputs(c *cli.Context) (*auditInputs, error) {
	in := &auditInputs{
		OutputDir:            c.String(FlagOutputDir),
		NamespaceConcurrency: c.Int(FlagNamespaceConcurrency),
	}
	if in.NamespaceConcurrency <= 0 {
		in.NamespaceConcurrency = 1
	}

	targets, err := resolveTargetsFromFlags(c.String(FlagNamespace), c.String(FlagScheduleID), c.String(FlagFile), os.Stdin)
	if err != nil {
		return nil, err
	}
	in.Targets = targets

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
	if len(in.Targets) == 0 {
		return errors.New("no audit targets resolved")
	}
	if !in.WindowEnd.After(in.WindowStart) {
		return fmt.Errorf("--end (%s) must be after --start (%s)",
			in.WindowEnd.UTC().Format(time.RFC3339),
			in.WindowStart.UTC().Format(time.RFC3339))
	}
	return nil
}

// resolveTargetsFromFlags returns the list of audit targets, sourced from one of (in priority order):
//   - namespace (+ optional scheduleID) for a single target
//   - file path (or "-" for stdin via the provided Reader) for many targets
//
// Takes flag values directly (not cli.Context) so tests don't need to fake a context.
func resolveTargetsFromFlags(namespace, scheduleID, file string, stdin io.Reader) ([]auditTarget, error) {
	switch {
	case namespace != "" && file != "":
		return nil, errors.New("--namespace and --file are mutually exclusive")
	case file != "" && scheduleID != "":
		return nil, errors.New("--schedule-id is only valid with --namespace; for multiple targets put schedule IDs inline in --file as 'namespace,schedule_id'")
	case namespace != "":
		return []auditTarget{{Namespace: namespace, ScheduleID: scheduleID}}, nil
	case file != "":
		return loadTargets(file, stdin)
	default:
		return nil, errors.New("one of --namespace or --file is required")
	}
}

// loadTargets reads audit targets from a file path (or the provided stdin Reader when path is "-") and parses them.
func loadTargets(path string, stdin io.Reader) ([]auditTarget, error) {
	r := stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	targets, err := parseTargetLines(r)
	if err != nil {
		return nil, fmt.Errorf("--file: %w", err)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("--file %q has no target entries", path)
	}
	return targets, nil
}

// parseTargetLines parses an io.Reader of audit-target lines. Each non-blank, non-comment line is 'namespace' or
// 'namespace,schedule_id'. Whitespace around fields is trimmed. Returns an error if any line has more than one comma.
func parseTargetLines(r io.Reader) ([]auditTarget, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var out []auditTarget
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) > 2 {
			return nil, fmt.Errorf("malformed line %q: expected 'namespace' or 'namespace,schedule_id'", line)
		}
		t := auditTarget{Namespace: strings.TrimSpace(parts[0])}
		if t.Namespace == "" {
			return nil, fmt.Errorf("malformed line %q: namespace is empty", line)
		}
		if len(parts) == 2 {
			t.ScheduleID = strings.TrimSpace(parts[1])
			if t.ScheduleID == "" {
				return nil, fmt.Errorf("malformed line %q: schedule_id is empty after comma", line)
			}
		}
		out = append(out, t)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// expectedFireTimes returns the nominal fire times the spec would produce in (start, end]. Uses the server's own spec
// compiler so the semantics exactly match the live scheduler.
func expectedFireTimes(spec *schedulepb.ScheduleSpec, start, end time.Time) ([]time.Time, error) {
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
		// Pass empty jitter seed: only GetNextTimeResult.Nominal is consumed, which is computed without the seed
		// (the seed only feeds GetNextTimeResult.Next, which we discard).
		res := compiled.GetNextTime("", cursor)
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
// For "never-skip-by-design" overlap policies (ALLOW_ALL, CANCEL_OTHER, TERMINATE_OTHER), the skip_overlap heuristic
// is bypassed because the policy never policy-skips a fire; any unmatched nominal time must be a true real_miss.
// For all other policies (SKIP, BUFFER_ONE, BUFFER_ALL, UNSPECIFIED), the heuristic applies: skip_overlap when some
// chain was active at the expected fire moment, real_miss otherwise.
func classifyFirings(r *scheduleResult, expected []time.Time, tl *timeline, policies *schedulepb.SchedulePolicies) {
	r.Categories = map[time.Time]string{}
	r.Counts = map[string]int{}
	r.Expected = len(expected)
	r.Actual = len(tl.byWorkflowID)

	neverSkips := neverSkipsByPolicy(policies)
	for _, et := range expected {
		if tl.matchNominal(et) != nil {
			r.Matched++
			continue
		}
		r.Missed = append(r.Missed, et)
		if !neverSkips && len(tl.activeAt(et)) > 0 {
			r.Categories[et] = categorySkipOverlap
			r.Counts[categorySkipOverlap]++
			continue
		}
		r.Categories[et] = categoryRealMiss
		r.Counts[categoryRealMiss]++
	}
}

// neverSkipsByPolicy reports whether the overlap policy always dispatches per nominal fire. For these policies,
// any unmatched nominal is a real_miss.
func neverSkipsByPolicy(policies *schedulepb.SchedulePolicies) bool {
	switch policies.GetOverlapPolicy() {
	case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		return true
	default:
		return false
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
	// ListSchedules pages through every schedule in the namespace, calls DescribeSchedule per schedule, and projects
	// the combined data into a scheduleEntry.
	ListSchedules(ctx context.Context, namespace string) ([]scheduleEntry, error)
	// LookupSchedule fetches a single schedule's full data via DescribeSchedule.
	LookupSchedule(ctx context.Context, namespace, scheduleID string) (scheduleEntry, error)
	// NamespaceRetention returns the workflow execution retention TTL for the namespace.
	NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error)
}

// scheduleEntry carries everything the audit needs to classify a schedule
type scheduleEntry struct {
	ID           string
	Spec         *schedulepb.ScheduleSpec
	WorkflowType string
	// Paused: skip analysis to avoid flagging never-fired runs as real_miss.
	Paused     bool
	Policies   *schedulepb.SchedulePolicies
	CreateTime time.Time
	UpdateTime time.Time
	// Exhausted: limited_actions schedule with remaining_actions == 0; dropped at load time.
	Exhausted     bool
	CatchupWindow time.Duration
}

// executionLoader fetches every scheduled workflow that was alive at some moment in [windowStart, queryEnd] -- where
// "alive" is the natural interval predicate StartTime <= queryEnd AND (CloseTime >= windowStart OR CloseTime IS NULL).
// One paginated query covers everything our analysis needs: in-window starts, still-running long-runners, closed
// long-runners, and heavily-delayed BUFFER fires. When scheduleIDFilter is set, the query restricts to that one
// schedule.
type executionLoader interface {
	ListExecutions(ctx context.Context, namespace, scheduleIDFilter string, windowStart, queryEnd time.Time) (map[string][]timelineEntry, error)
}

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
)

// scheduleResult is the per-schedule output for downstream writers.
type scheduleResult struct {
	Namespace    string
	ScheduleID   string
	WorkflowType string

	Expected   int
	Actual     int
	Matched    int
	Missed     []time.Time
	Categories map[time.Time]string
	Counts     map[string]int
	// CatchupWindowSeconds is the schedule's configured catchup window in seconds. Lets operators correlate a
	// real_miss with a known scheduler outage duration: if catchup_window < outage gap, fires are dropped permanently.
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

// loadSchedules returns the schedules to analyze. Each entry carries everything needed for classification (Spec,
// Policies, CreateTime, UpdateTime, Exhausted, CatchupWindow, etc.), fetched via DescribeSchedule per schedule
// either as part of ListSchedules pagination or as a one-shot LookupSchedule when --schedule-id is set. Schedules
// that cannot possibly produce a useful audit row are dropped here:
//   - Paused: spec evaluates to fire times but the scheduler won't fire them.
//   - Exhausted: limited_actions with remaining_actions == 0; same outcome as paused.
//   - CreateTime >= WindowEnd: schedule didn't exist for any part of the audit window.
//
// Schedules deleted between list and describe (NotFound race) are also skipped inside the loader.
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
		if s.Paused || s.Exhausted {
			continue
		}
		if !s.CreateTime.IsZero() && !s.CreateTime.Before(a.WindowEnd) {
			continue
		}
		active = append(active, s)
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
			res, err := a.analyzeSchedule(s, entries)
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

// analyzeSchedule runs the audit for a single schedule against the pre-fetched timeline entries.
func (a *scheduleAuditor) analyzeSchedule(s scheduleEntry, entries []timelineEntry) (*scheduleResult, error) {
	// If the spec was modified after the audit window began, the current spec doesn't describe what was firing
	// earlier in the window. Short-circuit: mark all fires as inconclusive_schedule_changed; don't attempt to match.
	if !s.UpdateTime.IsZero() && s.UpdateTime.After(a.WindowStart) {
		return a.inconclusiveResult(s)
	}
	// Truncate expectedStart to CreateTime when the schedule was created mid-window. Drops phantom expected fires
	// from before the schedule existed.
	expectedStart := a.WindowStart
	if !s.CreateTime.IsZero() && s.CreateTime.After(expectedStart) {
		expectedStart = s.CreateTime
	}
	expected, err := expectedFireTimes(s.Spec, expectedStart, a.WindowEnd)
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
		Namespace:            a.Namespace,
		ScheduleID:           s.ID,
		WorkflowType:         s.WorkflowType,
		CatchupWindowSeconds: int64(s.CatchupWindow.Seconds()),
	}
	classifyFirings(r, expected, tl, s.Policies)
	return r, nil
}

// inconclusiveResult constructs a result for a schedule whose spec was modified mid-window. All expected fires are
// labeled inconclusive_schedule_changed because the current spec can't be trusted to describe what was firing
// earlier in the window.
func (a *scheduleAuditor) inconclusiveResult(s scheduleEntry) (*scheduleResult, error) {
	expected, err := expectedFireTimes(s.Spec, a.WindowStart, a.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("expected fires for %s: %w", s.ID, err)
	}
	if len(expected) == 0 {
		return nil, nil
	}
	r := &scheduleResult{
		Namespace:            a.Namespace,
		ScheduleID:           s.ID,
		WorkflowType:         s.WorkflowType,
		CatchupWindowSeconds: int64(s.CatchupWindow.Seconds()),
		Expected:             len(expected),
		Missed:               append([]time.Time(nil), expected...),
		Categories:           make(map[time.Time]string, len(expected)),
		Counts:               map[string]int{categoryInconclusiveChanged: len(expected)},
	}
	for _, et := range expected {
		r.Categories[et] = categoryInconclusiveChanged
	}
	return r, nil
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

// ListSchedules pages through ListSchedules, then calls DescribeSchedule sequentially per schedule to populate the
// full scheduleEntry (Policies, CreateTime, UpdateTime, Exhausted, CatchupWindow).
func (l *grpcScheduleLoader) ListSchedules(ctx context.Context, namespace string) ([]scheduleEntry, error) {
	var ids []string
	var pageToken []byte
	for {
		var resp *workflowservice.ListSchedulesResponse
		err := l.retrier.do(ctx, fmt.Sprintf("ListSchedules(%s)", namespace), func() error {
			var rpcErr error
			resp, rpcErr = l.client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace:       namespace,
				MaximumPageSize: visibilityPageSize,
				NextPageToken:   pageToken,
			})
			return rpcErr
		})
		if err != nil {
			return nil, err
		}
		for _, s := range resp.GetSchedules() {
			if s.GetInfo() == nil {
				continue
			}
			ids = append(ids, s.GetScheduleId())
		}
		if len(resp.GetNextPageToken()) == 0 {
			break
		}
		pageToken = resp.GetNextPageToken()
	}

	out := make([]scheduleEntry, 0, len(ids))
	for _, id := range ids {
		entry, err := l.LookupSchedule(ctx, namespace, id)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				// Deleted between list and describe, so skip.
				continue
			}
			return nil, fmt.Errorf("describe %s/%s: %w", namespace, id, err)
		}
		out = append(out, entry)
	}
	return out, nil
}

// LookupSchedule fetches a single schedule via DescribeSchedule and projects the response into a scheduleEntry,
// including the policies, create/update times, exhausted/paused state, and catchup window needed for classification.
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
	info := resp.GetInfo()
	sched := resp.GetSchedule()
	spec := sched.GetSpec()
	state := sched.GetState()
	policies := sched.GetPolicies()
	entry := scheduleEntry{
		ID:           scheduleID,
		Spec:         spec,
		WorkflowType: sched.GetAction().GetStartWorkflow().GetWorkflowType().GetName(),
		Paused:       state.GetPaused(),
		Policies:     policies,
		Exhausted:    state.GetLimitedActions() && state.GetRemainingActions() == 0,
	}
	if ct := info.GetCreateTime(); ct != nil {
		entry.CreateTime = ct.AsTime()
	}
	if ut := info.GetUpdateTime(); ut != nil {
		entry.UpdateTime = ut.AsTime()
	}
	if cw := policies.GetCatchupWindow(); cw != nil {
		entry.CatchupWindow = cw.AsDuration()
	}
	return entry, nil
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

// grpcExecutionLoader pages through ListWorkflowExecutions for an entire namespace in a single visibility query,
// returning entries grouped by schedule ID. The query uses `TemporalScheduledById IS NOT NULL` so the server can
// satisfy it with an index scan, and pagination is shared across all schedules.
type grpcExecutionLoader struct {
	client  workflowservice.WorkflowServiceClient
	retrier *grpcRetrier
	log     io.Writer
}

// visibilityPageSize is the per-call page size for visibility queries (ListWorkflowExecutions, ListSchedules).
// 1000 is a safe default; the server may silently clamp to a lower max. Larger values reduce RPC count but each
// call returns more bytes (4MB gRPC limit caps practical sizes around ~5000).
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
	"expected",
	"actual",
	"matched",
	"missed",
	categoryRealMiss,
	categorySkipOverlap,
	categoryInconclusiveChanged,
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
	// Only emit the real_miss times -- the other buckets (skip_overlap, inconclusive_schedule_changed) all have
	// valid reasons, so their timestamps add noise to the output column.
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
		fmt.Sprintf("%d", r.Expected),
		fmt.Sprintf("%d", r.Actual),
		fmt.Sprintf("%d", r.Matched),
		fmt.Sprintf("%d", len(r.Missed)),
		fmt.Sprintf("%d", r.Counts[categoryRealMiss]),
		fmt.Sprintf("%d", r.Counts[categorySkipOverlap]),
		fmt.Sprintf("%d", r.Counts[categoryInconclusiveChanged]),
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
		// Sanitize path separators and dots so namespaces with literal dots (e.g. "foo.bar") or pathological values
		// like ".." cannot escape perNSDir or produce hidden files.
		safeName := strings.NewReplacer("/", "_", string(filepath.Separator), "_", ".", "_").Replace(ns)
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
		var realMisses, skips, specChanged int
		for _, r := range rs {
			realMisses += r.Counts[categoryRealMiss]
			skips += r.Counts[categorySkipOverlap]
			specChanged += r.Counts[categoryInconclusiveChanged]
		}
		if err := cw.Write([]string{
			ns,
			fmt.Sprintf("%d", len(rs)),
			fmt.Sprintf("%d", realMisses),
			fmt.Sprintf("%d", skips),
			fmt.Sprintf("%d", specChanged),
		}); err != nil {
			return err
		}
	}
	return cw.Error()
}
