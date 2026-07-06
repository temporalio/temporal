package scheduleaudit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/service/worker/scheduler"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ScheduledTimes returns the nominal times the spec would produce in (start, end]. It is a thin wrapper over the
// server's own spec compiler (scheduler.CompiledSpec.GetNextTime) so the semantics exactly match the live scheduler.
//
// We consume res.Nominal (the pre-jitter time), not res.Next, because that is what the scheduler stamps into the
// TemporalScheduledStartTime search attribute -- the value we match observed workflows against. The frontend's
// ListScheduleMatchingTimes RPC returns jittered .Next times, so it is not reusable here.
func ScheduledTimes(spec *schedulepb.ScheduleSpec, start, end time.Time) ([]time.Time, error) {
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
		// Empty jitter seed: only GetNextTimeResult.Nominal is consumed, which is computed without the seed
		// (the seed only feeds GetNextTimeResult.Next, which we discard).
		res := compiled.GetNextTime("", cursor)
		if res.Nominal.IsZero() || res.Nominal.After(end) {
			return out, nil
		}
		out = append(out, res.Nominal)
		cursor = res.Nominal
		if len(out) > 100_000 {
			// Defensive cap -- sub-minute schedules over very large windows could otherwise OOM. Caller should chunk windows.
			return nil, errors.New("scheduled times exceeded cap (100k)")
		}
	}
}

// Execution is a single workflow execution row observed in visibility.
type Execution struct {
	WorkflowID  string
	RunID       string
	StartTime   time.Time
	CloseTime   *time.Time // nil if still running
	Status      enumspb.WorkflowExecutionStatus
	NominalTime time.Time // from TemporalScheduledStartTime search attribute
}

// startedWorkflow is one logical scheduled action, possibly spanning many ContinueAsNew links that share the same
// WorkflowID. Its lifetime is the union of its links' lifetimes, used to answer "was this action still active at
// time T?".
type startedWorkflow struct {
	WorkflowID   string
	NominalTime  time.Time
	ChainStart   time.Time // earliest start across the links
	ChainEnd     time.Time // latest close; only meaningful when StillRunning is false
	StillRunning bool
}

func (w *startedWorkflow) activeAt(at time.Time) bool {
	if w.StillRunning {
		return !at.Before(w.ChainStart)
	}
	// Fully closed: active in [ChainStart, ChainEnd).
	return !at.Before(w.ChainStart) && at.Before(w.ChainEnd)
}

// startedWorkflows holds observed executions for a single schedule, grouped into ContinueAsNew chains keyed by WorkflowID.
type startedWorkflows map[string]*startedWorkflow

func groupExecutions(entries []Execution) startedWorkflows {
	s := startedWorkflows{}
	for _, e := range entries {
		s.add(e)
	}
	return s
}

func (s startedWorkflows) add(e Execution) {
	w, ok := s[e.WorkflowID]
	if !ok {
		w = &startedWorkflow{WorkflowID: e.WorkflowID, NominalTime: e.NominalTime, ChainStart: e.StartTime}
		s[e.WorkflowID] = w
	}
	if e.StartTime.Before(w.ChainStart) {
		w.ChainStart = e.StartTime
	}
	if e.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		w.StillRunning = true
	}
	if e.CloseTime != nil && !w.StillRunning {
		if w.ChainEnd.Before(*e.CloseTime) {
			w.ChainEnd = *e.CloseTime
		}
	}
}

func (s startedWorkflows) anyActiveAt(at time.Time) bool {
	for _, w := range s {
		if w.activeAt(at) {
			return true
		}
	}
	return false
}

// matchNominal uses strict equality: the server's spec compiler produces whole-second UTC nominal times and our local
// ScheduledTimes uses the same compiler, so both sides are byte-identical when the spec hasn't changed.
func (s startedWorkflows) matchNominal(scheduled time.Time) bool {
	for _, w := range s {
		if w.NominalTime.Equal(scheduled) {
			return true
		}
	}
	return false
}

// overlapClass groups overlap policies by what the scheduler does when a scheduled time arrives while a prior action
// of the same schedule is still active. It determines whether an unmatched scheduled time is a legitimate skip or a
// real miss. Source of truth: service/worker/scheduler/buffer.go (ProcessBuffer) and workflow.go (resolveOverlapPolicy).
type overlapClass int

const (
	// dropsOnOverlap: the scheduler permanently discards a scheduled time that overlaps a running action.
	// SKIP drops every overlap; BUFFER_ONE keeps the first overlap and drops the rest; UNSPECIFIED resolves to SKIP.
	// We treat BUFFER_ONE as drops-on-overlap because after the fact we can't distinguish its one buffered action
	// from the ones it dropped.
	dropsOnOverlap overlapClass = iota
	// delaysOnOverlap: the scheduler never drops; it buffers the action to run later (BUFFER_ALL) or cancels/terminates
	// the prior action and then runs this one (CANCEL_OTHER, TERMINATE_OTHER). An unmatched scheduled time here should
	// still have produced a (possibly delayed) workflow, so it is a real miss.
	delaysOnOverlap
	// concurrent: ALLOW_ALL runs every scheduled time immediately regardless of overlap; unmatched is always real miss.
	concurrent
)

func (c overlapClass) String() string {
	switch c {
	case dropsOnOverlap:
		return "drops"
	case delaysOnOverlap:
		return "delays"
	case concurrent:
		return "concurrent"
	default:
		return "unknown"
	}
}

func overlapClassOf(policy enumspb.ScheduleOverlapPolicy) overlapClass {
	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		return delaysOnOverlap
	case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
		return concurrent
	default:
		return dropsOnOverlap // SKIP, BUFFER_ONE, and UNSPECIFIED (which the scheduler resolves to SKIP)
	}
}

// overlapPolicyName is the enum's short display name (e.g. "BufferAll").
func overlapPolicyName(policy enumspb.ScheduleOverlapPolicy) string {
	return policy.String()
}

// Classify diffs scheduled times against the observed workflows and populates Expected/Actual/Matched/Missed on r.
//
// An unmatched scheduled time is bucketed by the schedule's overlap policy class:
//   - dropsOnOverlap: skip_overlap if some action was active at that moment (a legitimate policy skip), else real_miss.
//   - delaysOnOverlap / concurrent: always real_miss -- these policies never drop a scheduled time, so a missing
//     workflow is a genuine gap (the delayed-fire-buffer query window already gave delayed actions a chance to appear).
func Classify(r *Result, scheduled []time.Time, observed startedWorkflows, policy enumspb.ScheduleOverlapPolicy) {
	r.Missed = map[time.Time]string{}
	r.Expected = len(scheduled)
	r.Actual = len(observed)

	class := overlapClassOf(policy)
	for _, st := range scheduled {
		if observed.matchNominal(st) {
			r.Matched++
			continue
		}
		if class == dropsOnOverlap && observed.anyActiveAt(st) {
			r.Missed[st] = categorySkipOverlap
			continue
		}
		r.Missed[st] = categoryRealMiss
	}
}

// ScheduleLoader fetches schedule specs from the cluster.
type ScheduleLoader interface {
	// ListScheduleIDs pages through the namespace's schedules and calls yield with each schedule ID as pages arrive,
	// so processing can start before the full listing completes. It stops and returns the error if yield returns one.
	ListScheduleIDs(ctx context.Context, namespace string, yield func(id string) error) error
	// LookupSchedule fetches a single schedule's full data via DescribeSchedule.
	LookupSchedule(ctx context.Context, namespace, scheduleID string) (ScheduleEntry, error)
	// NamespaceRetention returns the workflow execution retention TTL for the namespace.
	NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error)
}

// ScheduleEntry carries everything the audit needs to classify a schedule.
type ScheduleEntry struct {
	ID           string
	Spec         *schedulepb.ScheduleSpec
	WorkflowType string
	// Paused: skip analysis to avoid flagging never-started actions as real_miss.
	Paused     bool
	Policies   *schedulepb.SchedulePolicies
	CreateTime time.Time
	UpdateTime time.Time
	// Exhausted: limited_actions schedule with remaining_actions == 0; dropped at load time.
	Exhausted     bool
	CatchupWindow time.Duration
}

// ExecutionLoader fetches every workflow started by one schedule that was alive at some moment in
// [windowStart, queryEnd] -- where "alive" is the natural interval predicate StartTime <= queryEnd AND
// (CloseTime >= windowStart OR CloseTime IS NULL). One paginated query per schedule covers everything our analysis
// needs: in-window starts, still-running long-runners, closed long-runners, and heavily-delayed buffered actions.
type ExecutionLoader interface {
	ListExecutions(ctx context.Context, namespace, scheduleID string, windowStart, queryEnd time.Time) ([]Execution, error)
}

// Target names an audit unit: a whole namespace (ScheduleID empty) or a single schedule within it.
type Target struct {
	Namespace  string
	ScheduleID string
}

// Auditor performs the audit over a stream of targets for one time window.
type Auditor struct {
	WindowStart time.Time
	WindowEnd   time.Time

	// DelayedFireBuffer is how far past WindowEnd the visibility query keeps looking for workflows, to catch
	// heavily-delayed buffered actions whose nominal time falls in the window but whose actual StartTime lands well
	// after WindowEnd (e.g. a 5-min schedule whose workflows take a day to run).
	DelayedFireBuffer time.Duration
	// RetentionSafetyBuffer is how far inside the retention boundary WindowStart must sit to be considered safe to
	// audit. Visibility purges workflows retention-from-CloseTime ago, so a WindowStart at the boundary risks missing
	// workflows that closed near it; this slack absorbs clock skew and retention-job lag. Zero disables the slack.
	RetentionSafetyBuffer time.Duration

	// Concurrency bounds how many namespaces are audited at once (the namespace worker-pool size). Defaults to 1 if
	// <= 0. Within each namespace, schedules are fanned out and analyzed concurrently, paced by RPS.
	Concurrency int
	// RPS bounds the number of schedules a single namespace analyzes concurrently, so it does not outrun the
	// per-namespace API rate limit configured on the loaders. Defaults to 1 if <= 0.
	RPS int

	// Progress receives one-line progress updates. Required -- callers that don't want logs should pass io.Discard.
	Progress io.Writer

	Schedules  ScheduleLoader
	Executions ExecutionLoader
}

// Classification bucket names. Used as values in Result.Missed and as JSONL count keys.
const (
	categoryRealMiss            = "real_miss"
	categorySkipOverlap         = "skip_overlap"
	categoryInconclusiveChanged = "inconclusive_schedule_changed"
)

// Result is the complete analysis record for one schedule: the identity, the audit window, every input the
// classification used (overlap policy, scheduled times, observed executions), and the classification itself.
type Result struct {
	Namespace    string
	ScheduleID   string
	WorkflowType string

	WindowStart time.Time
	WindowEnd   time.Time

	OverlapPolicy enumspb.ScheduleOverlapPolicy
	CatchupWindow time.Duration
	CreateTime    time.Time
	UpdateTime    time.Time

	// Scheduled is every nominal time the spec produced in the window; Observed is every workflow execution seen in
	// visibility. Together with OverlapPolicy they are sufficient to replay the classification.
	Scheduled []time.Time
	Observed  []Execution

	Expected int
	Actual   int
	Matched  int
	// Missed maps each unmatched nominal time to its category (real_miss | skip_overlap | inconclusive_schedule_changed).
	Missed map[time.Time]string
}

// TotalMissed is the number of unmatched scheduled times across all categories.
func (r Result) TotalMissed() int { return len(r.Missed) }

// Count returns how many missed times fall in the given category.
func (r Result) Count(category string) int {
	n := 0
	for _, c := range r.Missed {
		if c == category {
			n++
		}
	}
	return n
}

// MissedTimes returns the missed nominal times in the given category, sorted ascending.
func (r Result) MissedTimes(category string) []time.Time {
	var out []time.Time
	for t, c := range r.Missed {
		if c == category {
			out = append(out, t)
		}
	}
	sortTimes(out)
	return out
}

func sortTimes(times []time.Time) {
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
}

// scheduleJob is one concrete schedule to analyze. explicit distinguishes a schedule the caller named directly (a
// NotFound is a hard error) from one discovered by listing a namespace (a NotFound just means it was deleted between
// listing and describe, so it is skipped).
type scheduleJob struct {
	namespace  string
	scheduleID string
	explicit   bool
}

// Run consumes targets and calls emit once per analyzed schedule, as soon as that schedule's analysis completes.
// Schedules with no scheduled times in the window, paused/exhausted schedules, and namespaces whose window is past
// retention are silently omitted. emit is always invoked from the single collector goroutine, so it needs no
// synchronization.
//
// The pipeline streams end to end. A pool of Concurrency namespace workers pulls targets; each worker fully processes
// one target -- a single named schedule, or (for a whole-namespace target) every schedule paged in from
// ListScheduleIDs -- before taking the next. Within a namespace, schedules are fanned out and analyzed concurrently
// (describe -> visibility query -> classify), bounded by RPS and paced by the loaders' per-namespace rate limiter, so
// a namespace with millions of schedules streams through with bounded memory. A single collector forwards results to
// emit in completion order.
func (a *Auditor) Run(ctx context.Context, targets <-chan Target, emit func(Result) error) error {
	concurrency := a.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	g, ctx := errgroup.WithContext(ctx)
	results := make(chan Result)
	retention := newRetentionCache(a)

	// Namespace pool: up to Concurrency targets are processed at once.
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		g.Go(func() error {
			defer workers.Done()
			for t := range targets {
				if err := a.processTarget(ctx, retention, t, results); err != nil {
					return err
				}
			}
			return nil
		})
	}
	// Close results once every namespace worker has finished so the collector can drain and exit.
	go func() {
		workers.Wait()
		close(results)
	}()

	// Collector (single goroutine): forward each result to emit in completion order.
	g.Go(func() error {
		for res := range results {
			if err := emit(res); err != nil {
				return err
			}
		}
		return nil
	})

	return g.Wait()
}

// processTarget audits one target: a single named schedule, or every schedule in a whole namespace. Namespaces whose
// window is past retention are skipped.
func (a *Auditor) processTarget(ctx context.Context, retention *retentionCache, t Target, results chan<- Result) error {
	skip, err := retention.skip(ctx, t.Namespace)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	if t.ScheduleID != "" {
		return a.runJob(ctx, scheduleJob{namespace: t.Namespace, scheduleID: t.ScheduleID, explicit: true}, results)
	}
	return a.fanOutNamespace(ctx, t.Namespace, results)
}

// fanOutNamespace pages the namespace's schedule IDs and analyzes each one concurrently, bounded by RPS. errgroup's
// SetLimit makes the listing block once RPS analyses are in flight, providing backpressure so schedule IDs are not
// buffered ahead of the workers -- memory stays bounded regardless of how many schedules the namespace has.
func (a *Auditor) fanOutNamespace(ctx context.Context, namespace string, results chan<- Result) error {
	inner := a.RPS
	if inner <= 0 {
		inner = 1
	}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(inner)

	_, _ = fmt.Fprintf(a.Progress, "  listing schedules in %s...\n", namespace)
	listErr := a.Schedules.ListScheduleIDs(ctx, namespace, func(id string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.Go(func() error {
			return a.runJob(ctx, scheduleJob{namespace: namespace, scheduleID: id}, results)
		})
		return nil
	})
	waitErr := g.Wait()
	if listErr != nil {
		return fmt.Errorf("list schedules in %s: %w", namespace, listErr)
	}
	return waitErr
}

// runJob analyzes one schedule and forwards a non-empty result, unblocking if the run is cancelled.
func (a *Auditor) runJob(ctx context.Context, job scheduleJob, results chan<- Result) error {
	res, err := a.analyzeJob(ctx, job)
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	select {
	case results <- *res:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retentionCache memoizes the per-namespace retention skip decision. It is shared across the namespace workers, so its
// map is mutex-guarded. Racing first lookups for the same namespace may each call checkRetention, which is harmless
// (the result is deterministic).
type retentionCache struct {
	a    *Auditor
	mu   sync.Mutex
	seen map[string]bool
}

func newRetentionCache(a *Auditor) *retentionCache {
	return &retentionCache{a: a, seen: map[string]bool{}}
}

func (rc *retentionCache) skip(ctx context.Context, namespace string) (bool, error) {
	rc.mu.Lock()
	s, ok := rc.seen[namespace]
	rc.mu.Unlock()
	if ok {
		return s, nil
	}
	s, err := rc.a.checkRetention(ctx, namespace)
	if err != nil {
		return false, err
	}
	rc.mu.Lock()
	rc.seen[namespace] = s
	rc.mu.Unlock()
	return s, nil
}

// checkRetention refuses to audit a window that crosses the retention boundary -- purged workflows would silently turn
// into false-positive real_miss. Returns (skip=true, nil) when the caller should treat the namespace as a no-op.
func (a *Auditor) checkRetention(ctx context.Context, namespace string) (bool, error) {
	retention, err := a.Schedules.NamespaceRetention(ctx, namespace)
	if err != nil {
		return false, fmt.Errorf("namespace retention %s: %w", namespace, err)
	}
	if retention == 0 {
		return false, nil
	}
	safeStart := time.Now().Add(-retention + a.RetentionSafetyBuffer)
	if !a.WindowStart.Before(safeStart) {
		return false, nil
	}
	_, _ = fmt.Fprintf(a.Progress,
		"    %s: SKIPPING - windowStart %s is past retention boundary (retention=%s, safe-start=%s)\n",
		namespace,
		a.WindowStart.UTC().Format(time.RFC3339),
		retention,
		safeStart.UTC().Format(time.RFC3339))
	return true, nil
}

// analyzeJob describes one schedule, applies the load-time filters (paused / exhausted / created at or after
// WindowEnd), runs its per-schedule visibility query, and classifies. Returns (nil, nil) when the schedule is filtered
// out or has no scheduled times in the window.
func (a *Auditor) analyzeJob(ctx context.Context, job scheduleJob) (*Result, error) {
	entry, err := a.Schedules.LookupSchedule(ctx, job.namespace, job.scheduleID)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			if job.explicit {
				return nil, fmt.Errorf("schedule %q not found in namespace %q", job.scheduleID, job.namespace)
			}
			return nil, nil // deleted between listing and describe
		}
		return nil, fmt.Errorf("lookup schedule %s/%s: %w", job.namespace, job.scheduleID, err)
	}
	if entry.Paused || entry.Exhausted {
		return nil, nil
	}
	if !entry.CreateTime.IsZero() && !entry.CreateTime.Before(a.WindowEnd) {
		return nil, nil
	}

	queryEnd := a.WindowEnd.Add(a.DelayedFireBuffer)
	entries, err := a.Executions.ListExecutions(ctx, job.namespace, job.scheduleID, a.WindowStart, queryEnd)
	if err != nil {
		return nil, fmt.Errorf("list executions %s/%s: %w", job.namespace, job.scheduleID, err)
	}
	return a.analyzeSchedule(job.namespace, entry, entries)
}

// analyzeSchedule runs the audit for a single schedule against its executions. A schedule whose spec was modified
// after the window began is classified inconclusive_schedule_changed (the current spec can't be trusted to describe
// what was scheduled earlier). A schedule created mid-window has its scheduled times truncated to CreateTime.
func (a *Auditor) analyzeSchedule(namespace string, s ScheduleEntry, entries []Execution) (*Result, error) {
	inconclusive := !s.UpdateTime.IsZero() && s.UpdateTime.After(a.WindowStart)

	scheduledStart := a.WindowStart
	if !inconclusive && !s.CreateTime.IsZero() && s.CreateTime.After(scheduledStart) {
		scheduledStart = s.CreateTime
	}
	scheduled, err := ScheduledTimes(s.Spec, scheduledStart, a.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("scheduled times for %s: %w", s.ID, err)
	}
	if len(scheduled) == 0 {
		return nil, nil
	}

	observed := groupExecutions(entries)
	r := a.baseResult(namespace, s, scheduled, entries)
	if inconclusive {
		r.Expected = len(scheduled)
		r.Actual = len(observed)
		r.Missed = make(map[time.Time]string, len(scheduled))
		for _, st := range scheduled {
			r.Missed[st] = categoryInconclusiveChanged
		}
		return r, nil
	}
	Classify(r, scheduled, observed, s.Policies.GetOverlapPolicy())
	return r, nil
}

func (a *Auditor) baseResult(namespace string, s ScheduleEntry, scheduled []time.Time, observed []Execution) *Result {
	return &Result{
		Namespace:     namespace,
		ScheduleID:    s.ID,
		WorkflowType:  s.WorkflowType,
		WindowStart:   a.WindowStart,
		WindowEnd:     a.WindowEnd,
		OverlapPolicy: s.Policies.GetOverlapPolicy(),
		CatchupWindow: s.CatchupWindow,
		CreateTime:    s.CreateTime,
		UpdateTime:    s.UpdateTime,
		Scheduled:     scheduled,
		Observed:      observed,
	}
}
