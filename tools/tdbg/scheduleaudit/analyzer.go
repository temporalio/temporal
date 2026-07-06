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

// ScheduledTime is one fire the spec produced: the nominal (pre-jitter) time and the jittered time the scheduler
// actually intended to fire at. Jitter is a deterministic function of (nominal, jitterSeed, spec), so we reproduce the
// scheduler's intended fire time locally instead of reading it back from the started workflow.
type ScheduledTime struct {
	// Nominal is the pre-jitter time. It is what the scheduler stamps into the TemporalScheduledStartTime search
	// attribute, so it is the value we match observed workflows against.
	Nominal time.Time
	// Jittered is Nominal plus the scheduler's deterministic jitter (== nominal when the spec has no jitter). It is the
	// time the scheduler intended to fire, used as the baseline for delay measurement.
	Jittered time.Time
}

// ScheduledTimes returns the fires the spec would produce in (start, end]. It is a thin wrapper over the server's own
// spec compiler (scheduler.CompiledSpec.GetNextTime) so the semantics exactly match the live scheduler.
//
// jitterSeed must be the same seed the scheduler uses (see jitterSeed) so the returned Jittered times match the real
// ones. Matching still keys on Nominal (jitter-independent), so an incorrect seed only affects the delay numbers, not
// hit/miss classification.
func ScheduledTimes(spec *schedulepb.ScheduleSpec, jitterSeed string, start, end time.Time) ([]ScheduledTime, error) {
	if spec == nil {
		return nil, nil
	}
	builder := scheduler.NewSpecBuilder()
	compiled, err := builder.NewCompiledSpec(spec)
	if err != nil {
		return nil, fmt.Errorf("compile spec: %w", err)
	}
	var out []ScheduledTime
	cursor := start
	for {
		res := compiled.GetNextTime(jitterSeed, cursor)
		if res.Nominal.IsZero() || res.Nominal.After(end) {
			return out, nil
		}
		out = append(out, ScheduledTime{Nominal: res.Nominal, Jittered: res.Next})
		cursor = res.Nominal
		if len(out) > 100_000 {
			// Defensive cap -- sub-minute schedules over very large windows could otherwise OOM. Caller should chunk windows.
			return nil, errors.New("scheduled times exceeded cap (100k)")
		}
	}
}

// jitterSeed reproduces the CHASM scheduler's per-schedule jitter seed (chasm/lib/scheduler Scheduler.jitterSeed:
// "{namespaceID}-{scheduleID}"), so ScheduledTimes yields the same jittered fire times the scheduler used.
func jitterSeed(namespaceID, scheduleID string) string {
	return namespaceID + "-" + scheduleID
}

// nominalTimes projects the nominal times out of a slice of ScheduledTime for matching/classification.
func nominalTimes(sts []ScheduledTime) []time.Time {
	out := make([]time.Time, len(sts))
	for i, st := range sts {
		out[i] = st.Nominal
	}
	return out
}

// jitterByNominal maps each nominal time to its jittered time, so an observed action (matched by nominal) can recover
// the intended fire time it was measured against.
func jitterByNominal(sts []ScheduledTime) map[time.Time]time.Time {
	out := make(map[time.Time]time.Time, len(sts))
	for _, st := range sts {
		out[st.Nominal] = st.Jittered
	}
	return out
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

// desiredTime reconstructs when an action fired at jittered time actual became eligible to start. If a prior action of
// the same schedule was still running at actual, a delaying overlap policy held this start until that action closed, so
// eligibility is the prior action's close time -- exactly the value the scheduler records in BufferedStart.DesiredTime
// (chasm/lib/scheduler invoker.go: DesiredTime = completed.CloseTime). Otherwise the action was eligible at actual.
//
// Delaying policies run actions serially, so at most one prior action is active at any instant; a still-running blocker
// is ignored because this action could not have started behind it, and max() is defensive against grouped overlaps.
func (s startedWorkflows) desiredTime(self string, actual time.Time) time.Time {
	desired := actual
	for id, w := range s {
		if id == self || w.StillRunning {
			continue
		}
		if w.activeAt(actual) && w.ChainEnd.After(desired) {
			desired = w.ChainEnd
		}
	}
	return desired
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
// inWindow is the actions whose nominal falls inside the window (used for matching and the Actual count); active is the
// superset that also includes pre-window long-runners (used only to detect overlap at scheduled times).
//
// An unmatched scheduled time is bucketed by the schedule's overlap policy class:
//   - dropsOnOverlap: skip_overlap if some action was active at that moment (a legitimate policy skip), else real_miss.
//   - delaysOnOverlap / concurrent: always real_miss -- these policies never drop a scheduled time, so a missing
//     workflow is a genuine gap (the visibility query keys on nominal time, so a delayed action would still be matched).
func Classify(r *Result, scheduled []time.Time, inWindow, active startedWorkflows, policy enumspb.ScheduleOverlapPolicy) {
	r.Missed = map[time.Time]string{}
	r.Expected = len(scheduled)
	r.Actual = len(inWindow)

	class := overlapClassOf(policy)
	for _, st := range scheduled {
		if inWindow.matchNominal(st) {
			r.Matched++
			continue
		}
		if class == dropsOnOverlap && active.anyActiveAt(st) {
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
	// DescribeNamespace returns the namespace's UUID and workflow execution retention TTL. The UUID feeds the jitter
	// seed; the retention TTL gates the audit window.
	DescribeNamespace(ctx context.Context, namespace string) (NamespaceInfo, error)
}

// NamespaceInfo is the subset of DescribeNamespace the audit needs: the namespace's UUID (part of the scheduler's
// jitter seed) and its workflow execution retention TTL.
type NamespaceInfo struct {
	ID        string
	Retention time.Duration
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

// ExecutionLoader fetches the workflows one schedule needs for the audit, keyed on the nominal (pre-jitter) scheduled
// time: every action whose nominal falls in (windowStart, windowEnd] (regardless of when it actually started), plus any
// pre-window action still alive at windowStart (needed for overlap classification). One paginated query per schedule.
type ExecutionLoader interface {
	ListExecutions(ctx context.Context, namespace, scheduleID string, windowStart, windowEnd time.Time) ([]Execution, error)
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

	// Delays decomposes how late each started action was (one per observed action). Empty for inconclusive schedules,
	// whose changed spec makes the jittered baseline untrustworthy.
	Delays []ActionDelay

	Expected int
	Actual   int
	Matched  int
	// Missed maps each unmatched nominal time to its category (real_miss | skip_overlap | inconclusive_schedule_changed).
	Missed map[time.Time]string
}

// ActionDelay decomposes how late one started action was, from four timestamps: the nominal (pre-jitter) time N, the
// jittered intended fire time A, the eligibility time D (a prior action's close time when a delaying overlap policy
// held this start, otherwise A), and the actual workflow start S. The components sum consistently:
//
//	S - N = (A - N) + (D - A) + (S - D) = JitterOffset + OverlapWait + DispatchDelay
//
// DispatchDelay is the key "system was slow to start it" signal: the action was eligible at D but did not start until
// S, for reasons other than an intentional overlap wait.
type ActionDelay struct {
	WorkflowID string
	Nominal    time.Time // N
	Actual     time.Time // A: jittered intended fire time
	Desired    time.Time // D: eligibility time (prior close under a delaying overlap, else A)
	Start      time.Time // S: actual workflow start

	JitterOffset  time.Duration // A - N: intended load spreading
	OverlapWait   time.Duration // max(0, D - A): time held behind a prior action by the overlap policy
	DispatchDelay time.Duration // S - D: system lateness once eligible
	E2EDelay      time.Duration // S - A: total delay from the intended fire time
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

// MaxDispatchDelay returns the largest system dispatch delay across the started actions, or 0 if there were none. It
// is the "how badly was the scheduler slowed" summary used to decide whether a schedule with no misses is still worth
// flagging.
func (r Result) MaxDispatchDelay() time.Duration {
	var maxDelay time.Duration
	for _, d := range r.Delays {
		if d.DispatchDelay > maxDelay {
			maxDelay = d.DispatchDelay
		}
	}
	return maxDelay
}

func sortTimes(times []time.Time) {
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
}

// buildDelays computes the per-action delay decomposition for each in-window action. actual (A) comes from the jittered
// scheduled time matching the action's nominal (falling back to nominal when the action doesn't match a scheduled fire,
// e.g. a manual/backfill start); desired (D) is reconstructed from the close times in active, which includes pre-window
// blockers so an action held behind one is attributed correctly.
func buildDelays(inWindow, active startedWorkflows, jittered map[time.Time]time.Time) []ActionDelay {
	out := make([]ActionDelay, 0, len(inWindow))
	for _, w := range inWindow {
		n := w.NominalTime
		a := n
		if j, ok := jittered[n]; ok {
			a = j
		}
		d := active.desiredTime(w.WorkflowID, a)
		s := w.ChainStart
		out = append(out, ActionDelay{
			WorkflowID:    w.WorkflowID,
			Nominal:       n,
			Actual:        a,
			Desired:       d,
			Start:         s,
			JitterOffset:  a.Sub(n),
			OverlapWait:   max(0, d.Sub(a)),
			DispatchDelay: s.Sub(d),
			E2EDelay:      s.Sub(a),
		})
	}
	return out
}

// scheduleJob is one concrete schedule to analyze. explicit distinguishes a schedule the caller named directly (a
// NotFound is a hard error) from one discovered by listing a namespace (a NotFound just means it was deleted between
// listing and describe, so it is skipped).
type scheduleJob struct {
	namespace   string
	namespaceID string    // namespace UUID, for the jitter seed
	windowStart time.Time // effective window start after retention clamping
	scheduleID  string
	explicit    bool
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
	namespaces := newNamespaceCache(a)

	// Namespace pool: up to Concurrency targets are processed at once.
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		g.Go(func() error {
			defer workers.Done()
			for t := range targets {
				if err := a.processTarget(ctx, namespaces, t, results); err != nil {
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

// processTarget audits one target: a single named schedule, or every schedule in a whole namespace. The effective
// window start is clamped to the namespace's retention boundary (with a warning) so the audit never queries purged
// workflows, which would otherwise surface as false real_miss.
func (a *Auditor) processTarget(ctx context.Context, namespaces *namespaceCache, t Target, results chan<- Result) error {
	ns, err := namespaces.get(ctx, t.Namespace)
	if err != nil {
		return err
	}
	if t.ScheduleID != "" {
		job := scheduleJob{namespace: t.Namespace, namespaceID: ns.info.ID, windowStart: ns.windowStart, scheduleID: t.ScheduleID, explicit: true}
		return a.runJob(ctx, job, results)
	}
	return a.fanOutNamespace(ctx, t.Namespace, ns.info.ID, ns.windowStart, results)
}

// fanOutNamespace pages the namespace's schedule IDs and analyzes each one concurrently, bounded by RPS. errgroup's
// SetLimit makes the listing block once RPS analyses are in flight, providing backpressure so schedule IDs are not
// buffered ahead of the workers -- memory stays bounded regardless of how many schedules the namespace has.
func (a *Auditor) fanOutNamespace(ctx context.Context, namespace, namespaceID string, windowStart time.Time, results chan<- Result) error {
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
			return a.runJob(ctx, scheduleJob{namespace: namespace, namespaceID: namespaceID, windowStart: windowStart, scheduleID: id}, results)
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

// nsEntry is a namespace's cached describe result plus the effective window start after retention clamping.
type nsEntry struct {
	info        NamespaceInfo
	windowStart time.Time
}

// namespaceCache memoizes DescribeNamespace (retention TTL + UUID) and the clamped window start per namespace. It is
// shared across the namespace workers, so its map is mutex-guarded. Racing first lookups for the same namespace may
// each issue the RPC and clamp/warn, which is harmless (the result is deterministic bar a sub-second boundary jitter).
type namespaceCache struct {
	a    *Auditor
	mu   sync.Mutex
	seen map[string]nsEntry
}

func newNamespaceCache(a *Auditor) *namespaceCache {
	return &namespaceCache{a: a, seen: map[string]nsEntry{}}
}

func (nc *namespaceCache) get(ctx context.Context, namespace string) (nsEntry, error) {
	nc.mu.Lock()
	e, ok := nc.seen[namespace]
	nc.mu.Unlock()
	if ok {
		return e, nil
	}
	info, err := nc.a.Schedules.DescribeNamespace(ctx, namespace)
	if err != nil {
		return nsEntry{}, fmt.Errorf("describe namespace %s: %w", namespace, err)
	}
	e = nsEntry{info: info, windowStart: nc.a.clampToRetention(namespace, info.Retention)}
	nc.mu.Lock()
	nc.seen[namespace] = e
	nc.mu.Unlock()
	return e, nil
}

// clampToRetention returns the window start to use for a namespace: WindowStart, unless it precedes the retention
// boundary (now - retention), in which case it is clamped to the boundary and a warning is emitted. Visibility purges
// workflows retention-from-CloseTime ago, so querying before the boundary would surface purged runs as false real_miss;
// clamping shortens the audited window to the portion that still has data.
func (a *Auditor) clampToRetention(namespace string, retention time.Duration) time.Time {
	if retention == 0 {
		return a.WindowStart
	}
	boundary := time.Now().Add(-retention)
	if !a.WindowStart.Before(boundary) {
		return a.WindowStart
	}
	_, _ = fmt.Fprintf(a.Progress,
		"    %s: WARNING - windowStart %s is past the retention boundary (retention=%s); clamping window start to %s "+
			"to avoid false real_miss from purged workflows\n",
		namespace,
		a.WindowStart.UTC().Format(time.RFC3339),
		retention,
		boundary.UTC().Format(time.RFC3339))
	return boundary
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

	entries, err := a.Executions.ListExecutions(ctx, job.namespace, job.scheduleID, job.windowStart, a.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("list executions %s/%s: %w", job.namespace, job.scheduleID, err)
	}
	return a.analyzeSchedule(job.namespace, job.namespaceID, job.windowStart, entry, entries)
}

// analyzeSchedule runs the audit for a single schedule against its executions. A schedule whose spec was modified
// after the window began is classified inconclusive_schedule_changed (the current spec can't be trusted to describe
// what was scheduled earlier). A schedule created mid-window has its scheduled times truncated to CreateTime.
func (a *Auditor) analyzeSchedule(namespace, namespaceID string, windowStart time.Time, s ScheduleEntry, entries []Execution) (*Result, error) {
	inconclusive := !s.UpdateTime.IsZero() && s.UpdateTime.After(windowStart)

	scheduledStart := windowStart
	if !inconclusive && !s.CreateTime.IsZero() && s.CreateTime.After(scheduledStart) {
		scheduledStart = s.CreateTime
	}
	scheduled, err := ScheduledTimes(s.Spec, jitterSeed(namespaceID, s.ID), scheduledStart, a.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("scheduled times for %s: %w", s.ID, err)
	}
	if len(scheduled) == 0 {
		return nil, nil
	}

	nominals := nominalTimes(scheduled)
	// The query returns in-window-nominal actions plus pre-window long-runners (blockers). Overlap classification and
	// desired-time reconstruction need the full set (active); matching, the Actual count, delays, and the Observed
	// output are scoped to the in-window actions.
	active := groupExecutions(entries)
	inWindowEntries := filterInWindow(entries, windowStart, a.WindowEnd)
	inWindow := groupExecutions(inWindowEntries)
	r := a.baseResult(namespace, windowStart, s, nominals, inWindowEntries)
	if inconclusive {
		r.Expected = len(nominals)
		r.Actual = len(inWindow)
		r.Missed = make(map[time.Time]string, len(nominals))
		for _, st := range nominals {
			r.Missed[st] = categoryInconclusiveChanged
		}
		return r, nil
	}
	Classify(r, nominals, inWindow, active, s.Policies.GetOverlapPolicy())
	r.Delays = buildDelays(inWindow, active, jitterByNominal(scheduled))
	return r, nil
}

// filterInWindow returns the executions whose nominal time falls in (windowStart, windowEnd] -- the actions actually
// scheduled inside the window, excluding pre-window blockers that the query also returns for overlap analysis.
func filterInWindow(entries []Execution, windowStart, windowEnd time.Time) []Execution {
	out := make([]Execution, 0, len(entries))
	for _, e := range entries {
		if e.NominalTime.After(windowStart) && !e.NominalTime.After(windowEnd) {
			out = append(out, e)
		}
	}
	return out
}

func (a *Auditor) baseResult(namespace string, windowStart time.Time, s ScheduleEntry, scheduled []time.Time, observed []Execution) *Result {
	return &Result{
		Namespace:     namespace,
		ScheduleID:    s.ID,
		WorkflowType:  s.WorkflowType,
		WindowStart:   windowStart,
		WindowEnd:     a.WindowEnd,
		OverlapPolicy: s.Policies.GetOverlapPolicy(),
		CatchupWindow: s.CatchupWindow,
		CreateTime:    s.CreateTime,
		UpdateTime:    s.UpdateTime,
		Scheduled:     scheduled,
		Observed:      observed,
	}
}
