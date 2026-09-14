package scheduleaudit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
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

var errScheduledTimesExceededCap = errors.New("scheduled times exceeded cap (100k)")

// scheduledTimes returns the fires whose jittered fire time falls in (start, end]. It uses the server's spec compiler.
//
// jitterSeed must be the same seed the scheduler uses (see jitterSeed) so the returned Jittered times match the real
// ones. Historical V1 executions may have used an empty seed; callers must not issue a confident verdict when that
// historical version cannot be established.
func scheduledTimes(spec *schedulepb.ScheduleSpec, jitterSeed string, start, end time.Time) ([]ScheduledTime, error) {
	if spec == nil {
		return nil, nil
	}
	builder := scheduler.NewSpecBuilder(
		dynamicconfig.GetIntPropertyFn(scheduler.DefaultWarnIterations),
		dynamicconfig.GetIntPropertyFn(0),
	)
	compiled, err := builder.NewCompiledSpec(spec)
	if err != nil {
		return nil, fmt.Errorf("compile spec: %w", err)
	}
	var out []ScheduledTime
	maxJitter := maxSpecJitter(spec)
	cursor := start.Add(-maxJitter)
	iterations := 0
	for {
		res, err := compiled.GetNextTime(jitterSeed, cursor)
		if err != nil {
			return nil, fmt.Errorf("get next time after %s: %w", cursor.UTC().Format(time.RFC3339), err)
		}
		if res.Nominal.IsZero() || res.Nominal.After(end) {
			return out, nil
		}
		if res.Next.After(start) && !res.Next.After(end) {
			out = append(out, ScheduledTime{Nominal: res.Nominal, Jittered: res.Next})
		}
		cursor = res.Next
		iterations++
		if iterations > 100_000 {
			// Defensive cap -- sub-minute schedules over very large windows could otherwise OOM. Caller should chunk windows.
			return nil, errScheduledTimesExceededCap
		}
	}
}

func maxSpecJitter(spec *schedulepb.ScheduleSpec) time.Duration {
	if spec == nil {
		return 0
	}
	return max(0, min(timestamp.DurationValue(spec.GetJitter()), time.Duration(math.MaxUint32)*time.Millisecond))
}

// jitterSeed reproduces the CHASM scheduler's per-schedule jitter seed (chasm/lib/scheduler Scheduler.jitterSeed:
// "{namespaceID}-{scheduleID}"), so scheduledTimes yields the same jittered fire times the current scheduler uses.
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

func (s startedWorkflows) blockingAt(at time.Time) *startedWorkflow {
	for _, w := range s {
		if w.activeAt(at) {
			return w
		}
	}
	return nil
}

// desiredTime reconstructs serial overlap eligibility from every earlier scheduled action. Considering actions by
// nominal order captures a BufferAll queue where an intermediate action starts after this action's fire time.
func (s startedWorkflows) desiredTime(self *startedWorkflow, actual time.Time) time.Time {
	desired := actual
	for _, w := range s {
		if w == self || w.StillRunning || !w.NominalTime.Before(self.NominalTime) {
			continue
		}
		if w.ChainEnd.After(desired) {
			desired = w.ChainEnd
		}
	}
	return desired
}

// matchNominal uses strict equality: the server's spec compiler produces whole-second UTC nominal times and our local
// scheduledTimes uses the same compiler, so both sides are byte-identical when the spec hasn't changed.
func (s startedWorkflows) matchNominal(scheduled time.Time) *startedWorkflow {
	for _, w := range s {
		if w.NominalTime.Equal(scheduled) {
			return w
		}
	}
	return nil
}

// overlapClass groups overlap policies by what the scheduler does when a scheduled time arrives while a prior action
// of the same schedule is still active. It determines whether an unmatched scheduled time is a legitimate skip or a
// real miss. Source of truth: service/worker/scheduler/buffer.go (ProcessBuffer) and workflow.go (resolveOverlapPolicy).
type overlapClass int

const (
	// dropsOnOverlap: the scheduler permanently discards a scheduled time that overlaps a running action.
	// SKIP drops every overlap; BUFFER_ONE keeps the first overlap and drops the rest; UNSPECIFIED resolves to SKIP.
	dropsOnOverlap overlapClass = iota
	// delaysOnOverlap buffers every overlapping action to run serially.
	delaysOnOverlap
	// replacesOnOverlap cancels or terminates the running action and retains only the newest queued action.
	replacesOnOverlap
	// concurrent: ALLOW_ALL runs every scheduled time immediately regardless of overlap; unmatched is always real miss.
	concurrent
)

func (c overlapClass) String() string {
	switch c {
	case dropsOnOverlap:
		return "drops"
	case delaysOnOverlap:
		return "delays"
	case replacesOnOverlap:
		return "replaces"
	case concurrent:
		return "concurrent"
	default:
		return "unknown"
	}
}

func overlapClassOf(policy enumspb.ScheduleOverlapPolicy) overlapClass {
	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
		return delaysOnOverlap
	case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		return replacesOnOverlap
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

// classify matches expected fires to workflows and classifies the unmatched fires. Overlap decisions use the jittered
// fire time, while matching uses the nominal time stamped in visibility.
func classify(r *Result, scheduled []ScheduledTime, inWindow, active startedWorkflows, policy enumspb.ScheduleOverlapPolicy) {
	r.Missed = map[time.Time]string{}
	r.Expected = len(scheduled)
	r.Actual = len(inWindow)

	policy = resolveOverlapPolicy(policy)
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER || policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER {
		classifyReplacingPolicy(r, scheduled, inWindow, active)
		return
	}
	var bufferOne bufferOneState
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE {
		if blocker := active.blockingAt(r.WindowStart); blocker != nil {
			bufferOne.unknown = true
			bufferOne.until = blocker.ChainEnd
		}
	}
	for _, st := range scheduled {
		fireTime := st.Jittered
		bufferOne.advance(fireTime)
		if matched := inWindow.matchNominal(st.Nominal); matched != nil {
			r.Matched++
			bufferOne.observeMatch(policy, fireTime, matched, active)
			continue
		}
		r.Missed[st.Nominal] = classifyUnmatched(policy, active.blockingAt(fireTime), &bufferOne)
	}
}

type bufferOneState struct {
	occupied bool
	unknown  bool
	until    time.Time
}

func (b *bufferOneState) advance(at time.Time) {
	if b.occupied && !b.until.IsZero() && !at.Before(b.until) {
		b.occupied = false
	}
	if b.unknown && !b.until.IsZero() && !at.Before(b.until) {
		b.unknown = false
	}
}

func (b *bufferOneState) occupy(blocker *startedWorkflow) {
	b.occupied = true
	b.until = blocker.ChainEnd
}

func (b *bufferOneState) observeMatch(
	policy enumspb.ScheduleOverlapPolicy,
	fireTime time.Time,
	matched *startedWorkflow,
	active startedWorkflows,
) {
	if policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE {
		return
	}
	if blocker := active.blockingAt(fireTime); blocker != nil && blocker.WorkflowID != matched.WorkflowID {
		b.occupy(blocker)
	}
}

func classifyUnmatched(
	policy enumspb.ScheduleOverlapPolicy,
	blocker *startedWorkflow,
	bufferOne *bufferOneState,
) string {
	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
		if blocker != nil {
			return categorySkipOverlap
		}
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
		if bufferOne.unknown {
			return categoryInconclusiveOverlap
		}
		if bufferOne.occupied {
			return categorySkipOverlap
		}
		if blocker != nil {
			bufferOne.occupy(blocker)
			if blocker.StillRunning {
				return categoryPending
			}
		}
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
		if blocker != nil && blocker.StillRunning {
			return categoryPending
		}
	default:
		return categoryRealMiss
	}
	return categoryRealMiss
}

type replacingCandidate struct {
	nominal time.Time
	blocker *startedWorkflow
}

func classifyReplacingPolicy(r *Result, scheduled []ScheduledTime, inWindow, active startedWorkflows) {
	var candidate *replacingCandidate
	finishCandidate := func() {
		if candidate == nil {
			return
		}
		category := categoryRealMiss
		if candidate.blocker.StillRunning {
			category = categoryPending
		}
		r.Missed[candidate.nominal] = category
		candidate = nil
	}
	for _, st := range scheduled {
		fireTime := st.Jittered
		if candidate != nil && !candidate.blocker.StillRunning && !fireTime.Before(candidate.blocker.ChainEnd) {
			finishCandidate()
		}
		blocker := active.blockingAt(fireTime)
		if matched := inWindow.matchNominal(st.Nominal); matched != nil {
			r.Matched++
			if candidate != nil && blocker == candidate.blocker {
				r.Missed[candidate.nominal] = categorySkipOverlap
				candidate = nil
			}
			continue
		}
		if blocker == nil {
			r.Missed[st.Nominal] = categoryRealMiss
			continue
		}
		if candidate != nil && blocker == candidate.blocker {
			r.Missed[candidate.nominal] = categorySkipOverlap
		}
		candidate = &replacingCandidate{nominal: st.Nominal, blocker: blocker}
	}
	finishCandidate()
}

func resolveOverlapPolicy(policy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return policy
}

// classifyPaused classifies a currently-paused schedule whose pause predates the window: any scheduled time without a
// matching workflow is benign (categoryPaused), not a real_miss, because the scheduler intentionally wasn't firing.
func classifyPaused(r *Result, scheduled []time.Time, inWindow startedWorkflows) {
	r.Missed = map[time.Time]string{}
	r.Expected = len(scheduled)
	r.Actual = len(inWindow)
	for _, st := range scheduled {
		if inWindow.matchNominal(st) != nil {
			r.Matched++
			continue
		}
		r.Missed[st] = categoryPaused
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

// ExecutionLoader fetches workflows for a batch of schedules, grouped by schedule ID.
type ExecutionLoader interface {
	ListExecutions(ctx context.Context, namespace string, scheduleIDs []string, queryStart, windowEnd time.Time) (map[string][]Execution, error)
}

// At the default 1,000-byte ID limit, 25 IDs keep the query near 25 KiB while still amortizing visibility requests.
const (
	executionBatchSize   = 25
	batchesPerNamespace  = 4
	defaultLateThreshold = time.Minute
)

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
	// <= 0. Within each namespace, bounded schedule batches are analyzed concurrently.
	Concurrency int
	// IncludePaused audits currently-paused schedules instead of dropping them. Their unmatched scheduled times are
	// classified paused (benign) rather than real_miss, unless the pause/change happened mid-window (then inconclusive).
	IncludePaused bool
	// DelayThreshold separates operationally late starts from on-time starts in per-action classifications. Values <= 0
	// use the default threshold.
	DelayThreshold time.Duration

	// Progress receives one-line progress updates. Required -- callers that don't want logs should pass io.Discard.
	Progress io.Writer

	Schedules  ScheduleLoader
	Executions ExecutionLoader
	Stats      *Stats
}

// Stats summarizes schedules discovered and processed during an audit. Its methods are safe for concurrent namespace
// workers and schedule jobs.
type Stats struct {
	mu      sync.Mutex
	listed  int
	audited int
	skipped int
}

// StatsSnapshot is a stable view of audit progress after the audit has completed.
type StatsSnapshot struct {
	Listed  int
	Audited int
	Skipped int
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) addListed() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.listed++
	s.mu.Unlock()
}

func (s *Stats) addAudited() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.audited++
	s.mu.Unlock()
}

func (s *Stats) addSkipped() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.skipped++
	s.mu.Unlock()
}

func (s *Stats) Snapshot() StatsSnapshot {
	if s == nil {
		return StatsSnapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return StatsSnapshot{Listed: s.listed, Audited: s.audited, Skipped: s.skipped}
}

// Classification bucket names. Used as values in Result.Missed and as JSONL count keys.
const (
	auditStatusComplete                    = "complete"
	auditStatusInconclusiveScheduleChanged = "inconclusive_schedule_changed"
	auditStatusInconclusiveJitterSeed      = "inconclusive_jitter_seed"

	categoryOnTime              = "on_time"
	categoryLate                = "late"
	categoryDelayedOverlap      = "delayed_overlap"
	categoryRealMiss            = "real_miss"
	categorySkipOverlap         = "skip_overlap"
	categoryPending             = "pending"
	categoryInconclusiveOverlap = "inconclusive_overlap"
	// categoryPaused marks an unmatched scheduled time on a currently-paused schedule whose pause predates the window:
	// the scheduler intentionally didn't fire it, so it is benign rather than a real_miss.
	categoryPaused = "paused"
)

// Result is the complete analysis record for one schedule: the identity, the audit window, every input the
// classification used (overlap policy, scheduled times, observed executions), and the classification itself.
type Result struct {
	Namespace    string
	ScheduleID   string
	WorkflowType string

	WindowStart time.Time
	WindowEnd   time.Time
	AuditStatus string

	OverlapPolicy enumspb.ScheduleOverlapPolicy
	CatchupWindow time.Duration
	CreateTime    time.Time
	UpdateTime    time.Time
	// Paused is the schedule's current pause state (only ever true when the run included paused schedules).
	Paused bool
	// DelayThreshold is the effective threshold used to classify started actions as late.
	DelayThreshold time.Duration

	// Scheduled is every nominal/jittered fire in the window under the current scheduler seed.
	Scheduled []ScheduledTime
	Observed  []Execution

	// Delays decomposes how late each started action was (one per observed action). It is empty when AuditStatus is
	// inconclusive because the required historical schedule state cannot be reconstructed.
	Delays []ActionDelay

	Expected int
	Actual   int
	Matched  int
	// Missed maps each unmatched nominal time to its category.
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
	Category      string        // on_time, late, or delayed_overlap
}

// TotalMissed is the number of unmatched scheduled times across all categories.
func (r Result) TotalMissed() int { return len(r.Missed) }

func (r Result) auditStatus() string {
	if r.AuditStatus == "" {
		return auditStatusComplete
	}
	return r.AuditStatus
}

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

func (r Result) CountDelay(category string) int {
	n := 0
	for _, d := range r.Delays {
		if d.Category == category {
			n++
		}
	}
	return n
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
func buildDelays(
	inWindow, active startedWorkflows,
	jittered map[time.Time]time.Time,
	policy enumspb.ScheduleOverlapPolicy,
	lateThreshold time.Duration,
) []ActionDelay {
	lateThreshold = effectiveLateThreshold(lateThreshold)
	out := make([]ActionDelay, 0, len(inWindow))
	policy = resolveOverlapPolicy(policy)
	for _, w := range inWindow {
		n := w.NominalTime
		a := n
		if j, ok := jittered[n]; ok {
			a = j
		}
		d := a
		if policy != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP && policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
			d = active.desiredTime(w, a)
		}
		s := w.ChainStart
		delay := ActionDelay{
			WorkflowID:    w.WorkflowID,
			Nominal:       n,
			Actual:        a,
			Desired:       d,
			Start:         s,
			JitterOffset:  a.Sub(n),
			OverlapWait:   max(0, d.Sub(a)),
			DispatchDelay: s.Sub(d),
			E2EDelay:      s.Sub(a),
			Category:      categoryOnTime,
		}
		if lateThreshold > 0 && delay.DispatchDelay >= lateThreshold {
			delay.Category = categoryLate
		} else if delay.OverlapWait > 0 {
			delay.Category = categoryDelayedOverlap
		}
		out = append(out, delay)
	}
	return out
}

func effectiveLateThreshold(configured time.Duration) time.Duration {
	if configured <= 0 {
		return defaultLateThreshold
	}
	return configured
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

type preparedJob struct {
	scheduleJob
	entry       ScheduleEntry
	scheduled   []ScheduledTime
	auditStatus string
}

func (p preparedJob) scheduleChangedInWindow() bool {
	return !p.entry.UpdateTime.IsZero() && p.entry.UpdateTime.After(p.windowStart)
}

// Run consumes targets and calls emit once per analyzed schedule, as soon as that schedule's analysis completes.
// Schedules with no scheduled times in the window, paused/exhausted schedules, and namespaces whose window is past
// retention are silently omitted. emit is always invoked from the single collector goroutine, so it needs no
// synchronization.
//
// The pipeline streams end to end. A pool of Concurrency namespace workers pulls targets; each worker fully processes
// one target -- a single named schedule, or (for a whole-namespace target) every schedule paged in from
// ListScheduleIDs -- before taking the next. Within a namespace, schedules are described and filtered in bounded
// batches, one visibility query loads each batch, and results are classified and streamed to a single collector.
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
		return a.runBatch(ctx, []scheduleJob{job}, results)
	}
	return a.fanOutNamespace(ctx, t.Namespace, ns.info.ID, ns.windowStart, results)
}

// fanOutNamespace pages schedule IDs into a fixed number of concurrent batches. Request limiters independently pace
// describe and visibility calls. Backpressure keeps memory bounded regardless of namespace size.
func (a *Auditor) fanOutNamespace(ctx context.Context, namespace, namespaceID string, windowStart time.Time, results chan<- Result) error {
	// A failed schedule must not cancel ListSchedules or retry backoff for its siblings. Only the caller's context
	// cancels requests in this namespace; g.Wait reports the first schedule error after the in-flight work drains.
	var g errgroup.Group
	g.SetLimit(batchesPerNamespace)

	_, _ = fmt.Fprintf(a.Progress, "  listing schedules in %s...\n", namespace)
	batch := make([]scheduleJob, 0, executionBatchSize)
	flush := func() {
		jobs := batch
		batch = make([]scheduleJob, 0, executionBatchSize)
		g.Go(func() error {
			return a.runBatch(ctx, jobs, results)
		})
	}
	listErr := a.Schedules.ListScheduleIDs(ctx, namespace, func(id string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		a.Stats.addListed()
		batch = append(batch, scheduleJob{namespace: namespace, namespaceID: namespaceID, windowStart: windowStart, scheduleID: id})
		if len(batch) == executionBatchSize {
			flush()
		}
		return nil
	})
	if len(batch) > 0 {
		flush()
	}
	waitErr := g.Wait()
	if listErr != nil {
		return fmt.Errorf("list schedules in %s: %w", namespace, listErr)
	}
	return waitErr
}

func (a *Auditor) runBatch(ctx context.Context, jobs []scheduleJob, results chan<- Result) error {
	prepared := make([]preparedJob, 0, len(jobs))
	var firstErr error
	for _, job := range jobs {
		p, err := a.prepareJob(ctx, job)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if p == nil {
			a.Stats.addSkipped()
			continue
		}
		prepared = append(prepared, *p)
	}
	if len(prepared) == 0 {
		return firstErr
	}

	ids := make([]string, len(prepared))
	for i := range prepared {
		ids[i] = prepared[i].scheduleID
	}
	queryStart := prepared[0].windowStart.Add(-maxSpecJitter(prepared[0].entry.Spec))
	for _, p := range prepared {
		candidateStart := p.windowStart.Add(-maxSpecJitter(p.entry.Spec))
		if candidateStart.Before(queryStart) {
			queryStart = candidateStart
		}
	}
	entries, err := a.Executions.ListExecutions(ctx, prepared[0].namespace, ids, queryStart, a.WindowEnd)
	if err != nil {
		return fmt.Errorf("list executions in %s for %d schedules: %w", prepared[0].namespace, len(prepared), err)
	}
	for _, p := range prepared {
		res := a.analyzeSchedule(p, entries[p.scheduleID])
		a.Stats.addAudited()
		select {
		case results <- *res:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return firstErr
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

// prepareJob describes and filters one schedule, then computes its expected fires before any visibility query.
func (a *Auditor) prepareJob(ctx context.Context, job scheduleJob) (*preparedJob, error) {
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
	if entry.Exhausted {
		return nil, nil
	}
	if entry.Paused && !a.IncludePaused {
		return nil, nil
	}
	if !entry.CreateTime.IsZero() && !entry.CreateTime.Before(a.WindowEnd) {
		return nil, nil
	}

	inconclusive := !entry.UpdateTime.IsZero() && entry.UpdateTime.After(job.windowStart)
	if inconclusive {
		return &preparedJob{
			scheduleJob: job,
			entry:       entry,
			auditStatus: auditStatusInconclusiveScheduleChanged,
		}, nil
	}
	scheduledStart := job.windowStart
	if !entry.CreateTime.IsZero() && entry.CreateTime.After(scheduledStart) {
		scheduledStart = entry.CreateTime
	}
	scheduled, err := scheduledTimes(entry.Spec, jitterSeed(job.namespaceID, entry.ID), scheduledStart, a.WindowEnd)
	if err != nil {
		if errors.Is(err, errScheduledTimesExceededCap) && !job.explicit {
			_, _ = fmt.Fprintf(a.Progress, "    %s: WARNING - skipping schedule with more than 100k fires in the audit window\n", entry.ID)
			return nil, nil
		}
		return nil, fmt.Errorf("scheduled times for %s: %w", entry.ID, err)
	}
	if len(scheduled) == 0 {
		return nil, nil
	}
	auditStatus := auditStatusComplete
	if timestamp.DurationValue(entry.Spec.GetJitter()) > 0 {
		auditStatus = auditStatusInconclusiveJitterSeed
	}
	return &preparedJob{scheduleJob: job, entry: entry, scheduled: scheduled, auditStatus: auditStatus}, nil
}

// analyzeSchedule classifies one prepared schedule against its visibility rows.
func (a *Auditor) analyzeSchedule(p preparedJob, entries []Execution) *Result {
	s := p.entry
	nominals := nominalTimes(p.scheduled)
	// The query returns in-window-nominal actions plus pre-window long-runners (blockers). Overlap classification and
	// desired-time reconstruction need the full set (active); matching, the Actual count, delays, and the Observed
	// output are scoped to the in-window actions.
	active := groupExecutions(entries)
	inWindowEntries := filterScheduled(entries, p.scheduled)
	if p.scheduleChangedInWindow() {
		inWindowEntries = filterInWindow(entries, p.windowStart, a.WindowEnd)
	}
	inWindow := groupExecutions(inWindowEntries)
	r := a.baseResult(p, inWindowEntries)
	if p.scheduleChangedInWindow() {
		r.Actual = len(inWindow)
		return r
	}
	if p.auditStatus == auditStatusInconclusiveJitterSeed {
		r.Expected = len(nominals)
		for _, st := range nominals {
			if inWindow.matchNominal(st) != nil {
				r.Matched++
			}
		}
		r.Actual = len(inWindow)
		return r
	}
	if s.Paused {
		// Paused before the window and unchanged since (a mid-window pause bumps UpdateTime -> the inconclusive path
		// above). The scheduler wasn't firing, so unmatched times are benign rather than real_miss.
		classifyPaused(r, nominals, inWindow)
		return r
	}
	policy := s.Policies.GetOverlapPolicy()
	classify(r, p.scheduled, inWindow, active, policy)
	r.Delays = buildDelays(inWindow, active, jitterByNominal(p.scheduled), policy, a.DelayThreshold)
	return r
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

func filterScheduled(entries []Execution, scheduled []ScheduledTime) []Execution {
	wanted := make(map[time.Time]struct{}, len(scheduled))
	for _, st := range scheduled {
		wanted[st.Nominal] = struct{}{}
	}
	out := make([]Execution, 0, len(entries))
	for _, e := range entries {
		if _, ok := wanted[e.NominalTime]; ok {
			out = append(out, e)
		}
	}
	return out
}

func (a *Auditor) baseResult(p preparedJob, observed []Execution) *Result {
	s := p.entry
	return &Result{
		Namespace:      p.namespace,
		ScheduleID:     s.ID,
		WorkflowType:   s.WorkflowType,
		WindowStart:    p.windowStart,
		WindowEnd:      a.WindowEnd,
		AuditStatus:    p.auditStatus,
		OverlapPolicy:  s.Policies.GetOverlapPolicy(),
		CatchupWindow:  s.CatchupWindow,
		CreateTime:     s.CreateTime,
		UpdateTime:     s.UpdateTime,
		Paused:         s.Paused,
		DelayThreshold: effectiveLateThreshold(a.DelayThreshold),
		Scheduled:      p.scheduled,
		Observed:       observed,
	}
}
