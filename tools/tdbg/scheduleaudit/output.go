package scheduleaudit

import (
	"encoding/json"
	"io"
	"slices"
	"time"
)

// row is the per-schedule JSONL output shape: the complete, self-contained analysis result for one schedule. Each
// row carries its namespace, the audit window, every input the classification used (overlap policy, scheduled times,
// observed executions), and the classification itself -- enough to replay the verdict offline.
//
// Every field is always emitted. Timestamps are time.Time so encoding/json emits RFC3339; optional ones are
// *time.Time so they render as null when unset. Durations are Go duration strings (e.g. "5m0s") rather than raw
// time.Duration, which encoding/json would emit as nanoseconds.
type row struct {
	Namespace    string `json:"namespace"`
	ScheduleID   string `json:"schedule_id"`
	WorkflowType string `json:"workflow_type"`

	Window struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	} `json:"window"`
	AsOf           time.Time `json:"as_of"`
	AuditStatus    string    `json:"audit_status"`
	DelayThreshold string    `json:"delay_threshold"`

	OverlapPolicy string     `json:"overlap_policy"`
	OverlapClass  string     `json:"overlap_class"`
	CatchupWindow string     `json:"catchup_window"`
	Paused        bool       `json:"paused"`
	CreateTime    *time.Time `json:"create_time"`
	UpdateTime    *time.Time `json:"update_time"`
	// Backend is null unless the backend was resolved for this row.
	Backend *string `json:"backend"`

	ScheduleInfo struct {
		ActionCount         int64 `json:"action_count"`
		MissedCatchupWindow int64 `json:"missed_catchup_window"`
		OverlapSkipped      int64 `json:"overlap_skipped"`
		BufferDropped       int64 `json:"buffer_dropped"`
		BufferSize          int64 `json:"buffer_size"`
	} `json:"schedule_info"`

	Expected int `json:"expected"`
	Actual   int `json:"actual"`
	Matched  int `json:"matched"`
	Missed   int `json:"missed"`

	Counts struct {
		OnTime              int `json:"on_time"`
		Late                int `json:"late"`
		DelayedOverlap      int `json:"delayed_overlap"`
		RealMiss            int `json:"real_miss"`
		SkipOverlap         int `json:"skip_overlap"`
		Pending             int `json:"pending"`
		AwaitingStart       int `json:"awaiting_start"`
		InconclusiveOverlap int `json:"inconclusive_overlap"`
		Paused              int `json:"paused"`
	} `json:"counts"`

	Misses         []missRow          `json:"misses"`
	ScheduledTimes []scheduledTimeRow `json:"scheduled_times"`
	Observed       []executionRow     `json:"observed"`
	Delays         []delayRow         `json:"delays"`
}

type missRow struct {
	Nominal  time.Time `json:"nominal"`
	Category string    `json:"category"`
	// Verification is null unless the miss was looked up directly (--verify-misses, real_miss only).
	Verification *verificationRow `json:"verification"`
}

type verificationRow struct {
	Result     string     `json:"result"`
	WorkflowID string     `json:"workflow_id"`
	RunID      string     `json:"run_id"`
	Status     string     `json:"status"`
	Start      *time.Time `json:"start"`
	Error      string     `json:"error"`
}

type scheduledTimeRow struct {
	Nominal  time.Time `json:"nominal"`
	Jittered time.Time `json:"jittered"`
}

type executionRow struct {
	WorkflowID string     `json:"workflow_id"`
	RunID      string     `json:"run_id"`
	Nominal    time.Time  `json:"nominal"`
	Start      time.Time  `json:"start"`
	Close      *time.Time `json:"close"`
	Status     string     `json:"status"`
}

// delayRow decomposes how late one started action was. The four timestamps are the inputs; the four durations are
// derived from them and rendered as Go duration strings (e.g. "5m0s"). nominal (N) -> actual (A, jittered) ->
// desired (D, eligibility) -> start (S): jitter_offset = A-N, overlap_wait = max(0, D-A), dispatch_delay = S-D
// (system lateness), e2e_delay = S-A.
type delayRow struct {
	WorkflowID string    `json:"workflow_id"`
	Category   string    `json:"category"`
	Nominal    time.Time `json:"nominal"`
	Actual     time.Time `json:"actual"`
	Desired    time.Time `json:"desired"`
	Start      time.Time `json:"start"`

	JitterOffset  string `json:"jitter_offset"`
	OverlapWait   string `json:"overlap_wait"`
	DispatchDelay string `json:"dispatch_delay"`
	E2EDelay      string `json:"e2e_delay"`
}

// RowWriter streams one JSON object per flagged schedule to an underlying writer. A schedule is flagged when it has
// missed times, or -- when delayThreshold > 0 -- when some started action's dispatch delay reaches delayThreshold
// (surfacing schedules the system was slow to start even though nothing was missed). A single RowWriter is meant to be
// driven from one goroutine (e.g. the Auditor's collector).
type RowWriter struct {
	enc            *json.Encoder
	delayThreshold time.Duration
}

// NewRowWriter returns a RowWriter. delayThreshold flags otherwise-clean schedules whose worst dispatch delay reaches
// it; pass 0 to flag on missed times only.
func NewRowWriter(w io.Writer, delayThreshold time.Duration) *RowWriter {
	return &RowWriter{enc: json.NewEncoder(w), delayThreshold: delayThreshold}
}

// Write emits one row for r if it is flagged and reports whether it did; for an unflagged schedule it is a no-op and
// returns false.
func (rw *RowWriter) Write(r Result) (bool, error) {
	if !rw.flagged(r) {
		return false, nil
	}
	if err := rw.enc.Encode(toRow(r)); err != nil {
		return false, err
	}
	return true, nil
}

func (rw *RowWriter) flagged(r Result) bool {
	return Flagged(r, rw.delayThreshold)
}

// Flagged reports whether r is emitted as a row: it is inconclusive, has missed times, or -- when delayThreshold > 0 --
// some started action's dispatch delay reaches delayThreshold.
func Flagged(r Result, delayThreshold time.Duration) bool {
	if r.auditStatus() != auditStatusComplete {
		return true
	}
	if r.TotalMissed() > 0 {
		return true
	}
	return delayThreshold > 0 && r.MaxDispatchDelay() >= delayThreshold
}

func toRow(r Result) row {
	var out row
	out.Namespace = r.Namespace
	out.ScheduleID = r.ScheduleID
	out.WorkflowType = r.WorkflowType
	out.Window.Start = r.WindowStart.UTC()
	out.Window.End = r.WindowEnd.UTC()
	out.AsOf = r.AsOf.UTC()
	out.AuditStatus = r.auditStatus()
	out.DelayThreshold = r.DelayThreshold.String()
	out.OverlapPolicy = overlapPolicyName(r.OverlapPolicy)
	out.OverlapClass = overlapClassOf(r.OverlapPolicy).String()
	out.CatchupWindow = r.CatchupWindow.String()
	out.Paused = r.Paused
	out.CreateTime = utcPtr(r.CreateTime)
	out.UpdateTime = utcPtr(r.UpdateTime)
	if r.Backend != "" {
		out.Backend = &r.Backend
	}
	out.ScheduleInfo.ActionCount = r.Info.ActionCount
	out.ScheduleInfo.MissedCatchupWindow = r.Info.MissedCatchupWindow
	out.ScheduleInfo.OverlapSkipped = r.Info.OverlapSkipped
	out.ScheduleInfo.BufferDropped = r.Info.BufferDropped
	out.ScheduleInfo.BufferSize = r.Info.BufferSize

	out.Expected = r.Expected
	out.Actual = r.Actual
	out.Matched = r.Matched
	out.Missed = r.TotalMissed()

	out.Counts.RealMiss = r.Count(categoryRealMiss)
	out.Counts.SkipOverlap = r.Count(categorySkipOverlap)
	out.Counts.Pending = r.Count(categoryPending)
	out.Counts.AwaitingStart = r.Count(categoryAwaitingStart)
	out.Counts.InconclusiveOverlap = r.Count(categoryInconclusiveOverlap)
	out.Counts.Paused = r.Count(categoryPaused)
	out.Counts.OnTime = r.CountDelay(categoryOnTime)
	out.Counts.Late = r.CountDelay(categoryLate)
	out.Counts.DelayedOverlap = r.CountDelay(categoryDelayedOverlap)

	out.Misses = missRows(r)
	out.ScheduledTimes = scheduledTimeRows(r.Scheduled)
	out.Observed = executionRows(r.Observed)
	out.Delays = delayRows(r.Delays)
	return out
}

func scheduledTimeRows(times []ScheduledTime) []scheduledTimeRow {
	sorted := append([]ScheduledTime(nil), times...)
	slices.SortFunc(sorted, func(a, b ScheduledTime) int { return a.Nominal.Compare(b.Nominal) })
	out := make([]scheduledTimeRow, 0, len(sorted))
	for _, st := range sorted {
		out = append(out, scheduledTimeRow{Nominal: st.Nominal.UTC(), Jittered: st.Jittered.UTC()})
	}
	return out
}

// missRows returns every unmatched nominal time with its category, sorted ascending by time.
func missRows(r Result) []missRow {
	nominals := make([]time.Time, 0, len(r.Missed))
	for t := range r.Missed {
		nominals = append(nominals, t)
	}
	sortTimes(nominals)
	out := make([]missRow, 0, len(nominals))
	for _, t := range nominals {
		mr := missRow{Nominal: t.UTC(), Category: r.Missed[t]}
		if v, ok := r.Verifications[t]; ok {
			vr := verificationRow{
				Result:     v.Result,
				WorkflowID: v.WorkflowID,
				RunID:      v.RunID,
				Start:      utcPtr(v.StartTime),
				Error:      v.Error,
			}
			if v.Result == verificationStarted {
				vr.Status = v.Status.String()
			}
			mr.Verification = &vr
		}
		out = append(out, mr)
	}
	return out
}

func executionRows(execs []Execution) []executionRow {
	out := make([]executionRow, 0, len(execs))
	for _, e := range execs {
		out = append(out, executionRow{
			WorkflowID: e.WorkflowID,
			RunID:      e.RunID,
			Nominal:    e.NominalTime.UTC(),
			Start:      e.StartTime.UTC(),
			Close:      utcPtr(derefTime(e.CloseTime)),
			Status:     e.Status.String(),
		})
	}
	return out
}

// delayRows returns the per-action delay decomposition, sorted ascending by nominal time for stable output.
func delayRows(delays []ActionDelay) []delayRow {
	sorted := append([]ActionDelay(nil), delays...)
	slices.SortFunc(sorted, func(a, b ActionDelay) int { return a.Nominal.Compare(b.Nominal) })
	out := make([]delayRow, 0, len(sorted))
	for _, d := range sorted {
		out = append(out, delayRow{
			WorkflowID:    d.WorkflowID,
			Category:      d.Category,
			Nominal:       d.Nominal.UTC(),
			Actual:        d.Actual.UTC(),
			Desired:       d.Desired.UTC(),
			Start:         d.Start.UTC(),
			JitterOffset:  d.JitterOffset.String(),
			OverlapWait:   d.OverlapWait.String(),
			DispatchDelay: d.DispatchDelay.String(),
			E2EDelay:      d.E2EDelay.String(),
		})
	}
	return out
}

func utcPtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
