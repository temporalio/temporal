package scheduleaudit

import (
	"encoding/json"
	"io"
	"time"
)

// row is the per-schedule JSONL output shape: the complete, self-contained analysis result for one schedule. Each
// row carries its namespace, the audit window, every input the classification used (overlap policy, scheduled times,
// observed executions), and the classification itself -- enough to replay the verdict offline.
//
// Every field is always emitted. Timestamps are time.Time so encoding/json emits RFC3339; optional ones are
// *time.Time so they render as null when unset. Durations are explicit seconds because Go encodes time.Duration as
// raw nanoseconds.
type row struct {
	Namespace    string `json:"namespace"`
	ScheduleID   string `json:"schedule_id"`
	WorkflowType string `json:"workflow_type"`

	Window struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	} `json:"window"`

	OverlapPolicy       string     `json:"overlap_policy"`
	OverlapClass        string     `json:"overlap_class"`
	CatchupWindowSecond int64      `json:"catchup_window_s"`
	CreateTime          *time.Time `json:"create_time"`
	UpdateTime          *time.Time `json:"update_time"`

	Expected int `json:"expected"`
	Actual   int `json:"actual"`
	Matched  int `json:"matched"`
	Missed   int `json:"missed"`

	Counts struct {
		RealMiss                    int `json:"real_miss"`
		SkipOverlap                 int `json:"skip_overlap"`
		InconclusiveScheduleChanged int `json:"inconclusive_schedule_changed"`
	} `json:"counts"`

	Misses         []missRow      `json:"misses"`
	ScheduledTimes []time.Time    `json:"scheduled_times"`
	Observed       []executionRow `json:"observed"`
}

type missRow struct {
	Nominal  time.Time `json:"nominal"`
	Category string    `json:"category"`
}

type executionRow struct {
	WorkflowID string     `json:"workflow_id"`
	RunID      string     `json:"run_id"`
	Nominal    time.Time  `json:"nominal"`
	Start      time.Time  `json:"start"`
	Close      *time.Time `json:"close"`
	Status     string     `json:"status"`
}

// RowWriter streams one JSON object per flagged schedule to an underlying writer. Results with no missed times are
// skipped. A single RowWriter is meant to be driven from one goroutine (e.g. the Auditor's collector).
type RowWriter struct {
	enc *json.Encoder
}

func NewRowWriter(w io.Writer) *RowWriter {
	return &RowWriter{enc: json.NewEncoder(w)}
}

// Write emits one row for r, unless r has no missed times (a clean schedule), in which case it is a no-op.
func (rw *RowWriter) Write(r Result) error {
	if r.TotalMissed() == 0 {
		return nil
	}
	return rw.enc.Encode(toRow(r))
}

// WriteFlat emits one JSON object per flagged schedule to w. It is a convenience wrapper over RowWriter for callers
// that already hold the full result slice.
func WriteFlat(w io.Writer, results []Result) error {
	rw := NewRowWriter(w)
	for _, r := range results {
		if err := rw.Write(r); err != nil {
			return err
		}
	}
	return nil
}

func toRow(r Result) row {
	var out row
	out.Namespace = r.Namespace
	out.ScheduleID = r.ScheduleID
	out.WorkflowType = r.WorkflowType
	out.Window.Start = r.WindowStart.UTC()
	out.Window.End = r.WindowEnd.UTC()
	out.OverlapPolicy = overlapPolicyName(r.OverlapPolicy)
	out.OverlapClass = overlapClassOf(r.OverlapPolicy).String()
	out.CatchupWindowSecond = int64(r.CatchupWindow.Seconds())
	out.CreateTime = utcPtr(r.CreateTime)
	out.UpdateTime = utcPtr(r.UpdateTime)

	out.Expected = r.Expected
	out.Actual = r.Actual
	out.Matched = r.Matched
	out.Missed = r.TotalMissed()

	out.Counts.RealMiss = r.Count(categoryRealMiss)
	out.Counts.SkipOverlap = r.Count(categorySkipOverlap)
	out.Counts.InconclusiveScheduleChanged = r.Count(categoryInconclusiveChanged)

	out.Misses = missRows(r)
	out.ScheduledTimes = utcTimes(r.Scheduled)
	out.Observed = executionRows(r.Observed)
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
		out = append(out, missRow{Nominal: t.UTC(), Category: r.Missed[t]})
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

func utcTimes(times []time.Time) []time.Time {
	out := append([]time.Time(nil), times...)
	sortTimes(out)
	for i := range out {
		out[i] = out[i].UTC()
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
