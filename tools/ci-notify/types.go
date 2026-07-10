package cinotify

import (
	"sort"
	"time"

	"go.temporal.io/server/tools/common/github"
)

// CommitInfo represents commit metadata
type CommitInfo struct {
	SHA      string
	ShortSHA string // First 7 chars
	Author   string
	Message  string
}

// FailureReport aggregates all failure information
type FailureReport struct {
	Workflow   github.Run
	Commit     CommitInfo
	FailedJobs []github.Job
	TotalJobs  int
}

// DigestReport aggregates success metrics for a time period
type DigestReport struct {
	Branch                string
	WorkflowName          string
	StartDate             time.Time
	EndDate               time.Time
	TotalRuns             int
	SuccessfulRuns        int
	FailedRuns            int
	SuccessRate           float64
	AverageDuration       time.Duration
	MedianDuration        time.Duration
	Under20MinutesPercent float64
	Under25MinutesPercent float64
	Under30MinutesPercent float64
	Runs                  []github.Run
}

func (r *DigestReport) slowestRuns(limit int) []github.Run {
	if limit <= 0 {
		return nil
	}

	sorted := make([]github.Run, 0, len(r.Runs))
	for _, run := range r.Runs {
		if run.Duration > 0 {
			sorted = append(sorted, run)
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Duration > sorted[j].Duration
	})
	return sorted[:min(limit, len(sorted))]
}
