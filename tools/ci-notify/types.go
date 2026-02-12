package cinotify

import "time"

// Conclusion represents the conclusion status of a workflow run or job
type Conclusion string

const (
	ConclusionSuccess Conclusion = "success"
	ConclusionFailure Conclusion = "failure"
)

// WorkflowRun represents the GitHub workflow run information
type WorkflowRun struct {
	Name         string     `json:"name"`
	Conclusion   Conclusion `json:"conclusion"`
	HeadBranch   string     `json:"headBranch"`
	HeadSHA      string     `json:"headSha"`
	URL          string     `json:"url"`
	DisplayTitle string     `json:"displayTitle"`
	Event        string     `json:"event"`
	CreatedAt    time.Time  `json:"createdAt"`
	Jobs         []Job      `json:"jobs"`
}

// Job represents a single job in the workflow
type Job struct {
	Name        string     `json:"name"`
	Conclusion  Conclusion `json:"conclusion"`
	Status      string     `json:"status"`
	StartedAt   string     `json:"startedAt"`
	CompletedAt string     `json:"completedAt"`
	URL         string     `json:"url"`
}

// CommitInfo represents commit metadata
type CommitInfo struct {
	SHA      string
	ShortSHA string // First 7 chars
	Author   string
	Message  string
}

// FailureReport aggregates all failure information
type FailureReport struct {
	Workflow   WorkflowRun
	Commit     CommitInfo
	FailedJobs []Job
	TotalJobs  int
}

// WorkflowRunSummary represents a workflow run for success reporting
type WorkflowRunSummary struct {
	Name         string        `json:"name"`
	Conclusion   Conclusion    `json:"conclusion"`
	Event        string        `json:"event"`
	CreatedAt    time.Time     `json:"createdAt"`
	StartedAt    time.Time     `json:"startedAt"`
	UpdatedAt    time.Time     `json:"updatedAt"`
	Duration     time.Duration `json:"-"`
	HeadSHA      string        `json:"headSha"`
	DisplayTitle string        `json:"displayTitle"`
	URL          string        `json:"url"`
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
	Runs                  []WorkflowRunSummary
}
