package cinotify

import "time"

// WorkflowRun represents the GitHub workflow run information
type WorkflowRun struct {
	Name         string    `json:"name"`
	Conclusion   string    `json:"conclusion"`
	HeadBranch   string    `json:"headBranch"`
	HeadSHA      string    `json:"headSha"`
	URL          string    `json:"url"`
	DisplayTitle string    `json:"displayTitle"`
	Event        string    `json:"event"`
	CreatedAt    time.Time `json:"createdAt"`
	Jobs         []Job     `json:"jobs"`
}

// Job represents a single job in the workflow
type Job struct {
	Name        string `json:"name"`
	Conclusion  string `json:"conclusion"`
	Status      string `json:"status"`
	StartedAt   string `json:"startedAt"`
	CompletedAt string `json:"completedAt"`
	URL         string `json:"url"`
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
