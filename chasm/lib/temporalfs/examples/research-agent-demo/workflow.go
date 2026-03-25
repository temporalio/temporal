package main

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// WorkflowParams is the input to the research agent workflow.
type WorkflowParams struct {
	TopicName   string  `json:"topic_name"`
	TopicSlug   string  `json:"topic_slug"`
	PartitionID uint64  `json:"partition_id"`
	FailureRate float64 `json:"failure_rate"`
	Seed        int64   `json:"seed"`
}

// StepResult is the output of each activity.
type StepResult struct {
	FilesCreated int   `json:"files_created"`
	BytesWritten int64 `json:"bytes_written"`
	Retries      int   `json:"retries"`
}

// WorkflowResult aggregates results across all activities.
type WorkflowResult struct {
	TopicSlug     string `json:"topic_slug"`
	FilesCreated  int    `json:"files_created"`
	BytesWritten  int64  `json:"bytes_written"`
	SnapshotCount int    `json:"snapshot_count"`
	Retries       int    `json:"retries"`
}

// ResearchWorkflow chains 5 activities to research a topic, each producing
// files and an MVCC snapshot in the workflow's isolated TemporalFS partition.
func ResearchWorkflow(ctx workflow.Context, params WorkflowParams) (WorkflowResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumAttempts:    5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var a *Activities
	var result WorkflowResult
	result.TopicSlug = params.TopicSlug

	steps := []struct {
		fn   func(ctx workflow.Context) workflow.Future
		name string
	}{
		{func(ctx workflow.Context) workflow.Future { return workflow.ExecuteActivity(ctx, a.WebResearch, params) }, "WebResearch"},
		{func(ctx workflow.Context) workflow.Future { return workflow.ExecuteActivity(ctx, a.Summarize, params) }, "Summarize"},
		{func(ctx workflow.Context) workflow.Future { return workflow.ExecuteActivity(ctx, a.FactCheck, params) }, "FactCheck"},
		{func(ctx workflow.Context) workflow.Future { return workflow.ExecuteActivity(ctx, a.FinalReport, params) }, "FinalReport"},
		{func(ctx workflow.Context) workflow.Future { return workflow.ExecuteActivity(ctx, a.PeerReview, params) }, "PeerReview"},
	}

	for _, step := range steps {
		var sr StepResult
		if err := step.fn(ctx).Get(ctx, &sr); err != nil {
			return result, err
		}
		result.FilesCreated += sr.FilesCreated
		result.BytesWritten += sr.BytesWritten
		result.Retries += sr.Retries
		result.SnapshotCount++
	}

	return result, nil
}
