package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	sdkclient "go.temporal.io/sdk/client"
)

// WorkflowEvent describes a state change in a running workflow.
type WorkflowEvent struct {
	TopicSlug string
	StepIndex int    // 0-4
	StepName  string // "WebResearch", etc.
	State     string // "started", "completed", "retrying", "failed"
	Attempt   int
	Timestamp time.Time
}

// RunConfig holds configuration for the scale runner.
type RunConfig struct {
	Workflows   int
	Concurrency int
	FailureRate float64
	Seed        int64
	TaskQueue   string
}

// RunStats tracks aggregate statistics across all workflows.
type RunStats struct {
	Started      atomic.Int64
	Completed    atomic.Int64
	Failed       atomic.Int64
	FilesCreated atomic.Int64
	BytesWritten atomic.Int64
	Snapshots    atomic.Int64
	Retries      atomic.Int64
}

// Runner starts and monitors N workflows via the Temporal SDK.
type Runner struct {
	client sdkclient.Client
	store  *DemoStore
	config RunConfig
	stats  RunStats

	EventCh chan WorkflowEvent
}

// NewRunner creates a runner that will start workflows against the given Temporal client.
func NewRunner(client sdkclient.Client, store *DemoStore, config RunConfig) *Runner {
	return &Runner{
		client:  client,
		store:   store,
		config:  config,
		EventCh: make(chan WorkflowEvent, config.Workflows*5),
	}
}

// Run starts all workflows and waits for completion. It respects context cancellation.
func (r *Runner) Run(ctx context.Context) error {
	sem := make(chan struct{}, r.config.Concurrency)
	var wg sync.WaitGroup

	seed := r.config.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	for i := range r.config.Workflows {
		if ctx.Err() != nil {
			break
		}

		topic := TopicForIndex(i)
		partitionID := uint64(i + 1) // must be >0

		// Register in manifest for report/browse.
		if err := r.store.RegisterWorkflow(partitionID, topic); err != nil {
			return fmt.Errorf("register workflow %s: %w", topic.Slug, err)
		}

		params := WorkflowParams{
			TopicName:   topic.Name,
			TopicSlug:   topic.Slug,
			PartitionID: partitionID,
			FailureRate: r.config.FailureRate,
			Seed:        rng.Int63(),
		}

		wg.Add(1)
		sem <- struct{}{} // acquire semaphore
		r.stats.Started.Add(1)

		go func() {
			defer wg.Done()
			defer func() { <-sem }() // release semaphore

			r.runOne(ctx, params)
		}()
	}

	wg.Wait()
	close(r.EventCh)
	return nil
}

func (r *Runner) runOne(ctx context.Context, params WorkflowParams) {
	workflowID := "research-" + params.TopicSlug

	r.EventCh <- WorkflowEvent{
		TopicSlug: params.TopicSlug,
		State:     "started",
		Timestamp: time.Now(),
	}

	run, err := r.client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: r.config.TaskQueue,
	}, ResearchWorkflow, params)
	if err != nil {
		r.stats.Failed.Add(1)
		r.EventCh <- WorkflowEvent{
			TopicSlug: params.TopicSlug,
			State:     "failed",
			Timestamp: time.Now(),
		}
		return
	}

	var result WorkflowResult
	if err := run.Get(ctx, &result); err != nil {
		r.stats.Failed.Add(1)
		_ = r.store.UpdateWorkflowResult(params.TopicSlug, result, true)
		r.EventCh <- WorkflowEvent{
			TopicSlug: params.TopicSlug,
			State:     "failed",
			Timestamp: time.Now(),
		}
		return
	}

	r.stats.Completed.Add(1)
	r.stats.FilesCreated.Add(int64(result.FilesCreated))
	r.stats.BytesWritten.Add(result.BytesWritten)
	r.stats.Snapshots.Add(int64(result.SnapshotCount))
	r.stats.Retries.Add(int64(result.Retries))
	_ = r.store.UpdateWorkflowResult(params.TopicSlug, result, false)

	r.EventCh <- WorkflowEvent{
		TopicSlug: params.TopicSlug,
		StepIndex: 4,
		StepName:  "PeerReview",
		State:     "completed",
		Timestamp: time.Now(),
	}
}
