package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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
	Continuous  bool // keep running until cancelled
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
	bufSize := config.Workflows * 5
	if config.Continuous {
		bufSize = config.Concurrency * 10
	}
	return &Runner{
		client:  client,
		store:   store,
		config:  config,
		EventCh: make(chan WorkflowEvent, bufSize),
	}
}

// Run starts workflows and waits for completion. In continuous mode, it keeps
// starting new workflows until the context is cancelled, then waits for in-flight
// workflows to finish. In fixed mode, it runs exactly config.Workflows workflows.
func (r *Runner) Run(ctx context.Context) error {
	sem := make(chan struct{}, r.config.Concurrency)
	var wg sync.WaitGroup

	seed := r.config.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	limit := r.config.Workflows
	if r.config.Continuous {
		limit = 0 // no limit
	}

	// Pre-create all FS partitions before starting any workflows.
	// This ensures all superblocks and root inodes exist in PebbleDB before
	// any concurrent activity reads/writes, avoiding visibility issues.
	if !r.config.Continuous {
		for i := 0; i < limit; i++ {
			topic := TopicForIndex(i)
			partitionID := uint64(i + 1)
			if err := r.store.CreatePartition(partitionID); err != nil {
				return fmt.Errorf("create partition for %s: %w", topic.Slug, err)
			}
			if err := r.store.RegisterWorkflow(partitionID, topic); err != nil {
				return fmt.Errorf("register workflow %s: %w", topic.Slug, err)
			}
		}
	}

loop:
	for i := 0; limit == 0 || i < limit; i++ {
		if ctx.Err() != nil {
			break
		}

		topic := TopicForIndex(i)
		partitionID := uint64(i + 1) // must be >0

		// In continuous mode, create partitions on the fly.
		if r.config.Continuous {
			if err := r.store.CreatePartition(partitionID); err != nil {
				return fmt.Errorf("create partition for %s: %w", topic.Slug, err)
			}
			if err := r.store.RegisterWorkflow(partitionID, topic); err != nil {
				return fmt.Errorf("register workflow %s: %w", topic.Slug, err)
			}
		}

		params := WorkflowParams{
			TopicName:   topic.Name,
			TopicSlug:   topic.Slug,
			PartitionID: partitionID,
			FailureRate: r.config.FailureRate,
			Seed:        rng.Int63(),
		}

		wg.Add(1)
		r.stats.Started.Add(1)

		// Acquire semaphore — in continuous mode, also check for cancellation.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			r.stats.Started.Add(-1)
			break loop
		}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r.runOne(ctx, params)
		}()
	}

	if r.config.Continuous {
		// Wait for in-flight workflows to finish.
		fmt.Printf("\n  Waiting for %d in-flight workflows to complete...\n",
			r.stats.Started.Load()-r.stats.Completed.Load()-r.stats.Failed.Load())
	}

	wg.Wait()
	close(r.EventCh)
	return nil
}

// emitEvent sends a workflow event without blocking. If the channel is full
// the event is dropped to avoid stalling goroutines that hold the semaphore.
func (r *Runner) emitEvent(ev WorkflowEvent) {
	select {
	case r.EventCh <- ev:
	default:
	}
}

func (r *Runner) runOne(ctx context.Context, params WorkflowParams) {
	workflowID := "research-" + params.TopicSlug

	r.emitEvent(WorkflowEvent{
		TopicSlug: params.TopicSlug,
		State:     "started",
		Timestamp: time.Now(),
	})

	run, err := r.client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: r.config.TaskQueue,
		// Terminate stale workflows from previous runs that share the same ID.
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
	}, ResearchWorkflow, params)
	if err != nil {
		// Context cancellation (Ctrl+C) is not a failure — just stop tracking.
		if ctx.Err() != nil {
			r.stats.Started.Add(-1)
			return
		}
		r.stats.Failed.Add(1)
		r.emitEvent(WorkflowEvent{
			TopicSlug: params.TopicSlug,
			State:     "failed",
			Timestamp: time.Now(),
		})
		return
	}

	var result WorkflowResult
	if err := run.Get(ctx, &result); err != nil {
		// Context cancellation (Ctrl+C) means we stopped waiting, not that
		// the workflow failed. Don't count these as failures.
		if ctx.Err() != nil {
			r.stats.Started.Add(-1)
			return
		}
		r.stats.Failed.Add(1)
		_ = r.store.UpdateWorkflowResult(params.TopicSlug, result, true)
		r.emitEvent(WorkflowEvent{
			TopicSlug: params.TopicSlug,
			State:     "failed",
			Timestamp: time.Now(),
		})
		return
	}

	r.stats.Completed.Add(1)
	r.stats.FilesCreated.Add(int64(result.FilesCreated))
	r.stats.BytesWritten.Add(result.BytesWritten)
	r.stats.Snapshots.Add(int64(result.SnapshotCount))
	r.stats.Retries.Add(int64(result.Retries))
	_ = r.store.UpdateWorkflowResult(params.TopicSlug, result, false)

	r.emitEvent(WorkflowEvent{
		TopicSlug: params.TopicSlug,
		StepIndex: 4,
		StepName:  "PeerReview",
		State:     "completed",
		Timestamp: time.Now(),
	})
}
