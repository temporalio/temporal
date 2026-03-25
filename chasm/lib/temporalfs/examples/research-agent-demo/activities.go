package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"

	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	"github.com/temporalio/temporal-fs/pkg/store"
	"go.temporal.io/sdk/activity"
)

// Activities holds the shared store and implements the 5 research agent activities.
// Each activity opens an isolated TemporalFS partition, verifies that all files from
// the previous step survived (demonstrating durability), writes new files, and creates
// an MVCC snapshot. On retry, the FS state is intact — no intermediate state is lost.
type Activities struct {
	baseStore store.Store
	stats     *RunStats // shared stats for real-time dashboard updates
}

// openFS opens an existing FS for the workflow's partition.
func (a *Activities) openFS(partitionID uint64) (*tfs.FS, error) {
	s := store.NewPrefixedStore(a.baseStore, partitionID)
	f, err := tfs.Open(s)
	if err != nil {
		return nil, fmt.Errorf("open fs: %w", err)
	}
	return f, nil
}

// onRetry records a retry in shared stats and logs the recovery with prior state info.
func (a *Activities) onRetry(ctx context.Context, priorFiles int, priorSnapshot string) {
	a.stats.Retries.Add(1)
	activity.GetLogger(ctx).Info("Retrying with durable FS state intact",
		"attempt", activity.GetInfo(ctx).Attempt,
		"filesFromPriorStep", priorFiles,
		"lastSnapshot", priorSnapshot,
	)
}

// retries returns the number of retries for the current activity execution.
func retries(ctx context.Context) int {
	info := activity.GetInfo(ctx)
	if info.Attempt > 1 {
		return int(info.Attempt) - 1
	}
	return 0
}

// maybeFail injects a random failure based on the configured failure rate.
// It incorporates the attempt number so retries can succeed after earlier failures.
func maybeFail(ctx context.Context, seed int64, rate float64, msg string) error {
	attempt := int64(activity.GetInfo(ctx).Attempt)
	r := rand.New(rand.NewSource(seed + attempt*1000))
	if rate > 0 && r.Float64() < rate {
		return errors.New(msg)
	}
	return nil
}

// countFiles counts files in a directory (non-recursive).
func countFiles(f *tfs.FS, dir string) int {
	entries, err := f.ReadDir(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if e.Type != tfs.InodeTypeDir {
			count++
		}
	}
	return count
}

// WebResearch simulates gathering research sources: creates workspace dirs
// and writes 3-5 source files. Failure rate: 20% * multiplier.
func (a *Activities) WebResearch(ctx context.Context, params WorkflowParams) (StepResult, error) {
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// On retry: verify FS opened successfully (partition is durable).
	if activity.GetInfo(ctx).Attempt > 1 {
		a.onRetry(ctx, 0, "(none — first step)")
	}

	// Inject failure AFTER opening FS — proves partition survives failures.
	if err := maybeFail(ctx, params.Seed+1, 0.20*params.FailureRate, "simulated web API timeout"); err != nil {
		return StepResult{}, err
	}

	// Create workspace directories (idempotent — ignore ErrExist).
	for _, dir := range []string{
		"/research",
		"/research/" + params.TopicSlug,
		"/research/" + params.TopicSlug + "/sources",
	} {
		if mkErr := f.Mkdir(dir, 0o755); mkErr != nil && !errors.Is(mkErr, tfs.ErrExist) {
			return StepResult{}, fmt.Errorf("mkdir %s: %w", dir, mkErr)
		}
	}

	// Generate and write source files.
	sources := generateSources(params.TopicName, params.Seed)
	var result StepResult
	for _, src := range sources {
		path := "/research/" + params.TopicSlug + "/sources/" + src.Filename
		if err := f.WriteFile(path, src.Content, 0o644); err != nil {
			return StepResult{}, fmt.Errorf("write %s: %w", path, err)
		}
		result.FilesCreated++
		result.BytesWritten += int64(len(src.Content))
	}

	// Snapshot after this step.
	if _, err := f.CreateSnapshot("step-1-research"); err != nil && !errors.Is(err, tfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	result.Retries = retries(ctx)
	return result, nil
}

// Summarize reads all source files and produces a summary. Failure rate: 15%.
func (a *Activities) Summarize(ctx context.Context, params WorkflowParams) (StepResult, error) {
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Read source filenames — verifies step 1's files survived.
	sourcesDir := "/research/" + params.TopicSlug + "/sources"
	entries, err := f.ReadDir(sourcesDir)
	if err != nil {
		return StepResult{}, fmt.Errorf("readdir %s: %w", sourcesDir, err)
	}

	// On retry: step 1's source files are still here — TemporalFS is durable.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.onRetry(ctx, len(entries), "step-1-research")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+2, 0.15*params.FailureRate, "simulated LLM rate limit exceeded"); err != nil {
		return StepResult{}, err
	}

	sourceNames := make([]string, len(entries))
	for i, e := range entries {
		sourceNames[i] = e.Name
	}

	// Generate and write summary.
	content := generateSummary(params.TopicName, sourceNames, params.Seed)
	path := "/research/" + params.TopicSlug + "/summary.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write summary: %w", err)
	}

	if _, err := f.CreateSnapshot("step-2-summary"); err != nil && !errors.Is(err, tfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}

// FactCheck reads the summary and produces a fact-check report. Failure rate: 10%.
func (a *Activities) FactCheck(ctx context.Context, params WorkflowParams) (StepResult, error) {
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Verify step 2's summary file survived.
	topicDir := "/research/" + params.TopicSlug
	priorFiles := countFiles(f, topicDir)

	// On retry: summary + sources from prior steps are intact.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.onRetry(ctx, priorFiles, "step-2-summary")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+3, 0.10*params.FailureRate, "simulated fact-checking service unavailable"); err != nil {
		return StepResult{}, err
	}

	content := generateFactCheck(params.TopicName, params.Seed)
	path := topicDir + "/fact-check.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write fact-check: %w", err)
	}

	if _, err := f.CreateSnapshot("step-3-factcheck"); err != nil && !errors.Is(err, tfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}

// FinalReport reads all artifacts and produces a final report. Failure rate: 10%.
func (a *Activities) FinalReport(ctx context.Context, params WorkflowParams) (StepResult, error) {
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Verify prior steps' files survived.
	topicDir := "/research/" + params.TopicSlug
	priorFiles := countFiles(f, topicDir)

	// On retry: sources + summary + fact-check from prior steps are intact.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.onRetry(ctx, priorFiles, "step-3-factcheck")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+4, 0.10*params.FailureRate, "simulated context window exceeded"); err != nil {
		return StepResult{}, err
	}

	content := generateFinalReport(params.TopicName, params.Seed)
	path := topicDir + "/report.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write report: %w", err)
	}

	if _, err := f.CreateSnapshot("step-4-report"); err != nil && !errors.Is(err, tfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}

// PeerReview reads the report and produces a peer review. Failure rate: 5%.
func (a *Activities) PeerReview(ctx context.Context, params WorkflowParams) (StepResult, error) {
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Verify prior steps' files survived.
	topicDir := "/research/" + params.TopicSlug
	priorFiles := countFiles(f, topicDir)

	// On retry: all artifacts from prior steps are intact.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.onRetry(ctx, priorFiles, "step-4-report")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+5, 0.05*params.FailureRate, "simulated reviewer model overloaded"); err != nil {
		return StepResult{}, err
	}

	content := generatePeerReview(params.TopicName, params.Seed)
	path := topicDir + "/review.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write review: %w", err)
	}

	if _, err := f.CreateSnapshot("step-5-review"); err != nil && !errors.Is(err, tfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}
