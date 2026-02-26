package flakereport

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

// ArtifactJob represents a job to download and process an artifact
type ArtifactJob struct {
	Repo         string
	RunID        int64
	RunCreatedAt time.Time
	Artifact     WorkflowArtifact
	TempDir      string
	RunNumber    int
	TotalRuns    int
	ArtifactNum  int
}

// ArtifactResult represents the result of processing an artifact
type ArtifactResult struct {
	Failures []TestFailure
	AllRuns  []TestRun
	Error    error
}

// processArtifactsParallel downloads and processes artifacts in parallel with a worker pool
// Returns: all failures, all test runs, and count of successfully processed artifacts
func processArtifactsParallel(ctx context.Context, jobs []ArtifactJob, concurrency int) ([]TestFailure, []TestRun, int) {
	if len(jobs) == 0 {
		return nil, nil, 0
	}

	totalArtifacts := len(jobs)

	// Create channels
	jobChan := make(chan ArtifactJob, totalArtifacts)
	resultChan := make(chan ArtifactResult, totalArtifacts)

	// Start worker pool
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go worker(ctx, jobChan, resultChan, totalArtifacts, &wg)
	}

	// Send jobs to workers
	go func() {
		for _, job := range jobs {
			jobChan <- job
		}
		close(jobChan)
	}()

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var allFailures []TestFailure
	var allTestRuns []TestRun
	processedArtifacts := 0
	errorCount := 0

	for result := range resultChan {
		if result.Error != nil {
			errorCount++
			// Error already logged by worker
			continue
		}
		allFailures = append(allFailures, result.Failures...)
		allTestRuns = append(allTestRuns, result.AllRuns...)
		processedArtifacts++
	}

	if errorCount > 0 {
		fmt.Printf("Warning: %d artifacts failed to process\n", errorCount)
	}

	return allFailures, allTestRuns, processedArtifacts
}

// worker processes jobs from the job channel
func worker(ctx context.Context, jobs <-chan ArtifactJob, results chan<- ArtifactResult, totalArtifacts int, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		result := processArtifactJob(ctx, job, totalArtifacts)
		results <- result
	}
}

// processArtifactJob downloads and processes a single artifact
func processArtifactJob(ctx context.Context, job ArtifactJob, totalArtifacts int) ArtifactResult {
	var result ArtifactResult

	fmt.Printf("  [%d/%d] Run %d/%d: Downloading artifact %s (ID: %d)...\n",
		job.ArtifactNum, totalArtifacts, job.RunNumber, job.TotalRuns,
		job.Artifact.Name, job.Artifact.ID)

	// Download artifact
	zipPath, err := downloadArtifact(ctx, job.Repo, job.Artifact.ID, job.TempDir)
	if err != nil {
		result.Error = fmt.Errorf("failed to download artifact %d: %w", job.Artifact.ID, err)
		fmt.Printf("  Warning: %v\n", result.Error)
		return result
	}

	// Extract XML files
	xmlFiles, err := extractArtifactZip(zipPath, job.TempDir)
	if err != nil {
		result.Error = fmt.Errorf("failed to extract artifact %d: %w", job.Artifact.ID, err)
		fmt.Printf("  Warning: %v\n", result.Error)
		return result
	}

	fmt.Printf("  [%d/%d] Extracted %d XML files from %s\n",
		job.ArtifactNum, totalArtifacts, len(xmlFiles), job.Artifact.Name)

	// Parse JUnit XML files
	for _, xmlFile := range xmlFiles {
		suites, err := parseJUnitFile(xmlFile)
		if err != nil {
			fmt.Printf("  Warning: Failed to parse %s: %v\n", filepath.Base(xmlFile), err)
			continue
		}

		// Extract failures
		failures := extractFailures(suites, job.Artifact.Name, job.RunID, job.RunCreatedAt)
		result.Failures = append(result.Failures, failures...)

		// Extract all test runs for failure rate calculation
		testRuns := extractAllTestRuns(suites, job.RunID)
		result.AllRuns = append(result.AllRuns, testRuns...)
	}

	fmt.Printf("  [%d/%d] Found %d failures from %d test runs in %s\n",
		job.ArtifactNum, totalArtifacts, len(result.Failures), len(result.AllRuns), job.Artifact.Name)

	for i := 0; i < len(result.Failures); i++ {
		fmt.Printf("    Sample failure %d: %s\n", i+1, result.Failures[i].Name)
	}

	return result
}
