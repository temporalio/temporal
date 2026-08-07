package flakereport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"go.temporal.io/server/tools/common/github"
)

const maxFailureSamplesPerArtifact = 3

// ArtifactJob represents a job to download and process an artifact
type ArtifactJob struct {
	Repo         string
	RunID        int64
	RunCreatedAt time.Time
	Artifact     github.Artifact
	TempDir      string
	RunNumber    int
	TotalRuns    int
	ArtifactNum  int
}

// ArtifactResult represents the result of processing an artifact
type ArtifactResult struct {
	Failures []TestFailure
	Summary  *testRunSummary
	Error    error
}

type downloadedArtifact struct {
	Job     ArtifactJob
	ZipPath string
	Error   error
}

type artifactDownloadFunc func(context.Context, ArtifactJob) downloadedArtifact
type artifactParseFunc func(context.Context, downloadedArtifact) ArtifactResult

// processArtifactsParallel downloads and processes artifacts in parallel with a worker pool
// Returns: all failures, summarized test runs, and count of successfully processed artifacts.
func processArtifactsParallel(ctx context.Context, jobs []ArtifactJob, concurrency int) ([]TestFailure, *testRunSummary, int, error) {
	if concurrency < 1 {
		return nil, nil, 0, fmt.Errorf("concurrency must be at least 1")
	}
	if len(jobs) == 0 {
		return nil, newTestRunSummary(), 0, nil
	}

	jobChan := make(chan ArtifactJob, concurrency)
	go func() {
		defer close(jobChan)
		for _, job := range jobs {
			select {
			case jobChan <- job:
			case <-ctx.Done():
				return
			}
		}
	}()
	return processArtifactStream(ctx, jobChan, concurrency)
}

func processArtifactStream(ctx context.Context, jobs <-chan ArtifactJob, downloadConcurrency int) ([]TestFailure, *testRunSummary, int, error) {
	if downloadConcurrency < 1 {
		return nil, nil, 0, fmt.Errorf("concurrency must be at least 1")
	}

	parseConcurrency := min(downloadConcurrency, runtime.GOMAXPROCS(0))
	return processArtifactStreamWithFunctions(
		ctx,
		jobs,
		downloadConcurrency,
		parseConcurrency,
		downloadArtifactJob,
		processDownloadedArtifact,
	)
}

func processArtifactStreamWithFunctions(
	ctx context.Context,
	jobs <-chan ArtifactJob,
	downloadConcurrency int,
	parseConcurrency int,
	download artifactDownloadFunc,
	parse artifactParseFunc,
) ([]TestFailure, *testRunSummary, int, error) {
	if downloadConcurrency < 1 || parseConcurrency < 1 {
		return nil, nil, 0, fmt.Errorf("worker concurrency must be at least 1")
	}
	downloaded := make(chan downloadedArtifact, downloadConcurrency)
	results := make(chan ArtifactResult, parseConcurrency)

	var downloadWG sync.WaitGroup
	for range downloadConcurrency {
		downloadWG.Add(1)
		go downloadWorker(ctx, jobs, downloaded, download, &downloadWG)
	}
	go func() {
		downloadWG.Wait()
		close(downloaded)
	}()

	var parseWG sync.WaitGroup
	for range parseConcurrency {
		parseWG.Add(1)
		go parseWorker(ctx, downloaded, results, parse, &parseWG)
	}
	go func() {
		parseWG.Wait()
		close(results)
	}()

	// Collect results
	var allFailures []TestFailure
	summary := newTestRunSummary()
	processedArtifacts := 0
	errorCount := 0

	for result := range results {
		if result.Error != nil {
			errorCount++
			// Error already logged by worker
			continue
		}
		allFailures = append(allFailures, result.Failures...)
		summary.merge(result.Summary)
		processedArtifacts++
	}

	if errorCount > 0 {
		fmt.Printf("Warning: %d artifacts failed to process\n", errorCount)
	}

	if err := ctx.Err(); err != nil {
		return nil, nil, processedArtifacts, err
	}
	return allFailures, summary, processedArtifacts, nil
}

func downloadWorker(
	ctx context.Context,
	jobs <-chan ArtifactJob,
	downloaded chan<- downloadedArtifact,
	download artifactDownloadFunc,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			result := download(ctx, job)
			select {
			case downloaded <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func downloadArtifactJob(ctx context.Context, job ArtifactJob) downloadedArtifact {
	result := downloadedArtifact{Job: job}
	if err := ctx.Err(); err != nil {
		result.Error = err
		return result
	}

	fmt.Printf("  [artifact %d] Run %d/%d: Downloading %s (ID: %d)...\n",
		job.ArtifactNum, job.RunNumber, job.TotalRuns,
		job.Artifact.Name, job.Artifact.ID)

	zipPath, err := github.DownloadArtifact(ctx, job.Repo, job.Artifact.ID, job.TempDir)
	if err != nil {
		result.Error = fmt.Errorf("failed to download artifact %d: %w", job.Artifact.ID, err)
		fmt.Printf("  Warning: %v\n", result.Error)
		return result
	}
	result.ZipPath = zipPath
	return result
}

func parseWorker(
	ctx context.Context,
	downloaded <-chan downloadedArtifact,
	results chan<- ArtifactResult,
	parse artifactParseFunc,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case artifact, ok := <-downloaded:
			if !ok {
				return
			}
			result := parse(ctx, artifact)
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func processDownloadedArtifact(ctx context.Context, downloaded downloadedArtifact) ArtifactResult {
	result := ArtifactResult{Summary: newTestRunSummary()}
	if downloaded.Error != nil {
		result.Error = downloaded.Error
		return result
	}
	job := downloaded.Job
	defer func() {
		if err := os.Remove(downloaded.ZipPath); err != nil && !os.IsNotExist(err) {
			fmt.Printf("  Warning: Failed to remove artifact %d ZIP: %v\n", job.Artifact.ID, err)
		}
	}()

	// Extract XML files
	extractDir := filepath.Join(job.TempDir, fmt.Sprintf("artifact-%d", job.Artifact.ID))
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		result.Error = fmt.Errorf("failed to create extraction directory for artifact %d: %w", job.Artifact.ID, err)
		return result
	}
	defer func() {
		if err := os.RemoveAll(extractDir); err != nil {
			fmt.Printf("  Warning: Failed to remove artifact %d extraction directory: %v\n", job.Artifact.ID, err)
		}
	}()
	xmlFiles, err := extractArtifactZip(ctx, downloaded.ZipPath, extractDir)
	if err != nil {
		result.Error = fmt.Errorf("failed to extract artifact %d: %w", job.Artifact.ID, err)
		fmt.Printf("  Warning: %v\n", result.Error)
		return result
	}

	fmt.Printf("  [artifact %d] Extracted %d XML files from %s\n",
		job.ArtifactNum, len(xmlFiles), job.Artifact.Name)

	// Parse JUnit XML files
	for _, xmlFile := range xmlFiles {
		if err := ctx.Err(); err != nil {
			result.Error = err
			return result
		}
		suites, err := parseJUnitFile(ctx, xmlFile)
		if err != nil {
			fmt.Printf("  Warning: Failed to parse %s: %v\n", filepath.Base(xmlFile), err)
			continue
		}

		// Extract failures
		failures := extractFailures(suites, job.Artifact.Name, job.RunID, job.RunCreatedAt)
		result.Failures = append(result.Failures, failures...)

		// Extract all test runs for failure rate calculation
		_, jobID, matrixName := parseArtifactName(job.Artifact.Name)
		testRuns := extractAllTestRuns(suites, job.RunID, jobID, matrixName)
		result.Summary.add(testRuns)
		if err := ctx.Err(); err != nil {
			result.Error = err
			return result
		}
	}

	fmt.Printf("  [artifact %d] Found %d failures from %d test runs in %s\n",
		job.ArtifactNum, len(result.Failures), result.Summary.totalRuns, job.Artifact.Name)

	for i := 0; i < min(len(result.Failures), maxFailureSamplesPerArtifact); i++ {
		fmt.Printf("    Sample failure %d: %s\n", i+1, result.Failures[i].Name)
	}

	return result
}
