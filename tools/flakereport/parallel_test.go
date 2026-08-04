package flakereport

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/github"
)

func TestProcessArtifactsParallelRejectsInvalidConcurrency(t *testing.T) {
	_, _, _, err := processArtifactsParallel(context.Background(), nil, 0)
	require.ErrorContains(t, err, "concurrency must be at least 1")
}

func TestProcessArtifactsParallelCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _, err := processArtifactsParallel(ctx, []ArtifactJob{{}}, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestArtifactPipelineOverlapsDownloadsAndParsing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	jobs := make(chan ArtifactJob, 2)
	jobs <- ArtifactJob{Artifact: github.Artifact{ID: 1}}
	jobs <- ArtifactJob{Artifact: github.Artifact{ID: 2}}
	close(jobs)

	parseStarted := make(chan struct{})
	secondDownloaded := make(chan struct{})
	releaseParse := make(chan struct{})
	download := func(ctx context.Context, job ArtifactJob) downloadedArtifact {
		if job.Artifact.ID == 2 {
			select {
			case <-parseStarted:
				close(secondDownloaded)
			case <-ctx.Done():
				return downloadedArtifact{Job: job, Error: ctx.Err()}
			}
		}
		return downloadedArtifact{Job: job}
	}
	parse := func(ctx context.Context, artifact downloadedArtifact) ArtifactResult {
		if artifact.Job.Artifact.ID == 1 {
			close(parseStarted)
			select {
			case <-releaseParse:
			case <-ctx.Done():
				return ArtifactResult{Summary: newTestRunSummary(), Error: ctx.Err()}
			}
		}
		summary := newTestRunSummary()
		summary.add([]TestRun{{Name: "TestPipeline", RunID: artifact.Job.Artifact.ID}})
		return ArtifactResult{Summary: summary}
	}

	type pipelineResult struct {
		summary   *testRunSummary
		processed int
		err       error
	}
	done := make(chan pipelineResult, 1)
	go func() {
		_, summary, processed, err := processArtifactStreamWithFunctions(ctx, jobs, 2, 1, download, parse)
		done <- pipelineResult{summary: summary, processed: processed, err: err}
	}()

	select {
	case <-secondDownloaded:
		close(releaseParse)
	case <-ctx.Done():
		t.Fatal("second download did not finish while the first artifact was parsing")
	}

	result := <-done
	require.NoError(t, result.err)
	require.Equal(t, 2, result.processed)
	require.Equal(t, 2, result.summary.totalRuns)
}

func TestProcessDownloadedArtifactParsesAndCleansTemporaryFiles(t *testing.T) {
	tempDir := t.TempDir()
	zipPath := filepath.Join(tempDir, "artifact-42.zip")
	zipFile, err := os.Create(zipPath)
	require.NoError(t, err)
	zipWriter := zip.NewWriter(zipFile)
	entry, err := zipWriter.Create("results.xml")
	require.NoError(t, err)
	_, err = entry.Write([]byte(`<testsuites tests="1" failures="1"><testsuite name="suite" tests="1" failures="1"><testcase name="TestStandalone"><failure message="failed"/></testcase></testsuite></testsuites>`))
	require.NoError(t, err)
	require.NoError(t, zipWriter.Close())
	require.NoError(t, zipFile.Close())

	result := processDownloadedArtifact(context.Background(), downloadedArtifact{
		Job: ArtifactJob{
			RunID:       7,
			Artifact:    github.Artifact{ID: 42, Name: "junit-xml--7--8--1--unit-test"},
			TempDir:     tempDir,
			ArtifactNum: 1,
		},
		ZipPath: zipPath,
	})
	require.NoError(t, result.Error)
	require.Len(t, result.Failures, 1)
	require.Equal(t, 1, result.Summary.totalRuns)
	_, err = os.Stat(zipPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(tempDir, "artifact-42"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
