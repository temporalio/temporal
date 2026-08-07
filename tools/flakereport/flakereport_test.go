package flakereport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/tools/common/github"
)

func TestGenerateCommandRPSFlag(t *testing.T) {
	app := NewCliApp()
	command := app.Commands[0]
	for _, flag := range command.Flags {
		intFlag, ok := flag.(*cli.IntFlag)
		if !ok || intFlag.Name != "rps" {
			continue
		}
		require.Equal(t, github.DefaultAPIRPS, intFlag.Value)
		require.Equal(t, "Maximum GitHub API requests per second", intFlag.Usage)
		return
	}
	t.Fatal("generate command does not define an rps flag")
}

func TestStreamArtifactJobsEmitsCompletedRunImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	runs := []github.Run{
		{DatabaseID: 1},
		{DatabaseID: 2},
	}
	releaseSlowRun := make(chan struct{})
	fetch := func(ctx context.Context, _ string, runID int64) ([]github.Artifact, error) {
		if runID == 1 {
			select {
			case <-releaseSlowRun:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []github.Artifact{{ID: runID, Name: "junit"}}, nil
	}

	jobs, done := streamArtifactJobsWithFetcher(ctx, "temporalio/temporal", runs, t.TempDir(), fetch)
	firstJob := <-jobs
	require.Equal(t, int64(2), firstJob.RunID)
	close(releaseSlowRun)
	for range jobs {
	}
	result := <-done
	require.NoError(t, result.err)
	require.Len(t, result.jobs, 2)
}
