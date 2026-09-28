package github

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunShortSHA(t *testing.T) {
	tests := []struct {
		name     string
		sha      string
		expected string
	}{
		{
			name:     "normal SHA",
			sha:      "abc1234567890defghijk",
			expected: "abc1234",
		},
		{
			name:     "short SHA",
			sha:      "abc123",
			expected: "abc123",
		},
		{
			name:     "exactly 7 chars",
			sha:      "abc1234",
			expected: "abc1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := Run{HeadSHA: tt.sha}
			require.Equal(t, tt.expected, run.ShortSHA())
		})
	}
}

func TestRunDownloadRetriesBeforeReplacingDestination(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "run-1")
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	existing := filepath.Join(runDir, "existing")
	require.NoError(t, os.WriteFile(existing, nil, 0o600))

	attempts := 0
	downloader := func(_ context.Context, _ string, opts RunDownloadOptions) error {
		attempts++
		require.NotEqual(t, runDir, opts.Dir)
		entries, err := os.ReadDir(filepath.Dir(runDir))
		require.NoError(t, err)
		require.Len(t, entries, 2)
		_, err = os.Stat(existing)
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(opts.Dir, "partial"))
		require.ErrorIs(t, err, os.ErrNotExist)
		if attempts < 3 {
			require.NoError(t, os.WriteFile(filepath.Join(opts.Dir, "partial"), nil, 0o600))
			return errors.New("transient download failure")
		}
		return os.WriteFile(filepath.Join(opts.Dir, "complete"), nil, 0o600)
	}

	err := runDownloadWithRetry(
		context.Background(),
		"1",
		RunDownloadOptions{Pattern: "junit-*", Dir: runDir},
		time.Nanosecond,
		downloader,
	)
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	_, err = os.Stat(existing)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(runDir, "complete"))
	require.NoError(t, err)
}

func TestRunDownloadFailsAfterThreeAttempts(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "run-1")
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	existing := filepath.Join(runDir, "existing")
	require.NoError(t, os.WriteFile(existing, nil, 0o600))

	attempts := 0
	downloader := func(_ context.Context, _ string, opts RunDownloadOptions) error {
		attempts++
		require.NoError(t, os.WriteFile(filepath.Join(opts.Dir, "partial"), nil, 0o600))
		return errors.New("download failure")
	}

	err := runDownloadWithRetry(
		context.Background(),
		"1",
		RunDownloadOptions{Pattern: "junit-*", Dir: runDir},
		time.Nanosecond,
		downloader,
	)
	require.ErrorContains(t, err, "after 3 attempts")
	require.Equal(t, 3, attempts)
	_, err = os.Stat(existing)
	require.NoError(t, err)
	entries, readErr := os.ReadDir(filepath.Dir(runDir))
	require.NoError(t, readErr)
	require.Len(t, entries, 1)
}

func TestRunDownloadAcceptsDestinationWithTrailingSeparator(t *testing.T) {
	baseDir := t.TempDir()
	runDir := filepath.Join(baseDir, "run-1")
	downloader := func(_ context.Context, _ string, opts RunDownloadOptions) error {
		return os.WriteFile(filepath.Join(opts.Dir, "complete"), nil, 0o600)
	}

	err := runDownloadWithRetry(
		context.Background(),
		"1",
		RunDownloadOptions{Pattern: "junit-*", Dir: runDir + string(filepath.Separator)},
		time.Nanosecond,
		downloader,
	)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(runDir, "complete"))
	require.NoError(t, err)
}

func TestRunDownloadRejectsWorkingDirectoryAndAncestors(t *testing.T) {
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	workingDirLink := filepath.Join(t.TempDir(), "working-dir")
	require.NoError(t, os.Symlink(workingDir, workingDirLink))

	for _, destination := range []string{".", "..", workingDir, workingDirLink} {
		t.Run(destination, func(t *testing.T) {
			called := false
			downloader := func(context.Context, string, RunDownloadOptions) error {
				called = true
				return errors.New("unexpected download")
			}

			err := runDownloadWithRetry(
				context.Background(),
				"1",
				RunDownloadOptions{Dir: destination},
				time.Nanosecond,
				downloader,
			)
			require.ErrorContains(t, err, "working directory")
			require.False(t, called)
		})
	}
}

func TestReplaceDownloadDirectoryRestoresDestinationWhenInstallFails(t *testing.T) {
	baseDir := t.TempDir()
	destination := filepath.Join(baseDir, "run-1")
	require.NoError(t, os.MkdirAll(destination, 0o755))
	existing := filepath.Join(destination, "existing")
	require.NoError(t, os.WriteFile(existing, nil, 0o600))

	err := replaceDownloadDirectory(filepath.Join(baseDir, "missing"), destination)
	require.Error(t, err)
	_, err = os.Stat(existing)
	require.NoError(t, err)
	entries, readErr := os.ReadDir(baseDir)
	require.NoError(t, readErr)
	require.Len(t, entries, 1)
}
