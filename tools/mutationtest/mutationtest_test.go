package mutationtest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfigAcceptsUnlimitedOrPositiveRunTimeout(t *testing.T) {
	t.Parallel()

	baseArgs := []string{
		"-output-root", t.TempDir(),
		"-include-files", "value.go",
		"-test-files", "value_test.go",
	}
	for _, value := range []string{"0", "30s", "20m"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			cfg, _, ok := parseConfig(append(slices.Clone(baseArgs), "-run-timeout", value))
			require.True(t, ok)
			expected, err := time.ParseDuration(value)
			require.NoError(t, err)
			require.Equal(t, expected, cfg.runTimeout)
		})
	}
}

func TestParseConfigRejectsNegativeRunTimeout(t *testing.T) {
	t.Parallel()

	_, exitCode, ok := parseConfig([]string{
		"-output-root", t.TempDir(),
		"-include-files", "value.go",
		"-test-files", "value_test.go",
		"-run-timeout", "-1s",
	})
	require.False(t, ok)
	require.Equal(t, exitMutationSkipped, exitCode)
}

func TestMainRunsDeterministicMutationSuite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	runGitCommand(t, repoDir, "init", "--quiet")
	runGitCommand(t, repoDir, "add", ".")
	runGitCommand(t, repoDir, "-c", "user.name=Mutation Test", "-c", "user.email=mutation@example.com", "commit", "--quiet", "-m", "fixture")

	first := runMainFixture(ctx, t, repoDir)
	second := runMainFixture(ctx, t, repoDir)
	require.Equal(t, first, second)
	require.Equal(t, 4, strings.Count(first["shard-01.log"], "diff --git"))
	require.Equal(t, 3, strings.Count(first["shard-02.log"], "diff --git"))
	require.Equal(t, 2, strings.Count(first["survivors.diff"], "diff --git"))
	for _, name := range []string{"summary.txt", "uncovered.txt"} {
		want, err := os.ReadFile(filepath.Join("testdata", "want", name))
		require.NoError(t, err)
		require.Equal(t, string(want), first[name], "unexpected %s", name)
	}
}

func TestMainUsesRequestedShardsForSingleSourceFile(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	runGitCommand(t, repoDir, "init", "--quiet")
	runGitCommand(t, repoDir, "add", ".")
	runGitCommand(t, repoDir, "-c", "user.name=Mutation Test", "-c", "user.email=mutation@example.com", "commit", "--quiet", "-m", "fixture")

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMainFixtureHelper$")
	cmd.Dir = repoDir
	cmd.Env = append(os.Environ(),
		"MUTATION_TEST_MAIN_HELPER=true",
		"MUTATION_TEST_INCLUDE_FILES=value.go",
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	for _, name := range []string{"shard-01.log", "shard-02.log"} {
		contents, err := os.ReadFile(filepath.Join(repoDir, "output", name))
		require.NoError(t, err)
		require.Contains(t, string(contents), "diff --git", "%s did not execute any mutants", name)
	}
}

func TestMainHonorsWholeRunTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	runGitCommand(t, repoDir, "init", "--quiet")
	runGitCommand(t, repoDir, "add", ".")
	runGitCommand(t, repoDir, "-c", "user.name=Mutation Test", "-c", "user.email=mutation@example.com", "commit", "--quiet", "-m", "fixture")

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMainFixtureHelper$")
	cmd.Dir = repoDir
	cmd.Env = append(os.Environ(),
		"MUTATION_TEST_MAIN_HELPER=true",
		"MUTATION_TEST_RUN_TIMEOUT=1ns",
		"MUTATION_TEST_EXPECTED_EXIT=2",
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	_, err = os.Stat(filepath.Join(repoDir, "output", "worktree-01"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMainFixtureHelper(t *testing.T) {
	if os.Getenv("MUTATION_TEST_MAIN_HELPER") == "" {
		return
	}
	runTimeout := "25s"
	if configured := os.Getenv("MUTATION_TEST_RUN_TIMEOUT"); configured != "" {
		runTimeout = configured
	}
	includeFiles := "."
	if configured := os.Getenv("MUTATION_TEST_INCLUDE_FILES"); configured != "" {
		includeFiles = configured
	}
	os.Args = []string{
		"mutationtest",
		"-output-root", "output",
		"-include-files", includeFiles,
		"-test-files", "value_test.go",
		"-test-tags", "test_dep",
		"-timeout", "5s",
		"-run-timeout", runTimeout,
		"-shard-level", "2",
	}
	expectedExit := exitMutationSurvived
	if configured := os.Getenv("MUTATION_TEST_EXPECTED_EXIT"); configured != "" {
		var err error
		expectedExit, err = strconv.Atoi(configured)
		require.NoError(t, err)
	}
	require.Equal(t, expectedExit, Main())
}

func runMainFixture(ctx context.Context, t *testing.T, repoDir string) map[string]string {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMainFixtureHelper$")
	cmd.Dir = repoDir
	cmd.Env = append(os.Environ(), "MUTATION_TEST_MAIN_HELPER=true")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	_, err = os.Stat(filepath.Join(repoDir, "output", "worktree-01"))
	require.ErrorIs(t, err, os.ErrNotExist)

	artifacts := make(map[string]string)
	for _, name := range []string{
		"summary.txt",
		"uncovered.txt",
		"shard-01.log",
		"shard-02.log",
		"survivors-01.diff",
		"survivors-02.diff",
		"survivors.diff",
	} {
		contents, err := os.ReadFile(filepath.Join(repoDir, "output", name))
		require.NoError(t, err)
		artifacts[name] = string(contents)
	}
	return artifacts
}

func runGitCommand(t *testing.T, dir string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	return strings.TrimSpace(string(output))
}
