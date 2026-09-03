package mutationtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecuteMutantClassifiesResultAndRestoresSource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	runGitCommand(t, repoDir, "init", "--quiet")
	runGitCommand(t, repoDir, "add", ".")
	runGitCommand(t, repoDir, "-c", "user.name=Mutation Test", "-c", "user.email=mutation@example.com", "commit", "--quiet", "-m", "fixture")
	originalPath := filepath.Join(repoDir, "value.go")
	original, err := os.ReadFile(originalPath)
	require.NoError(t, err)
	record := mutantRecord{
		id:       1,
		operator: "conditional/negated",
		file:     "value.go",
		line:     4,
		column:   9,
		digest:   "digest",
		source: []byte(`package smoke

func value(input int) bool {
	return input <= 0
}

func untested(input int) int {
	return input + 1
}
`),
	}

	outcome := executeMutant(ctx, repoDir, config{testTags: "test_dep", timeout: 5 * time.Second}, record)
	require.Equal(t, mutationKilled, outcome.status)
	require.Contains(t, outcome.diff, "return input <= 0")
	restored, err := os.ReadFile(originalPath)
	require.NoError(t, err)
	require.Equal(t, original, restored, "source was not restored after mutation test")
}

func TestExecuteMutantClassifiesCompileFailureAsSkipped(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	runGitCommand(t, repoDir, "init", "--quiet")
	runGitCommand(t, repoDir, "add", ".")
	runGitCommand(t, repoDir, "-c", "user.name=Mutation Test", "-c", "user.email=mutation@example.com", "commit", "--quiet", "-m", "fixture")
	originalPath := filepath.Join(repoDir, "value.go")
	original, err := os.ReadFile(originalPath)
	require.NoError(t, err)

	outcome := executeMutant(ctx, repoDir, config{testTags: "test_dep", timeout: 5 * time.Second}, mutantRecord{
		id:       1,
		operator: "test/invalid",
		file:     "value.go",
		source:   []byte("package smoke\n\nfunc value(input int) bool { return input > }\n"),
	})
	require.NoError(t, outcome.err)
	require.Equal(t, mutationSkipped, outcome.status)
	restored, err := os.ReadFile(originalPath)
	require.NoError(t, err)
	require.Equal(t, original, restored, "source was not restored after compile failure")
}
