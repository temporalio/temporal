package mixedbrain

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/headers"
)

const (
	retryTimeout = 30 * time.Second
	omesModule   = "github.com/temporalio/omes"
	omesBranch   = "main"
	omesRepo     = "https://" + omesModule + ".git"
	temporalRepo = "https://github.com/temporalio/temporal.git"
)

func sourceRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// resolveReleaseVersion returns the highest version for the previous minor.
// Stable releases are preferred over pre-releases per semver ordering.
// Pre-release tags (e.g. v1.30.1-184.3) serve as a fallback when no stable
// release exists yet. Returns the zero value if no matching tag is found.
func resolveReleaseVersion(serverVersion string, tags []string) semver.Version {
	current := semver.MustParse(serverVersion)
	targetMajor := current.Major
	targetMinor := current.Minor - 1

	var best semver.Version
	for _, tag := range tags {
		v, err := semver.ParseTolerant(tag)
		if err != nil {
			continue
		}
		if v.Major == targetMajor && v.Minor == targetMinor && v.GT(best) {
			best = v
		}
	}
	return best
}

// fetchPreviousMinorTag asks the temporal git remote for tags and resolves
// the latest patch of the previous minor relative to headers.ServerVersion.
func fetchPreviousMinorTag(t *testing.T) string {
	t.Helper()
	t.Log("Resolving release tags...")
	var version semver.Version
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		out, err := exec.CommandContext(t.Context(), "git", "ls-remote", "--tags", "--refs", temporalRepo).CombinedOutput()
		require.NoError(collect, err, "git ls-remote failed:\n%s", out)

		var tags []string
		for line := range strings.SplitSeq(string(out), "\n") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				tags = append(tags, strings.TrimPrefix(parts[1], "refs/tags/"))
			}
		}

		version = resolveReleaseVersion(headers.ServerVersion, tags)
		require.NotEqual(collect, semver.Version{}, version, "no tags found for previous minor")
	}, retryTimeout, 2*time.Second, "fetch release tags")
	return "v" + version.String()
}

// downloadAndBuildOmes clones omes main into workDir/omes and builds its
// CLI into outputPath. We clone instead of going through the module cache so
// the build resolves omes's transitive deps independently of this module's
// go.sum (omes uses replace directives that block `go install`).
func downloadAndBuildOmes(t *testing.T, workDir, outputPath string) {
	t.Helper()

	repoDir := filepath.Join(workDir, "omes")
	t.Logf("Cloning %s branch %s...", omesRepo, omesBranch)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_ = os.RemoveAll(repoDir)
		out, err := exec.CommandContext(t.Context(),
			"git", "clone",
			"--filter=blob:none",
			"--branch", omesBranch,
			"--single-branch", omesRepo,
			repoDir,
		).CombinedOutput()
		require.NoError(collect, err, "git clone failed:\n%s", out)
	}, retryTimeout, 2*time.Second, "git clone omes")

	t.Logf("Building omes into %s...", outputPath)
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", outputPath, "./cmd/omes")
	cmd.Dir = repoDir
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=auto") // Omes main may require a newer toolchain
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build omes failed:\n%s", out)
}
