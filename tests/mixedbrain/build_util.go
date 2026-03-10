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
	temporalRepo = "https://github.com/temporalio/temporal.git"
	omesRepo     = "https://github.com/temporalio/omes"
	omesCommit   = "8e4c1f54f3b0fb5e39d131f859c56fb2236395b1"
)

func sourceRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

func cloneRepo(t *testing.T, url, destDir, ref string) {
	t.Helper()
	t.Logf("Cloning %s at %s...", url, ref)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_ = os.RemoveAll(destDir)
		out, err := exec.CommandContext(t.Context(), "git", "clone", "--filter=blob:none", url, destDir).CombinedOutput()
		require.NoError(collect, err, "git clone failed:\n%s", out)
	}, retryTimeout, 2*time.Second, "git clone "+filepath.Base(url))

	out, err := exec.CommandContext(t.Context(), "git", "-C", destDir, "checkout", ref).CombinedOutput()
	require.NoError(t, err, "git checkout %s failed:\n%s", ref, out)
}

func buildServer(t *testing.T, srcDir, outputPath string) {
	t.Helper()
	t.Logf("Building server binary from %s...", srcDir)
	cmd := exec.CommandContext(t.Context(), "go",
		"build",
		"-tags", "disable_grpc_modules",
		"-o", outputPath,
		"./cmd/server",
	)
	cmd.Dir = srcDir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build server binary failed:\n%s", out)
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

// downloadAndBuildReleaseServer finds the highest patch of the previous minor
// version, clones the repo at that tag, and builds the server binary. For
// example, if ServerVersion is 1.31.x, it will look for the highest 1.30.x
// tag. Falls back to pre-release tags (cloud versions) if no stable release
// exists yet.
func downloadAndBuildReleaseServer(t *testing.T, outputPath string) string {
	t.Helper()

	t.Log("Resolving release tags...")
	var version semver.Version
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		out, err := exec.CommandContext(t.Context(), "git", "ls-remote", "--tags", "--refs", temporalRepo).CombinedOutput()
		require.NoError(collect, err, "git ls-remote failed:\n%s", out)

		var tags []string
		for _, line := range strings.Split(string(out), "\n") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				tags = append(tags, strings.TrimPrefix(parts[1], "refs/tags/"))
			}
		}

		version = resolveReleaseVersion(headers.ServerVersion, tags)
		require.NotEqual(collect, semver.Version{}, version, "no tags found for previous minor")
	}, retryTimeout, 2*time.Second, "fetch release tags")

	tag := "v" + version.String()
	repoDir := filepath.Join(filepath.Dir(outputPath), "temporal-release")
	cloneRepo(t, temporalRepo, repoDir, tag)

	t.Log("Building release server binary...")
	buildServer(t, repoDir, outputPath)
	return tag
}

func downloadAndBuildOmes(t *testing.T, workDir string) {
	t.Helper()

	repoDir := filepath.Join(workDir, "omes")
	cloneRepo(t, omesRepo, repoDir, omesCommit)

	t.Log("Building Omes...")
	omesBinary := filepath.Join(workDir, "omes-bin")
	buildCmd := exec.CommandContext(t.Context(), "go",
		"build",
		"-o", omesBinary,
		"./cmd",
	)
	buildCmd.Dir = repoDir
	out, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "build Omes failed:\n%s", out)
}
