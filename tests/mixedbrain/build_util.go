package mixedbrain

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	omesRepo     = "https://" + omesModule + ".git"
	temporalRepo = "https://github.com/temporalio/temporal.git"
)

func sourceRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// cloneRepo shallow-clones url into destDir and checks out ref. Retries the
// clone on transient failures; wipes destDir on each attempt so a partial
// clone doesn't poison the next try.
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

// omesRef returns the sha of github.com/temporalio/omes as pinned in go.mod,
// queried via `go list -m` so go.mod stays the single source of truth.
// Pseudo-versions look like "v0.0.0-20260512170720-ab5a6ff22874"; we take the
// trailing 12-char sha. (debug.ReadBuildInfo().Deps is empty for `go test`
// binaries, so we can't read it from build info.)
func omesRef(t *testing.T) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "list", "-m", "-f", "{{.Version}}", omesModule).Output()
	require.NoError(t, err, "go list -m %s", omesModule)
	version := strings.TrimSpace(string(out))
	if i := strings.LastIndex(version, "-"); i >= 0 {
		return version[i+1:]
	}
	return version
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

// cloudReleasePattern matches cloud-release tags of the form
// `vX.Y.Z-A.B` (no `-rc.*` suffix). The two pre-release components are the
// cloud iteration number and patch.
var cloudReleasePattern = regexp.MustCompile(`^v\d+\.\d+\.\d+-\d+\.\d+$`)

// resolveCloudReleaseVersion returns the highest non-rc cloud-release tag.
// Cloud RC tags (e.g. v1.32.0-156.0-rc.20260513120230) are excluded — only
// stable cloud releases like v1.32.0-155.3 are considered. Returns the zero
// value if no matching tag is found.
func resolveCloudReleaseVersion(tags []string) semver.Version {
	var best semver.Version
	for _, tag := range tags {
		if !cloudReleasePattern.MatchString(tag) {
			continue
		}
		v, err := semver.ParseTolerant(tag)
		if err != nil {
			continue
		}
		if v.GT(best) {
			best = v
		}
	}
	return best
}

// fetchPreviousMinorTag asks the temporal git remote for tags and resolves
// the latest patch of the previous minor relative to headers.ServerVersion.
func fetchPreviousMinorTag(t *testing.T) string {
	t.Helper()
	return fetchTag(t, "previous-minor", func(tags []string) semver.Version {
		return resolveReleaseVersion(headers.ServerVersion, tags)
	})
}

// fetchLastCloudReleaseTag asks the temporal git remote for tags and resolves
// the highest non-rc cloud-release tag.
func fetchLastCloudReleaseTag(t *testing.T) string {
	t.Helper()
	return fetchTag(t, "cloud-release", resolveCloudReleaseVersion)
}

func fetchTag(t *testing.T, label string, resolve func([]string) semver.Version) string {
	t.Helper()
	t.Logf("Resolving %s release tags...", label)
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

		version = resolve(tags)
		require.NotEqual(collect, semver.Version{}, version, "no %s tag found", label)
	}, retryTimeout, 2*time.Second, "fetch "+label+" tag")
	return "v" + version.String()
}

// downloadAndBuildOmes clones omes at omesRef into workDir/omes and builds its
// CLI into outputPath. We clone instead of going through the module cache so
// the build resolves omes's transitive deps independently of this module's
// go.sum (omes uses replace directives that block `go install`).
func downloadAndBuildOmes(t *testing.T, workDir, outputPath string) {
	t.Helper()

	repoDir := filepath.Join(workDir, "omes")
	cloneRepo(t, omesRepo, repoDir, omesRef(t))

	t.Logf("Building omes into %s...", outputPath)
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", outputPath, "./cmd")
	cmd.Dir = repoDir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build omes failed:\n%s", out)
}
