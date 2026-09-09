package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDerivePseudoRelease(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		wantRev     string
		wantTag     string
		wantBase    string
		errContains string
	}{
		{
			name:     "patch bump",
			version:  "v1.63.6-0.20260825170506-bd1da98aab15",
			wantRev:  "bd1da98aab15",
			wantTag:  "v1.63.6",
			wantBase: "v1.63.5",
		},
		{
			// A patch of zero cannot have come from incrementing a base, and
			// x/mod rejects it rather than rolling the minor over.
			name:        "patch zero has no base to decrement",
			version:     "v1.64.0-0.20240101000000-abcdef012345",
			errContains: "negative patch number",
		},
		{
			name:        "no reachable tag",
			version:     "v1.0.0-20240101000000-abcdef012345",
			errContains: "no base version",
		},
		{
			name:        "prerelease base",
			version:     "v1.63.6-rc1.0.20240101000000-abcdef012345",
			errContains: "is a prerelease",
		},
		{
			name:        "not a pseudo-version",
			version:     "v1.63.5",
			errContains: "cannot read revision",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := derivePseudoRelease(tc.version)
			if tc.errContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantRev, got.rev)
			require.Equal(t, tc.wantTag, got.tag)
			require.Equal(t, tc.wantBase, got.baseTag)
		})
	}
}

// stubResolveCommit replaces the GitHub lookups so planAPIRelease can be
// exercised end to end without network access.
func stubResolveCommit(t *testing.T, apiGoSHA, apiRef string) {
	t.Helper()
	orig := resolveCommit
	t.Cleanup(func() { resolveCommit = orig })
	resolveCommit = func(context.Context, string) (string, string, error) {
		return apiGoSHA, apiRef, nil
	}
}

func TestPlanAPIRelease(t *testing.T) {
	const branch = "master"
	const cloudBranch = "cloud/v1.32.0-163"
	repo := initLocalRepo(t, branch)
	setupLocalKnownModules(t, repo, branch)
	stubResolveCommit(t, "full-api-go-sha", "corresponding-api-sha")

	// initLocalRepo tags v1.2.3 and v1.2.4, so v1.2.5 is free to create and
	// v1.2.4 is a usable base for it.
	pseudo := func(tag, hash string) string {
		return fmt.Sprintf("%s-0.20240101000000-%s", tag, hash[:12])
	}

	tests := []struct {
		name        string
		version     string
		wantAction  string
		wantTag     string
		wantBase    string
		errContains string
	}{
		{
			name:       "tagged release is a no-op",
			version:    "v1.40.0",
			wantAction: actionNoop,
		},
		{
			name:       "pseudo-version on a tagged commit is a no-op",
			version:    pseudo("v1.2.5", repo.lightweightTagHash),
			wantAction: actionNoop,
		},
		{
			name:       "pseudo-version on an untagged commit plans a release",
			version:    pseudo("v1.2.5", repo.untaggedHash),
			wantAction: actionRelease,
			wantTag:    "v1.2.5",
			wantBase:   "v1.2.4",
		},
		{
			// The commit carries only a prerelease and a non-semver tag, so it
			// does not count as released and still needs one.
			name:       "prerelease-only tag does not count as released",
			version:    pseudo("v1.2.5", repo.unreleasedTagHash),
			wantAction: actionRelease,
			wantTag:    "v1.2.5",
			wantBase:   "v1.2.4",
		},
		{
			name:        "base tag missing",
			version:     pseudo("v1.9.9", repo.untaggedHash),
			errContains: "base tag v1.9.8 does not exist",
		},
		{
			name:        "target tag already taken",
			version:     pseudo("v1.2.4", repo.untaggedHash),
			errContains: "tag v1.2.4 already exists",
		},
		{
			// A prerelease tag is a tag but not a release, so it neither
			// satisfies the invariant nor implies the next version.
			name:        "prerelease pinned directly is raised, not a no-op",
			version:     "v1.32.1-cherry-pick-for-cli",
			errContains: "prerelease versions are not releases",
		},
		{
			name:        "no reachable base version",
			version:     "v1.0.0-20240101000000-abcdef012345",
			errContains: "no base version",
		},
		{
			name:        "prerelease base",
			version:     "v1.2.5-rc1.0.20240101000000-abcdef012345",
			errContains: "is a prerelease",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := parseGoMod(t, makeGoMod(map[string]string{
				apiModulePath:        tc.version,
				"go.temporal.io/sdk": "v1.31.0",
			}))

			plan, err := planAPIRelease(context.Background(), cloudBranch, f)
			require.Equal(t, tc.version, plan.version, "version is reported even on failure")

			if tc.errContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantAction, plan.action)
			require.Equal(t, tc.wantTag, plan.tag)
			require.Equal(t, tc.wantBase, plan.baseTag)

			if tc.wantAction == actionRelease {
				require.Equal(t, "full-api-go-sha", plan.apiGoSHA)
				require.Equal(t, "corresponding-api-sha", plan.apiRef)
			}
		})
	}
}

func TestPlanAPIReleaseModuleMissing(t *testing.T) {
	f := parseGoMod(t, "module test\n\ngo 1.21\n")
	_, err := planAPIRelease(context.Background(), "cloud/v1.32.0-163", f)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not required by go.mod")
}

// TestPlanAPIReleaseNonCloudBranch guards the one thing keeping this from
// tagging arbitrary commits: main is allowed to pin a pseudo-version on an
// untagged commit, so planning a release from it would tag whatever main
// happens to point at. The go.mod here would otherwise plan a release.
func TestPlanAPIReleaseNonCloudBranch(t *testing.T) {
	const branch = "master"
	repo := initLocalRepo(t, branch)
	setupLocalKnownModules(t, repo, branch)
	stubResolveCommit(t, "full-api-go-sha", "corresponding-api-sha")

	version := fmt.Sprintf("v1.2.5-0.20240101000000-%s", repo.untaggedHash[:12])
	f := parseGoMod(t, makeGoMod(map[string]string{
		apiModulePath:        version,
		"go.temporal.io/sdk": "v1.31.0",
	}))

	for _, b := range []string{"main", "release/v1.32.x", "my-feature", ""} {
		t.Run(b, func(t *testing.T) {
			_, err := planAPIRelease(context.Background(), b, f)
			require.Error(t, err)
			require.Contains(t, err.Error(), "cloud/* branches only")
		})
	}

	t.Run("cloud branch is allowed", func(t *testing.T) {
		plan, err := planAPIRelease(context.Background(), "cloud/v1.32.0-163", f)
		require.NoError(t, err)
		require.Equal(t, actionRelease, plan.action)
	})
}

func TestAppendGitHubOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	t.Setenv("GITHUB_OUTPUT", path)

	require.NoError(t, appendGitHubOutput("action", "release"))
	// A value spanning lines would break the key=value format and fail the
	// step, so whitespace is collapsed.
	require.NoError(t, appendGitHubOutput("reason", "first line\nsecond line"))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "action=release\nreason=first line second line\n", string(data))
}

func TestAppendGitHubOutputNotUnderActions(t *testing.T) {
	t.Setenv("GITHUB_OUTPUT", "")
	require.NoError(t, appendGitHubOutput("action", "noop"))
}

func TestGitHubSHA(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		switch {
		case strings.HasSuffix(r.URL.Path, "/missing"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Not Found"}`))
		case strings.HasSuffix(r.URL.Path, "/no-sha"):
			_, _ = w.Write([]byte(`{"message":"ok"}`))
		default:
			_, _ = w.Write([]byte(`{"sha":"abc123"}`))
		}
	}))
	defer srv.Close()

	orig := githubAPIBase
	t.Cleanup(func() { githubAPIBase = orig })
	githubAPIBase = srv.URL + "/"
	t.Setenv("GITHUB_TOKEN", "s3cret")

	t.Run("returns sha and sends the token", func(t *testing.T) {
		sha, err := githubSHA(context.Background(), "repos/x/y/commits/deadbeef")
		require.NoError(t, err)
		require.Equal(t, "abc123", sha)
		require.Equal(t, "Bearer s3cret", gotAuth)
	})

	t.Run("non-200 is an error naming the status", func(t *testing.T) {
		_, err := githubSHA(context.Background(), "repos/x/y/commits/missing")
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")
	})

	t.Run("response without a sha is an error", func(t *testing.T) {
		_, err := githubSHA(context.Background(), "repos/x/y/commits/no-sha")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no sha")
	})
}
