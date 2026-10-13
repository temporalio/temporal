package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

func parseGoMod(t *testing.T, content string) *modfile.File {
	t.Helper()
	f, err := modfile.Parse("go.mod", []byte(content), nil)
	require.NoError(t, err)
	return f
}

func makeGoMod(deps map[string]string) string {
	var s strings.Builder
	s.WriteString("module test\n\ngo 1.21\n\nrequire (\n")
	for mod, ver := range deps {
		s.WriteString(fmt.Sprintf("\t%s %s\n", mod, ver))
	}
	return s.String() + ")\n"
}

func TestFindRequiredModuleVersion(t *testing.T) {
	f := parseGoMod(t, makeGoMod(map[string]string{
		"go.temporal.io/api": "v1.2.3",
		"go.temporal.io/sdk": "v1.4.0",
	}))

	t.Run("found", func(t *testing.T) {
		v, ok := findRequiredModuleVersion(f, "go.temporal.io/api")
		require.True(t, ok)
		require.Equal(t, "v1.2.3", v.Version)
	})

	t.Run("not found", func(t *testing.T) {
		_, ok := findRequiredModuleVersion(f, "go.temporal.io/missing")
		require.False(t, ok)
	})
}

func TestValidateReleaseBranch(t *testing.T) {
	tests := []struct {
		name           string
		deps           map[string]string
		wantErr        bool
		errContains    []string
		errNotContains []string
	}{
		{
			name: "tagged semver passes",
			deps: map[string]string{
				"go.temporal.io/api": "v1.40.0",
				"go.temporal.io/sdk": "v1.31.0",
			},
		},
		{
			name: "pseudo-version fails",
			deps: map[string]string{
				"go.temporal.io/api": "v1.40.1-0.20240101000000-abcdef012345",
				"go.temporal.io/sdk": "v1.31.0",
			},
			wantErr:     true,
			errContains: []string{"go.temporal.io/api", "tagged semver release"},
		},
		{
			name: "prerelease is not a release",
			deps: map[string]string{
				"go.temporal.io/api": "v1.32.1-cherry-pick-for-cli",
				"go.temporal.io/sdk": "v1.31.0",
			},
			wantErr:     true,
			errContains: []string{"go.temporal.io/api", "must be a tagged semver release"},
		},
		{
			name:        "both modules missing fails",
			deps:        nil, // empty go.mod
			wantErr:     true,
			errContains: []string{"go.temporal.io/api", "go.temporal.io/sdk"},
		},
		{
			name: "one pseudo one tagged fails with one error",
			deps: map[string]string{
				"go.temporal.io/api": "v1.40.0",
				"go.temporal.io/sdk": "v1.31.1-0.20240101000000-abcdef012345",
			},
			wantErr:        true,
			errContains:    []string{"go.temporal.io/sdk"},
			errNotContains: []string{"go.temporal.io/api"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var content string
			if tc.deps == nil {
				content = "module test\n\ngo 1.21\n"
			} else {
				content = makeGoMod(tc.deps)
			}
			f := parseGoMod(t, content)
			err := validateReleaseBranch(f)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, s := range tc.errContains {
				require.Contains(t, err.Error(), s)
			}
			for _, s := range tc.errNotContains {
				require.NotContains(t, err.Error(), s)
			}
		})
	}
}

// Tags created by initLocalRepo, one per shape releaseTagsForCommit has to
// distinguish.
const (
	testLightweightTag = "v1.2.3"
	testAnnotatedTag   = "v1.2.4"
	testPrereleaseTag  = "v1.2.5-rc1"
	testNonSemverTag   = "cherry-pick-for-cli"
)

// localRepo is a bare git repo with commits for testing.
type localRepo struct {
	// Path to the bare repo.
	path string
	// Hash of the commit on the default branch.
	onBranchHash string
	// Hash of a commit that exists in the repo but is NOT on the default branch.
	offBranchHash string
	// Hash of the commit carrying the lightweight tag testLightweightTag.
	lightweightTagHash string
	// Hash of the commit carrying the annotated tag testAnnotatedTag.
	annotatedTagHash string
	// Hash of the commit carrying only testPrereleaseTag and testNonSemverTag,
	// neither of which counts as a release.
	unreleasedTagHash string
	// Hash of an on-branch commit carrying no tags at all.
	untaggedHash string
}

// initLocalRepo creates a bare git repo with several commits on the default
// branch and one commit on a side branch. Every commit exists as an object in
// the bare repo, but the side-branch commit is not reachable from
// refs/heads/<branch>.
//
// Tags are laid out so that releaseTagsForCommit can be exercised against each
// shape it must handle: a lightweight release tag, an annotated release tag
// (which ls-remote reports via an extra ^{} ref), a prerelease tag, a
// non-semver tag, and a commit with no tags.
func initLocalRepo(t *testing.T, branch string) localRepo {
	t.Helper()

	work := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = work
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "git %v: %s", args, out)
		return string(out)
	}
	commit := func(name string) string {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(work, name), []byte(name), 0o600))
		run("add", ".")
		run("commit", "-m", name)
		return strings.TrimSpace(run("rev-parse", "HEAD"))
	}

	run("init", "-b", branch)
	run("config", "user.email", "test@test.com")
	run("config", "user.name", "Test")

	onHash := commit("file.txt")

	lightweightHash := commit("lightweight.txt")
	run("tag", testLightweightTag)

	annotatedHash := commit("annotated.txt")
	run("tag", "-a", testAnnotatedTag, "-m", testAnnotatedTag)

	unreleasedHash := commit("unreleased.txt")
	run("tag", testPrereleaseTag)
	run("tag", testNonSemverTag)

	untaggedHash := commit("untagged.txt")

	// Create a side branch with its own commit.
	run("checkout", "-b", "side")
	offHash := commit("side.txt")
	run("checkout", branch)

	// Clone to a bare repo without --single-branch so that git fetches all
	// branches, making the side-branch commit reachable as an object. This
	// mirrors the scenario where a pseudo-version references a commit that
	// exists in the repo but is not on the default branch.
	bare := t.TempDir()
	cmd := exec.Command("git", "clone", "--bare", work, bare)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git clone --bare: %s", out)

	return localRepo{
		path:               bare,
		onBranchHash:       onHash,
		offBranchHash:      offHash,
		lightweightTagHash: lightweightHash,
		annotatedTagHash:   annotatedHash,
		unreleasedTagHash:  unreleasedHash,
		untaggedHash:       untaggedHash,
	}
}

func TestResolveModuleOriginForSpec(t *testing.T) {
	const branch = "master"
	repo := initLocalRepo(t, branch)

	spec := moduleSpec{
		modulePath:    "go.temporal.io/api",
		repoURL:       repo.path,
		defaultBranch: branch,
	}

	tests := []struct {
		name      string
		hash      string
		onDefault bool
	}{
		{"commit on default branch", repo.onBranchHash[:12], true},
		{"commit not on default branch", repo.offBranchHash[:12], false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			onDefault, err := resolveModuleOriginForSpec(context.Background(), spec, tc.hash)
			require.NoError(t, err)
			require.Equal(t, tc.onDefault, onDefault)
		})
	}
}

// setupLocalKnownModules replaces knownModules with specs pointing at the local
// bare repo, restoring the original on cleanup.
func setupLocalKnownModules(t *testing.T, repo localRepo, branch string) {
	t.Helper()
	orig := knownModules
	t.Cleanup(func() { knownModules = orig })
	knownModules = []moduleSpec{
		{modulePath: "go.temporal.io/api", repoURL: repo.path, defaultBranch: branch},
		{modulePath: "go.temporal.io/sdk", repoURL: repo.path, defaultBranch: branch},
	}
}

func TestValidateMainBranch(t *testing.T) {
	t.Run("tagged release passes", func(t *testing.T) {
		f := parseGoMod(t, makeGoMod(map[string]string{
			"go.temporal.io/api": "v1.40.0",
			"go.temporal.io/sdk": "v1.31.0",
		}))
		require.NoError(t, validateMainBranch(context.Background(), f))
	})

	t.Run("missing module fails", func(t *testing.T) {
		f := parseGoMod(t, "module test\n\ngo 1.21\n")
		err := validateMainBranch(context.Background(), f)
		require.Error(t, err)
		require.Contains(t, err.Error(), "go.temporal.io/api")
	})

	const branch = "master"
	repo := initLocalRepo(t, branch)
	setupLocalKnownModules(t, repo, branch)

	t.Run("pseudo-version on default branch passes", func(t *testing.T) {
		ver := fmt.Sprintf("v0.0.0-20240101000000-%s", repo.onBranchHash[:12])
		f := parseGoMod(t, makeGoMod(map[string]string{
			"go.temporal.io/api": ver,
			"go.temporal.io/sdk": ver,
		}))
		require.NoError(t, validateMainBranch(context.Background(), f))
	})

	t.Run("pseudo-version not on default branch fails", func(t *testing.T) {
		ver := fmt.Sprintf("v0.0.0-20240101000000-%s", repo.offBranchHash[:12])
		f := parseGoMod(t, makeGoMod(map[string]string{
			"go.temporal.io/api": ver,
			"go.temporal.io/sdk": ver,
		}))
		err := validateMainBranch(context.Background(), f)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not on the default branch")
	})
}

func TestReleaseTagsForCommit(t *testing.T) {
	repo := initLocalRepo(t, "master")
	spec := moduleSpec{
		modulePath:    "go.temporal.io/api",
		repoURL:       repo.path,
		defaultBranch: "master",
	}

	tests := []struct {
		name string
		hash string
		want []string
	}{
		{"lightweight release tag", repo.lightweightTagHash, []string{testLightweightTag}},
		// An annotated tag's refs/tags/<name> points at the tag object, not the
		// commit; only the ^{} ref resolves to the commit. Finding it proves the
		// dereferenced line is the one being used.
		{"annotated release tag", repo.annotatedTagHash, []string{testAnnotatedTag}},
		{"prerelease and non-semver tags are not releases", repo.unreleasedTagHash, nil},
		{"commit with no tags", repo.untaggedHash, nil},
		{"commit off the default branch is still searched", repo.offBranchHash, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Abbreviate the hash the way a pseudo-version does.
			tags, err := releaseTagsForCommit(context.Background(), spec, tc.hash[:12])
			require.NoError(t, err)
			require.Equal(t, tc.want, tags)
		})
	}
}

func TestValidateCloudBranch(t *testing.T) {
	const branch = "master"
	repo := initLocalRepo(t, branch)
	setupLocalKnownModules(t, repo, branch)

	pseudo := func(hash string) string {
		return fmt.Sprintf("v1.2.4-0.20240101000000-%s", hash[:12])
	}

	tests := []struct {
		name        string
		deps        map[string]string
		wantErr     bool
		errContains []string
	}{
		{
			name: "tagged release passes",
			deps: map[string]string{
				"go.temporal.io/api": "v1.40.0",
				"go.temporal.io/sdk": "v1.31.0",
			},
		},
		{
			name: "api pseudo-version on a tagged commit passes",
			deps: map[string]string{
				"go.temporal.io/api": pseudo(repo.lightweightTagHash),
				"go.temporal.io/sdk": "v1.31.0",
			},
		},
		{
			name: "api pseudo-version on an annotated tag passes",
			deps: map[string]string{
				"go.temporal.io/api": pseudo(repo.annotatedTagHash),
				"go.temporal.io/sdk": "v1.31.0",
			},
		},
		{
			name: "api prerelease pinned directly fails",
			deps: map[string]string{
				"go.temporal.io/api": "v1.32.1-cherry-pick-for-cli",
				"go.temporal.io/sdk": "v1.31.0",
			},
			wantErr:     true,
			errContains: []string{"go.temporal.io/api", "is a prerelease, not a release"},
		},
		{
			name: "sdk prerelease pinned directly fails",
			deps: map[string]string{
				"go.temporal.io/api": "v1.40.0",
				"go.temporal.io/sdk": "v1.31.0-rc1",
			},
			wantErr:     true,
			errContains: []string{"go.temporal.io/sdk", "must be a tagged semver release"},
		},
		{
			// Only the API module is relaxed: there is no automation that
			// creates SDK releases, so it keeps the strict rule.
			name: "sdk pseudo-version on a tagged commit still fails",
			deps: map[string]string{
				"go.temporal.io/api": "v1.40.0",
				"go.temporal.io/sdk": pseudo(repo.lightweightTagHash),
			},
			wantErr:     true,
			errContains: []string{"go.temporal.io/sdk", "must be a tagged semver release"},
		},
		{
			name: "pseudo-version on an untagged commit fails",
			deps: map[string]string{
				"go.temporal.io/api": pseudo(repo.untaggedHash),
				"go.temporal.io/sdk": "v1.31.0",
			},
			wantErr:     true,
			errContains: []string{"go.temporal.io/api", "has no release tag"},
		},
		{
			name: "pseudo-version on a commit tagged only as a prerelease fails",
			deps: map[string]string{
				"go.temporal.io/api": pseudo(repo.unreleasedTagHash),
				"go.temporal.io/sdk": "v1.31.0",
			},
			wantErr:     true,
			errContains: []string{"go.temporal.io/api", "has no release tag"},
		},
		{
			name:        "missing modules fail",
			deps:        nil,
			wantErr:     true,
			errContains: []string{"go.temporal.io/api", "go.temporal.io/sdk", "not found in go.mod"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := parseGoMod(t, makeGoMod(tc.deps))
			err := validateCloudBranch(context.Background(), f)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, s := range tc.errContains {
				require.Contains(t, err.Error(), s)
			}
		})
	}
}

// TestValidateCloudModuleInvalidSemver reaches the semver guard in
// validateCloudModule, which modfile.Parse would otherwise reject first, by
// building the file without the parser. The guard mirrors the one in
// validateReleaseBranch and exists for versions the parser lets through.
func TestValidateCloudModuleInvalidSemver(t *testing.T) {
	mod := moduleSpec{modulePath: "go.temporal.io/api", repoURL: "unused", defaultBranch: "master"}
	f := &modfile.File{
		Require: []*modfile.Require{
			{Mod: module.Version{Path: mod.modulePath, Version: "not-a-version"}},
		},
	}

	err := validateCloudModule(context.Background(), f, mod)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not valid semver")
}

// TestValidateReleaseBranchRejectsTaggedCommitPseudoVersion guards the split
// between the release/* and cloud/* policies: relaxing cloud/* must not relax
// release/*, where go.mod itself still has to name the tag.
func TestValidateReleaseBranchRejectsTaggedCommitPseudoVersion(t *testing.T) {
	repo := initLocalRepo(t, "master")

	f := parseGoMod(t, makeGoMod(map[string]string{
		"go.temporal.io/api": fmt.Sprintf("v1.2.4-0.20240101000000-%s", repo.lightweightTagHash[:12]),
		"go.temporal.io/sdk": "v1.31.0",
	}))

	err := validateReleaseBranch(f)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a tagged semver release")
}
