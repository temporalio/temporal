package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
)

func parseGoMod(t *testing.T, content string) *modfile.File {
	t.Helper()
	f, err := modfile.Parse("go.mod", []byte(content), nil)
	require.NoError(t, err)
	return f
}

func makeGoMod(deps map[string]string) string {
	s := "module test\n\ngo 1.21\n\nrequire (\n"
	for mod, ver := range deps {
		s += fmt.Sprintf("\t%s %s\n", mod, ver)
	}
	return s + ")\n"
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

// localRepo is a bare git repo with commits for testing.
type localRepo struct {
	// Path to the bare repo.
	path string
	// Hash of the commit on the default branch.
	onBranchHash string
	// Hash of a commit that exists in the repo but is NOT on the default branch.
	offBranchHash string
}

// initLocalRepo creates a bare git repo with one commit on the default branch
// and one commit on a side branch. Both commits exist as objects in the bare
// repo, but only onBranchHash is reachable from refs/heads/<branch>.
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

	run("init", "-b", branch)
	run("config", "user.email", "test@test.com")
	run("config", "user.name", "Test")

	require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("hello"), 0o600))
	run("add", ".")
	run("commit", "-m", "initial")
	onHash := run("rev-parse", "HEAD")

	// Create a side branch with its own commit.
	run("checkout", "-b", "side")
	require.NoError(t, os.WriteFile(filepath.Join(work, "side.txt"), []byte("side"), 0o600))
	run("add", ".")
	run("commit", "-m", "side commit")
	offHash := run("rev-parse", "HEAD")
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
		path:          bare,
		onBranchHash:  onHash[:len(onHash)-1],
		offBranchHash: offHash[:len(offHash)-1],
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
