// check-dependencies validates that key Go module dependencies (go.temporal.io/api
// and go.temporal.io/sdk) meet version policies for the PR's base branch:
//
//   - release/* and cloud/* branches: dependencies must be tagged semver releases.
//   - main: tagged releases are accepted; pseudo-versions must reference a commit
//     on the dependency's default branch.
//   - Other branches: no policy enforced.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

const defaultGoModPath = "go.mod"

type moduleSpec struct {
	modulePath    string
	repoURL       string
	defaultBranch string
}

var knownModules = []moduleSpec{
	{
		modulePath:    "go.temporal.io/api",
		repoURL:       "https://github.com/temporalio/api-go.git",
		defaultBranch: "master",
	},
	{
		modulePath:    "go.temporal.io/sdk",
		repoURL:       "https://github.com/temporalio/sdk-go.git",
		defaultBranch: "master",
	},
}

func main() {
	baseBranch := flag.String("base-branch", "", "PR base branch (e.g. main, release/v1.31)")
	goModPath := flag.String("go-mod", defaultGoModPath, "Path to go.mod")
	flag.Parse()

	branch := strings.TrimSpace(*baseBranch)
	if branch == "" {
		fmt.Fprintln(os.Stderr, "Error: base branch is required; pass --base-branch")
		os.Exit(1)
	}

	modPath := strings.TrimSpace(*goModPath)
	goModData, err := os.ReadFile(modPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read %s: %v\n", modPath, err)
		os.Exit(1)
	}

	modFile, err := modfile.Parse(modPath, goModData, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to parse %s: %v\n", modPath, err)
		os.Exit(1)
	}

	var validateErr error
	switch {
	case strings.HasPrefix(branch, "release/") || strings.HasPrefix(branch, "cloud/"):
		validateErr = validateReleaseBranch(modFile)
	case branch == "main":
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		validateErr = validateMainBranch(ctx, modFile)
	default:
		fmt.Printf("No dependency policy for base branch %q; skipping validation\n", branch)
	}

	if validateErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", validateErr)
		os.Exit(1)
	}
}

func validateReleaseBranch(modFile *modfile.File) error {
	var failures []string
	for _, mod := range knownModules {
		modVersion, ok := findRequiredModuleVersion(modFile, mod.modulePath)
		if !ok {
			failures = append(failures, fmt.Sprintf("%s: dependency not found in go.mod", mod.modulePath))
			continue
		}

		if !semver.IsValid(modVersion.Version) || module.IsPseudoVersion(modVersion.Version) {
			failures = append(failures, fmt.Sprintf("%s: version %q must be a tagged semver release", mod.modulePath, modVersion.Version))
			continue
		}

		fmt.Printf("  - %s@%s (ok)\n", mod.modulePath, modVersion.Version)
	}

	if len(failures) > 0 {
		return fmt.Errorf("release dependency validation failed:\n  - %s", strings.Join(failures, "\n  - "))
	}

	fmt.Println("All required dependencies use tagged releases")
	return nil
}

func validateMainBranch(
	ctx context.Context,
	modFile *modfile.File,
) error {
	var failures []string
	for _, mod := range knownModules {
		if err := validateMainModule(ctx, modFile, mod); err != nil {
			failures = append(failures, err.Error())
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("main branch dependency validation failed:\n  - %s", strings.Join(failures, "\n  - "))
	}

	fmt.Println("All required dependencies are valid for main branch")
	return nil
}

func validateMainModule(
	ctx context.Context,
	modFile *modfile.File,
	mod moduleSpec,
) error {
	modVersion, ok := findRequiredModuleVersion(modFile, mod.modulePath)
	if !ok {
		return fmt.Errorf("%s: dependency not found in go.mod", mod.modulePath)
	}
	version := modVersion.Version

	fmt.Printf("Found %s version: %s\n", mod.modulePath, version)

	if !module.IsPseudoVersion(version) {
		if !semver.IsValid(version) {
			return fmt.Errorf("%s@%s: not a valid semver tag", mod.modulePath, version)
		}
		fmt.Printf("  - %s@%s is a tagged release (ok)\n", mod.modulePath, version)
		return nil
	}

	shortHash, err := module.PseudoVersionRev(version)
	if err != nil {
		return fmt.Errorf("%s@%s: failed to parse pseudo-version revision: %v", mod.modulePath, version, err)
	}

	onDefault, err := resolveModuleOriginForSpec(ctx, mod, shortHash)
	if err != nil {
		return fmt.Errorf("%s@%s: failed to resolve module origin: %v", mod.modulePath, version, err)
	}

	if !onDefault {
		return fmt.Errorf("%s@%s: commit %s is not on the default branch (%s) of %s",
			mod.modulePath, version, shortHash, mod.defaultBranch, mod.repoURL)
	}

	fmt.Printf("  - %s@%s is on %s (ok)\n", mod.modulePath, version, mod.defaultBranch)
	return nil
}

func findRequiredModuleVersion(modFile *modfile.File, modulePath string) (module.Version, bool) {
	for _, req := range modFile.Require {
		if req.Mod.Path == modulePath {
			return req.Mod, true
		}
	}
	return module.Version{}, false
}

// resolveModuleOriginForSpec reports whether shortHash is reachable from the
// default branch of mod's repository.
//
// It runs two git commands:
//
//  1. git clone --bare --filter=blob:none --single-branch --branch <defaultBranch> <repoURL> <tmpDir>
//     --bare: clone without a working tree; only the git object store and refs
//     are written to tmpDir.
//     --filter=blob:none: partial clone — fetch commits and trees but skip file
//     blobs entirely, since we only need commit graph reachability.
//     --single-branch: fetch only the ref for --branch, not all remote branches.
//     --branch <defaultBranch>: which branch to fetch.
//
//  2. git -C <tmpDir> merge-base --is-ancestor <shortHash> refs/heads/<defaultBranch>
//     -C <tmpDir>: run in the cloned bare repo.
//     merge-base --is-ancestor: tests reachability rather than finding a common
//     ancestor — exits 0 if <shortHash> is an ancestor of (or equal to) the
//     branch tip, exits 1 if it is not.
//     <shortHash>: the abbreviated commit hash extracted from the pseudo-version.
//     refs/heads/<defaultBranch>: the branch tip to check ancestry against.
//     Any other exit code indicates an error (e.g. the object does not exist).
func resolveModuleOriginForSpec(ctx context.Context, mod moduleSpec, shortHash string) (bool, error) {
	tmpRepo, err := os.MkdirTemp("", "check-dependencies-*")
	if err != nil {
		return false, fmt.Errorf("failed to create temp repo dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpRepo) }()

	cmd := exec.CommandContext(ctx, "git", "clone", "--bare", "--filter=blob:none", "--single-branch", "--branch", mod.defaultBranch, mod.repoURL, tmpRepo)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("git clone failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	out, err = exec.CommandContext(ctx, "git", "-C", tmpRepo, "merge-base", "--is-ancestor", shortHash, "refs/heads/"+mod.defaultBranch).CombinedOutput()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	fmt.Printf("git merge-base --is-ancestor output: %s\n", strings.TrimSpace(string(out)))
	return false, fmt.Errorf("git merge-base --is-ancestor failed: %w", err)
}
