// check-dependencies validates that key Go module dependencies (go.temporal.io/api
// and go.temporal.io/sdk) meet version policies for the PR's base branch:
//
//   - release/* branches: dependencies must be tagged semver releases.
//   - cloud/* branches: as release/*, except that go.temporal.io/api may also be
//     a pseudo-version whose commit carries a release tag, since that still
//     corresponds to a tagged release even though go.mod does not name it.
//   - main: tagged releases are accepted; pseudo-versions must reference a commit
//     on the dependency's default branch.
//   - Other branches: no policy enforced.
//
// Prereleases never count as releases on release/* or cloud/*, whether named
// directly in go.mod or reached through a pseudo-version.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"slices"
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

// apiModulePath is the only module whose cloud/* policy accepts a
// pseudo-version on a tagged commit, and the only one an api-go release can be
// planned for. There is no equivalent automation for the SDK, so it keeps the
// strict rule.
const apiModulePath = "go.temporal.io/api"

var knownModules = []moduleSpec{
	{
		modulePath:    apiModulePath,
		repoURL:       "https://github.com/temporalio/api-go.git",
		defaultBranch: "main",
	},
	{
		modulePath:    "go.temporal.io/sdk",
		repoURL:       "https://github.com/temporalio/sdk-go.git",
		defaultBranch: "main",
	},
}

const (
	modeCheck          = "check"
	modePlanAPIRelease = "plan-api-release"
)

const (
	releaseBranchPrefix = "release/"
	cloudBranchPrefix   = "cloud/"
	mainBranch          = "main"
)

func main() {
	mode := flag.String("mode", modeCheck,
		`Either "check", to validate go.mod against the base branch's policy, or `+
			`"plan-api-release", to decide whether a cloud branch needs an api-go release`)
	baseBranch := flag.String("base-branch", "", "Branch the policy applies to (e.g. main, release/v1.31, cloud/v1.32.0-163)")
	goModPath := flag.String("go-mod", defaultGoModPath, "Path to go.mod")
	flag.Parse()

	// Checked before reading go.mod so that a missing flag is reported as
	// such rather than as whatever go.mod problem happens to come first.
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var runErr error
	switch *mode {
	case modeCheck:
		runErr = runCheck(ctx, branch, modFile)
	case modePlanAPIRelease:
		runErr = runPlanAPIRelease(ctx, branch, modFile)
	default:
		runErr = fmt.Errorf("unknown mode %q; want %q or %q", *mode, modeCheck, modePlanAPIRelease)
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", runErr)
		os.Exit(1)
	}
}

func runCheck(ctx context.Context, branch string, modFile *modfile.File) error {
	switch {
	case strings.HasPrefix(branch, releaseBranchPrefix):
		return validateReleaseBranch(modFile)
	case strings.HasPrefix(branch, cloudBranchPrefix):
		return validateCloudBranch(ctx, modFile)
	case branch == mainBranch:
		return validateMainBranch(ctx, modFile)
	default:
		fmt.Printf("No dependency policy for base branch %q; skipping validation\n", branch)
		return nil
	}
}

func validateReleaseBranch(modFile *modfile.File) error {
	var failures []string
	for _, mod := range knownModules {
		if err := validateTaggedModule(modFile, mod); err != nil {
			failures = append(failures, err.Error())
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("release dependency validation failed:\n  - %s", strings.Join(failures, "\n  - "))
	}

	fmt.Println("All required dependencies use tagged releases")
	return nil
}

// validateTaggedModule requires that mod resolve to a tagged semver release
// named directly in go.mod. A prerelease is not a release, so it does not
// satisfy this however it is spelled.
func validateTaggedModule(modFile *modfile.File, mod moduleSpec) error {
	modVersion, ok := findRequiredModuleVersion(modFile, mod.modulePath)
	if !ok {
		return fmt.Errorf("%s: dependency not found in go.mod", mod.modulePath)
	}

	version := modVersion.Version
	if !semver.IsValid(version) || module.IsPseudoVersion(version) || semver.Prerelease(version) != "" {
		return fmt.Errorf("%s: version %q must be a tagged semver release", mod.modulePath, version)
	}

	fmt.Printf("  - %s@%s (ok)\n", mod.modulePath, version)
	return nil
}

// validateCloudBranch enforces the cloud/* policy. It matches release/* except
// for apiModulePath, which may also be a pseudo-version whose commit carries a
// release tag: the requirement is that the version in use have a tag, not that
// go.mod name it, and RE automation creates that tag.
func validateCloudBranch(ctx context.Context, modFile *modfile.File) error {
	var failures []string
	for _, mod := range knownModules {
		var err error
		if mod.modulePath == apiModulePath {
			err = validateCloudModule(ctx, modFile, mod)
		} else {
			err = validateTaggedModule(modFile, mod)
		}
		if err != nil {
			failures = append(failures, err.Error())
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("cloud branch dependency validation failed:\n  - %s", strings.Join(failures, "\n  - "))
	}

	fmt.Println("All required dependencies correspond to tagged releases")
	return nil
}

func validateCloudModule(
	ctx context.Context,
	modFile *modfile.File,
	mod moduleSpec,
) error {
	modVersion, ok := findRequiredModuleVersion(modFile, mod.modulePath)
	if !ok {
		return fmt.Errorf("%s: dependency not found in go.mod", mod.modulePath)
	}
	version := modVersion.Version

	if !semver.IsValid(version) {
		return fmt.Errorf("%s: version %q is not valid semver", mod.modulePath, version)
	}

	if !module.IsPseudoVersion(version) {
		// A prerelease is not a release, so it does not satisfy the
		// requirement even though it is a tag. releaseTagsAt excludes them for
		// the same reason, and the two paths have to agree.
		if semver.Prerelease(version) != "" {
			return fmt.Errorf("%s: version %q is a prerelease, not a release", mod.modulePath, version)
		}
		fmt.Printf("  - %s@%s is a tagged release (ok)\n", mod.modulePath, version)
		return nil
	}

	shortHash, err := module.PseudoVersionRev(version)
	if err != nil {
		return fmt.Errorf("%s@%s: failed to parse pseudo-version revision: %v", mod.modulePath, version, err)
	}

	tags, err := releaseTagsForCommit(ctx, mod, shortHash)
	if err != nil {
		return fmt.Errorf("%s@%s: failed to list tags for commit %s: %v", mod.modulePath, version, shortHash, err)
	}

	if len(tags) == 0 {
		return fmt.Errorf("%s@%s: commit %s has no release tag in %s; a cloud release requires a tagged version",
			mod.modulePath, version, shortHash, mod.repoURL)
	}

	fmt.Printf("  - %s@%s pins commit %s, tagged %s (ok)\n",
		mod.modulePath, version, shortHash, strings.Join(tags, ", "))
	return nil
}

// remoteTags maps every tag in mod's repository to the commit it points at,
// using a single `git ls-remote --tags` rather than a clone.
//
// An annotated tag appears twice: refs/tags/<name> resolves to the tag object,
// and refs/tags/<name>^{} to the commit it points at. Only the latter is a
// commit, so it wins.
func remoteTags(ctx context.Context, mod moduleSpec) (map[string]string, error) {
	out, err := exec.CommandContext(ctx, "git", "ls-remote", "--tags", mod.repoURL).Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-remote --tags failed: %w", err)
	}

	commits := make(map[string]string) // tag name -> commit sha
	for line := range strings.SplitSeq(string(out), "\n") {
		sha, ref, found := strings.Cut(strings.TrimSpace(line), "\t")
		if !found {
			continue
		}
		name := strings.TrimPrefix(ref, "refs/tags/")
		if name == ref {
			continue
		}
		if deref, ok := strings.CutSuffix(name, "^{}"); ok {
			commits[deref] = sha
			continue
		}
		if _, seen := commits[name]; !seen {
			commits[name] = sha
		}
	}
	return commits, nil
}

// releaseTagsAt returns the semver release tags pointing at the commit named by
// shortHash, which may be abbreviated. Prereleases are excluded: they do not
// satisfy the requirement that the version correspond to a release.
func releaseTagsAt(tags map[string]string, shortHash string) []string {
	var found []string
	for name, sha := range tags {
		if !strings.HasPrefix(sha, shortHash) {
			continue
		}
		if !semver.IsValid(name) || semver.Prerelease(name) != "" {
			continue
		}
		found = append(found, name)
	}
	slices.Sort(found)
	return found
}

func releaseTagsForCommit(ctx context.Context, mod moduleSpec, shortHash string) ([]string, error) {
	tags, err := remoteTags(ctx, mod)
	if err != nil {
		return nil, err
	}
	return releaseTagsAt(tags, shortHash), nil
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
