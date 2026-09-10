package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

// apiGoRepoSlug is where the API module is published from. Its module path is
// go.temporal.io/api, so the commit a pseudo-version names lives here.
const apiGoRepoSlug = "temporalio/api-go"

const (
	actionNoop    = "noop"
	actionRelease = "release"
	actionError   = "error"
)

// releasePlan is the decision plan-api-release mode reports.
type releasePlan struct {
	action   string
	reason   string
	version  string
	tag      string
	baseTag  string
	apiGoSHA string
	apiRef   string
}

// pseudoRelease is the release implied by a pseudo-version.
type pseudoRelease struct {
	rev     string // commit to tag, abbreviated as go.mod records it
	tag     string // tag to create
	baseTag string // tag its release notes start from
}

// derivePseudoRelease reads the release implied by a pseudo-version.
//
// Go builds one by incrementing the patch of the most recent tag reachable from
// the commit, so vX.Y.Z-0.<ts>-<sha> carries both values: the tag to create is
// the vX.Y.Z prefix, and the base tag is what PseudoVersionBase reports.
//
// The other two shapes Go emits have no usable answer. vX.0.0-<ts>-<sha> means
// no tag was reachable, leaving no base tag for release notes, and
// vX.Y.Z-pre.0.<ts>-<sha> is based on a prerelease, where what comes next is a
// judgment call rather than a patch bump.
func derivePseudoRelease(version string) (pseudoRelease, error) {
	rev, err := module.PseudoVersionRev(version)
	if err != nil {
		return pseudoRelease{}, fmt.Errorf("cannot read revision: %w", err)
	}

	base, err := module.PseudoVersionBase(version)
	if err != nil {
		return pseudoRelease{}, fmt.Errorf("cannot read base version: %w", err)
	}
	if base == "" {
		return pseudoRelease{}, errors.New("no base version to increment from")
	}
	if semver.Prerelease(base) != "" {
		return pseudoRelease{}, fmt.Errorf("base %s is a prerelease; the next version is not a patch bump", base)
	}

	tag := strings.TrimSuffix(version, semver.Prerelease(version))
	if !semver.IsValid(tag) || semver.Prerelease(tag) != "" {
		return pseudoRelease{}, errors.New("cannot derive a release tag")
	}

	return pseudoRelease{rev: rev, tag: tag, baseTag: base}, nil
}

// resolveCommit is swapped out in tests so the decision logic can be exercised
// without reaching GitHub.
var resolveCommit = resolveAPIGoCommit

// planAPIRelease decides whether the API version in modFile needs an api-go
// release created for it.
func planAPIRelease(ctx context.Context, branch string, modFile *modfile.File) (releasePlan, error) {
	// Only cloud branches are subject to the invariant this plans for. main
	// deliberately allows a pseudo-version on an untagged commit, so planning
	// from it would tag whatever main happens to pin at the time.
	if !strings.HasPrefix(branch, cloudBranchPrefix) {
		return releasePlan{}, fmt.Errorf("%s applies to %s* branches only, not %q",
			modePlanAPIRelease, cloudBranchPrefix, branch)
	}

	mod, ok := findKnownModule(apiModulePath)
	if !ok {
		return releasePlan{}, fmt.Errorf("%s is not a known module", apiModulePath)
	}

	modVersion, ok := findRequiredModuleVersion(modFile, apiModulePath)
	if !ok {
		return releasePlan{}, fmt.Errorf("%s is not required by go.mod", apiModulePath)
	}
	version := modVersion.Version
	plan := releasePlan{version: version}

	if !semver.IsValid(version) {
		return plan, fmt.Errorf("%s@%s: not valid semver", apiModulePath, version)
	}

	if !module.IsPseudoVersion(version) {
		// A prerelease is a tag but not a release, so the invariant is not
		// satisfied and no patch bump can be inferred from it either. Which
		// version should follow is a judgment call, so raise it rather than
		// reporting nothing to do.
		if semver.Prerelease(version) != "" {
			return plan, fmt.Errorf("%s@%s: prerelease versions are not releases; pin a release instead", apiModulePath, version)
		}
		plan.action = actionNoop
		plan.reason = fmt.Sprintf("%s %s is already a tagged release", apiModulePath, version)
		return plan, nil
	}

	rel, err := derivePseudoRelease(version)
	if err != nil {
		return plan, fmt.Errorf("%s@%s: %w", apiModulePath, version, err)
	}

	tags, err := remoteTags(ctx, mod)
	if err != nil {
		return plan, fmt.Errorf("failed to list tags for %s: %w", mod.repoURL, err)
	}

	if at := releaseTagsAt(tags, rel.rev); len(at) > 0 {
		plan.action = actionNoop
		plan.reason = fmt.Sprintf("commit %s is already tagged %s", rel.rev, strings.Join(at, ", "))
		return plan, nil
	}

	if _, ok := tags[rel.baseTag]; !ok {
		return plan, fmt.Errorf("base tag %s does not exist in %s", rel.baseTag, apiGoRepoSlug)
	}
	// Another cloud branch pinning the same commit, or a hand-made tag, can
	// have claimed this one already. Either way it is not ours to move.
	if _, ok := tags[rel.tag]; ok {
		return plan, fmt.Errorf("tag %s already exists in %s on another commit", rel.tag, apiGoRepoSlug)
	}

	apiGoSHA, apiRef, err := resolveCommit(ctx, rel.rev)
	if err != nil {
		return plan, err
	}

	plan.action = actionRelease
	plan.reason = fmt.Sprintf("commit %s has no tag; releasing it as %s (base %s)", rel.rev, rel.tag, rel.baseTag)
	plan.tag = rel.tag
	plan.baseTag = rel.baseTag
	plan.apiGoSHA = apiGoSHA
	plan.apiRef = apiRef
	return plan, nil
}

func findKnownModule(modulePath string) (moduleSpec, bool) {
	for _, mod := range knownModules {
		if mod.modulePath == modulePath {
			return mod, true
		}
	}
	return moduleSpec{}, false
}

// resolveAPIGoCommit expands an abbreviated api-go commit to its full sha and
// reports the api commit it corresponds to.
//
// Both are needed by temporalio/api's create-release.yml: actions/checkout
// rejects an abbreviated sha, and that workflow asserts the api-go commit's
// proto/api submodule points at the api commit being released, so the pair has
// to be consistent. The submodule pointer is the only record of which api
// commit an api-go commit was generated from.
func resolveAPIGoCommit(ctx context.Context, rev string) (apiGoSHA string,
	apiRef string, err error,
) {
	apiGoSHA, err = githubSHA(ctx, fmt.Sprintf("repos/%s/commits/%s", apiGoRepoSlug, url.PathEscape(rev)))
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve commit %s in %s: %w", rev, apiGoRepoSlug, err)
	}

	apiRef, err = githubSHA(ctx, fmt.Sprintf("repos/%s/contents/proto/api?ref=%s", apiGoRepoSlug, url.QueryEscape(apiGoSHA)))
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve the api commit for api-go %s: %w", apiGoSHA, err)
	}
	return apiGoSHA, apiRef, nil
}

// githubSHA fetches a GitHub API path and returns its "sha" field; both the
// commits and contents endpoints report one.
func githubSHA(ctx context.Context, apiPath string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubAPIBase+apiPath, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	// Public repos answer unauthenticated requests, but at 60/hour, which CI
	// would exhaust.
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("GET %s: %s: %s", apiPath, resp.Status, strings.TrimSpace(string(body)))
	}

	var payload struct {
		SHA string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("GET %s: failed to decode response: %w", apiPath, err)
	}
	if payload.SHA == "" {
		return "", fmt.Errorf("GET %s: response has no sha", apiPath)
	}
	return payload.SHA, nil
}

// githubAPIBase is a variable so tests can point it at a local server.
var githubAPIBase = "https://api.github.com/"

// runPlanAPIRelease reports the plan on stdout and, under GitHub Actions, as
// step outputs. Outputs are written even when planning fails, so an alert can
// name the version and the reason.
func runPlanAPIRelease(ctx context.Context, branch string, modFile *modfile.File) error {
	plan, planErr := planAPIRelease(ctx, branch, modFile)
	if planErr != nil {
		plan.action = actionError
		plan.reason = planErr.Error()
	}

	outputs := map[string]string{
		"action":     plan.action,
		"reason":     plan.reason,
		"version":    plan.version,
		"tag":        plan.tag,
		"base_tag":   plan.baseTag,
		"api_go_sha": plan.apiGoSHA,
		"api_ref":    plan.apiRef,
	}
	for name, value := range outputs {
		fmt.Printf("%s=%s\n", name, value)
		if err := appendGitHubOutput(name, value); err != nil {
			return err
		}
	}

	return planErr
}

// appendGitHubOutput records a step output when running under GitHub Actions.
// Whitespace is collapsed: a value spanning lines would break the key=value
// format and fail the step.
func appendGitHubOutput(name, value string) error {
	path := os.Getenv("GITHUB_OUTPUT")
	if path == "" {
		return nil
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("failed to open GITHUB_OUTPUT: %w", err)
	}
	defer func() { _ = f.Close() }()

	if _, err := fmt.Fprintf(f, "%s=%s\n", name, strings.Join(strings.Fields(value), " ")); err != nil {
		return fmt.Errorf("failed to write step output %s: %w", name, err)
	}
	return nil
}
