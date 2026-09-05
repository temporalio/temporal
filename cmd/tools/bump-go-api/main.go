// bump-go-api decides whether go.temporal.io/api should be bumped to a given
// release tag, and classifies the bump as patch, minor, or major.
//
// Usage:
//
//	bump-go-api -tag v1.63.6 <go.mod>...
//
// It is invoked by .github/workflows/bump-go-api.yml, which enumerates the
// go.mod files with find and forwards the tag from a temporalio/api-go release.
// The paths are operands rather than a list baked in here so that the workflow
// and this tool cannot disagree about which modules exist. Results are printed
// as key=value lines suitable for appending to $GITHUB_OUTPUT.
//
// A tag we should not bump to -- a prerelease, a malformed tag, or a version
// that is not newer than what every inspected go.mod already requires -- is
// reported as needs_bump=false, not as an error. Only genuine problems (no
// paths given, an unreadable go.mod, or nothing requiring the module at all)
// exit non-zero.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

const modulePath = "go.temporal.io/api"

type bumpKind string

const (
	bumpMajor bumpKind = "major"
	bumpMinor bumpKind = "minor"
	bumpPatch bumpKind = "patch"
)

// decision is the result of comparing a release tag against what the repo
// currently requires.
type decision struct {
	needsBump bool
	// currentVersion is the oldest version required by any inspected go.mod.
	currentVersion string
	targetVersion  string
	kind           bumpKind
	// reasons explains, for a needsBump=false decision, why each go.mod
	// declined the bump.
	reasons []string
}

func main() {
	tag := flag.String("tag", "", "api-go release tag to bump to (e.g. v1.63.6)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: bump-go-api -tag <tag> <go.mod>...")
		flag.PrintDefaults()
	}
	flag.Parse()

	if strings.TrimSpace(*tag) == "" {
		fmt.Fprintln(os.Stderr, "Error: release tag is required; pass --tag")
		os.Exit(1)
	}

	goMods := flag.Args()
	if len(goMods) == 0 {
		fmt.Fprintln(os.Stderr, "Error: at least one go.mod path is required")
		flag.Usage()
		os.Exit(1)
	}

	current, err := currentRequirements(goMods)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	d := decide(strings.TrimSpace(*tag), current)
	for _, reason := range d.reasons {
		fmt.Fprintln(os.Stderr, reason)
	}
	fmt.Print(d.output())
}

// output renders the decision as key=value lines for $GITHUB_OUTPUT. bump_kind
// is omitted when there is nothing to bump, so a stale value from an earlier
// step can never be mistaken for this one.
func (d decision) output() string {
	var s strings.Builder
	fmt.Fprintf(&s, "needs_bump=%t\n", d.needsBump)
	fmt.Fprintf(&s, "current_version=%s\n", d.currentVersion)
	fmt.Fprintf(&s, "target_version=%s\n", d.targetVersion)
	if d.needsBump {
		fmt.Fprintf(&s, "bump_kind=%s\n", d.kind)
	}
	return s.String()
}

// requirement is the version of modulePath required by one go.mod.
type requirement struct {
	goMod   string
	version string
}

// currentRequirements reads the modulePath version required by each go.mod,
// preserving the given order so log output is deterministic.
//
// A go.mod that does not require modulePath is skipped rather than rejected:
// the caller enumerates every go.mod in the repo, and not all of them
// necessarily depend on the api module. It is an error for none of them to.
func currentRequirements(goMods []string) ([]requirement, error) {
	requirements := make([]requirement, 0, len(goMods))
	for _, path := range goMods {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", path, err)
		}

		modFile, err := modfile.Parse(path, data, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", path, err)
		}

		version, ok := findRequiredModuleVersion(modFile, modulePath)
		if !ok {
			fmt.Fprintf(os.Stderr, "%s does not require %s; skipping\n", path, modulePath)
			continue
		}

		requirements = append(requirements, requirement{goMod: path, version: version})
	}

	if len(requirements) == 0 {
		return nil, fmt.Errorf("none of the %d given go.mod files require %s",
			len(goMods), modulePath)
	}

	return requirements, nil
}

func findRequiredModuleVersion(modFile *modfile.File, path string) (string, bool) {
	for _, req := range modFile.Require {
		if req.Mod.Path == path {
			return req.Mod.Version, true
		}
	}
	return "", false
}

// decide reports whether tag is a bump worth making over every version in
// current.
func decide(tag string, current []requirement) decision {
	d := decision{targetVersion: tag, currentVersion: oldest(current)}

	if reason, ok := rejectTag(tag); !ok {
		d.reasons = append(d.reasons, reason)
		return d
	}

	// The bump must be strictly newer than *every* go.mod's requirement.
	// api-go maintains several minor lines concurrently, so a patch released
	// on an older line (v1.62.6 published while we are on v1.63.5) must not
	// drag us backwards.
	needsBump := true
	for _, req := range current {
		if semver.Compare(tag, req.version) <= 0 {
			d.reasons = append(d.reasons, fmt.Sprintf(
				"%s requires %s %s, which is not older than %s",
				req.goMod, modulePath, req.version, tag))
			needsBump = false
		}
	}
	if !needsBump {
		return d
	}

	d.needsBump = true
	// Classified against the oldest requirement, so if the two go.mod files
	// ever drift, the more conservative classification wins -- which keeps
	// auto-merge off for what is a minor change to at least one module.
	d.kind = classify(d.currentVersion, tag)
	return d
}

// rejectTag reports whether tag is a plain, tagged semver release. Anything
// else -- a prerelease, a pseudo-version, a truncated or malformed tag -- is
// not something we bump to automatically.
func rejectTag(tag string) (string, bool) {
	switch {
	case !semver.IsValid(tag):
		return fmt.Sprintf("%s is not a valid semver version", tag), false
	case module.IsPseudoVersion(tag):
		return fmt.Sprintf("%s is a pseudo-version, not a release", tag), false
	case semver.Prerelease(tag) != "":
		return fmt.Sprintf("%s is a prerelease", tag), false
	case semver.Build(tag) != "":
		return fmt.Sprintf("%s carries build metadata", tag), false
	case semver.Canonical(tag) != tag:
		// Catches abbreviated tags like "v1.64", which semver.IsValid accepts
		// but which no api-go release actually uses.
		return fmt.Sprintf("%s is not a canonical vX.Y.Z tag", tag), false
	}
	return "", true
}

func classify(from, to string) bumpKind {
	switch {
	case semver.Major(from) != semver.Major(to):
		return bumpMajor
	case semver.MajorMinor(from) != semver.MajorMinor(to):
		return bumpMinor
	default:
		return bumpPatch
	}
}

// oldest returns "" when requirements is empty.
func oldest(requirements []requirement) string {
	var lowest string
	for _, req := range requirements {
		if lowest == "" || semver.Compare(req.version, lowest) < 0 {
			lowest = req.version
		}
	}
	return lowest
}
