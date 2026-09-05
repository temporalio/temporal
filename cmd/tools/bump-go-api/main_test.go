package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeGoMod(apiVersion string) string {
	return fmt.Sprintf("module test\n\ngo 1.21\n\nrequire (\n\t%s %s\n)\n", modulePath, apiVersion)
}

func writeGoMod(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "go.mod")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func requirements(versions ...string) []requirement {
	reqs := make([]requirement, 0, len(versions))
	for i, version := range versions {
		reqs = append(reqs, requirement{goMod: fmt.Sprintf("go.mod.%d", i), version: version})
	}
	return reqs
}

// TestDecideSingleModule covers the version-ordering decision: the boundary at
// the currently required version, and each side of it.
func TestDecideSingleModule(t *testing.T) {
	const current = "v1.63.5"

	tests := []struct {
		name      string
		tag       string
		needsBump bool
		kind      bumpKind
	}{
		{name: "next patch", tag: "v1.63.6", needsBump: true, kind: bumpPatch},
		{name: "next minor", tag: "v1.64.0", needsBump: true, kind: bumpMinor},
		{name: "next major", tag: "v2.0.0", needsBump: true, kind: bumpMajor},
		{
			// Would be wrong under a lexicographic comparison.
			name:      "double-digit patch sorts numerically",
			tag:       "v1.63.10",
			needsBump: true,
			kind:      bumpPatch,
		},
		{
			name:      "many minors ahead is still a minor bump",
			tag:       "v1.70.0",
			needsBump: true,
			kind:      bumpMinor,
		},
		// The boundary itself, and everything below it.
		{name: "same version", tag: current},
		{name: "previous patch", tag: "v1.63.4"},
		{
			// api-go maintains several minor lines at once, so this is a real
			// event, and the one a naive `go get @tag` gets wrong.
			name: "patch on an older minor line",
			tag:  "v1.62.6",
		},
		{name: "older minor", tag: "v1.62.0"},
		{name: "older major", tag: "v0.99.0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := decide(tc.tag, requirements(current))

			require.Equal(t, tc.needsBump, d.needsBump)
			require.Equal(t, tc.kind, d.kind)
			require.Equal(t, current, d.currentVersion)
			require.Equal(t, tc.tag, d.targetVersion)
		})
	}
}

// TestDecideRejectsTag covers tags that are not plain releases. All are
// non-errors: the workflow simply does nothing.
func TestDecideRejectsTag(t *testing.T) {
	tests := []struct {
		name string
		tag  string
	}{
		{name: "prerelease", tag: "v1.64.0-rc1"},
		{name: "prerelease with dotted identifier", tag: "v1.64.0-alpha.1"},
		{name: "pseudo-version", tag: "v1.63.6-0.20260825170506-bd1da98aab15"},
		{name: "build metadata", tag: "v1.64.0+meta"},
		{name: "abbreviated to major.minor", tag: "v1.64"},
		{name: "abbreviated to major", tag: "v1"},
		{name: "missing v prefix", tag: "1.64.0"},
		{name: "not a version", tag: "latest"},
		{name: "empty", tag: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := decide(tc.tag, requirements("v1.63.5"))

			require.False(t, d.needsBump)
			require.Empty(t, d.kind)
			require.NotEmpty(t, d.reasons, "a rejected tag should explain itself")
		})
	}
}

// TestDecideMultipleModules covers the lockstep requirement: the bump has to be
// newer than every go.mod, and is classified against the oldest of them.
func TestDecideMultipleModules(t *testing.T) {
	tests := []struct {
		name      string
		versions  []string
		tag       string
		needsBump bool
		kind      bumpKind
		current   string
	}{
		{
			name:      "both older",
			versions:  []string{"v1.63.5", "v1.63.5"},
			tag:       "v1.63.6",
			needsBump: true,
			kind:      bumpPatch,
			current:   "v1.63.5",
		},
		{
			name:      "drifted, both older, classified against the oldest",
			versions:  []string{"v1.63.5", "v1.62.0"},
			tag:       "v1.63.6",
			needsBump: true,
			kind:      bumpMinor,
			current:   "v1.62.0",
		},
		{
			// The newer module blocks the bump even though the other would
			// accept it -- otherwise it would be silently downgraded.
			name:     "one module already newer",
			versions: []string{"v1.64.0", "v1.62.0"},
			tag:      "v1.63.6",
			current:  "v1.62.0",
		},
		{
			name:     "one module already at the tag",
			versions: []string{"v1.63.6", "v1.63.5"},
			tag:      "v1.63.6",
			current:  "v1.63.5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := decide(tc.tag, requirements(tc.versions...))

			require.Equal(t, tc.needsBump, d.needsBump)
			require.Equal(t, tc.kind, d.kind)
			require.Equal(t, tc.current, d.currentVersion)
		})
	}
}

func TestOutput(t *testing.T) {
	t.Run("bump includes bump_kind", func(t *testing.T) {
		out := decide("v1.64.0", requirements("v1.63.5")).output()

		require.Equal(t, strings.Join([]string{
			"needs_bump=true",
			"current_version=v1.63.5",
			"target_version=v1.64.0",
			"bump_kind=minor",
			"",
		}, "\n"), out)
	})

	t.Run("no bump omits bump_kind", func(t *testing.T) {
		out := decide("v1.62.0", requirements("v1.63.5")).output()

		require.Equal(t, strings.Join([]string{
			"needs_bump=false",
			"current_version=v1.63.5",
			"target_version=v1.62.0",
			"",
		}, "\n"), out)
		require.NotContains(t, out, "bump_kind")
	})
}

func TestCurrentRequirements(t *testing.T) {
	t.Run("reads the required version", func(t *testing.T) {
		path := writeGoMod(t, makeGoMod("v1.63.5"))

		reqs, err := currentRequirements([]string{path})

		require.NoError(t, err)
		require.Equal(t, []requirement{{goMod: path, version: "v1.63.5"}}, reqs)
	})

	t.Run("preserves the given order", func(t *testing.T) {
		first := writeGoMod(t, makeGoMod("v1.63.5"))
		second := writeGoMod(t, makeGoMod("v1.62.0"))

		reqs, err := currentRequirements([]string{first, second})

		require.NoError(t, err)
		require.Equal(t, []string{first, second}, []string{reqs[0].goMod, reqs[1].goMod})
	})

	t.Run("skips a go.mod that does not require the module", func(t *testing.T) {
		// The workflow enumerates every go.mod in the repo, so it can hand us
		// modules that have nothing to do with the api module.
		withModule := writeGoMod(t, makeGoMod("v1.63.5"))
		without := writeGoMod(t, "module test\n\ngo 1.21\n")

		reqs, err := currentRequirements([]string{withModule, without})

		require.NoError(t, err)
		require.Equal(t, []requirement{{goMod: withModule, version: "v1.63.5"}}, reqs)
	})

	t.Run("errors when no go.mod requires the module", func(t *testing.T) {
		first := writeGoMod(t, "module a\n\ngo 1.21\n")
		second := writeGoMod(t, "module b\n\ngo 1.21\n")

		_, err := currentRequirements([]string{first, second})

		require.ErrorContains(t, err, modulePath)
		require.ErrorContains(t, err, "none of the 2 given go.mod files")
	})

	t.Run("errors when given no paths at all", func(t *testing.T) {
		_, err := currentRequirements(nil)

		require.ErrorContains(t, err, "none of the 0 given go.mod files")
	})

	t.Run("errors when the go.mod is missing", func(t *testing.T) {
		_, err := currentRequirements([]string{filepath.Join(t.TempDir(), "absent.go.mod")})

		require.ErrorContains(t, err, "failed to read")
	})

	t.Run("errors when the go.mod is malformed", func(t *testing.T) {
		path := writeGoMod(t, "this is not a go.mod\n")

		_, err := currentRequirements([]string{path})

		require.ErrorContains(t, err, "failed to parse")
	})
}
