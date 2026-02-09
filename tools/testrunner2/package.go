package testrunner2

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
)

// compileRunFilter compiles a -run filter string into a regexp that matches
// top-level test names. Returns nil if the filter is empty (matches all).
func compileRunFilter(filter string) *regexp.Regexp {
	if filter == "" {
		return nil
	}
	// Go's test framework splits -run on "/" and matches each level.
	// For suite filtering, we only need the top-level pattern.
	topLevel := filter
	if idx := strings.Index(filter, "/"); idx >= 0 {
		topLevel = filter[:idx]
	}
	return regexp.MustCompile(topLevel)
}

// testCase represents a single test function with its metadata.
type testCase struct {
	name     string
	attempts int // number of times this test has been attempted
}

// workUnit represents a schedulable unit of test work.
type workUnit struct {
	pkg       string     // package path
	tests     []testCase // specific tests to run (for test mode or retries)
	label     string     // display label for progress output
	skipTests []string   // tests to skip via -test.skip (for timeout retries)
}

// findTestPackages scans directories for _test.go files and returns package
// paths (one per directory that contains at least one test file).
func findTestPackages(dirs []string) ([]string, error) {
	var pkgs []string
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("reading dir %s: %w", dir, err)
		}
		hasTests := false
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), "_test.go") {
				hasTests = true
				break
			}
		}
		if !hasTests {
			continue
		}
		pkg := dir
		if !strings.HasPrefix(pkg, "./") && !strings.HasPrefix(pkg, "/") {
			pkg = "./" + pkg
		}
		pkgs = append(pkgs, pkg)
	}
	return pkgs, nil
}

// listTestsFromBinary runs a compiled test binary with -test.list to discover
// test names. Returns one test name per line, skipping summary lines.
func listTestsFromBinary(ctx context.Context, execFn execFunc, binaryPath string) ([]string, error) {
	var buf bytes.Buffer
	exitCode := execFn(ctx, "", binaryPath, []string{"-test.list", ".*"}, nil, &buf)
	if exitCode != 0 {
		return nil, fmt.Errorf("test list failed (exit code %d): %s", exitCode, buf.String())
	}

	var tests []string
	scanner := bufio.NewScanner(&buf)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Only accept lines that look like Go test names (must start with "Test").
		// This filters out summary lines ("ok", "?", "FAIL"), coverage messages
		// ("program not built with -cover"), and any other non-test output.
		if !strings.HasPrefix(line, "Test") {
			continue
		}
		tests = append(tests, line)
	}
	return tests, scanner.Err()
}

// buildWorkUnits creates one workUnit per test, applying run filter and sharding.
func buildWorkUnits(pkg string, testNames []string, runFilter string, totalShards, shardIndex int) []workUnit {
	runRe := compileRunFilter(runFilter)
	var units []workUnit
	for _, name := range testNames {
		if runRe != nil && !runRe.MatchString(name) {
			continue
		}
		if totalShards > 1 {
			shardKey := pkg + "/" + name
			if getShardForKey(shardKey, totalShards) != shardIndex {
				continue
			}
		}
		units = append(units, workUnit{
			pkg:   pkg,
			tests: []testCase{{name: name}},
			label: name,
		})
	}
	return units
}

// getShardForKey returns the shard index for a given key using consistent hashing.
func getShardForKey(key string, totalShards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(totalShards))
}

// filterParentNames removes parent names from a sorted list.
// Given [A, A/B, C], returns [A/B, C] because A is a parent of A/B.
// The input must be sorted lexicographically.
func filterParentNames(sorted []string) []string {
	var leaves []string
	for i, name := range sorted {
		if i+1 < len(sorted) && strings.HasPrefix(sorted[i+1], name+"/") {
			continue
		}
		leaves = append(leaves, name)
	}
	return leaves
}

// testCasesToRunPattern converts a list of test cases to a -run flag regex pattern.
func testCasesToRunPattern(tests []testCase) string {
	names := make([]string, len(tests))
	for i, tc := range tests {
		names[i] = tc.name
	}
	return buildTestFilterPattern(names)
}

// buildTestFilterPattern builds a regex pattern for -test.run or -test.skip from
// a list of test names (which may include subtest paths like "TestSuite/SubTest").
//
// Go's testing package splits -run/-skip patterns by "/" (via splitRegexp) before
// matching each part against the corresponding level of the test name. This means
// "|" alternation must NOT cross "/" boundaries, or the pattern will be corrupted.
//
// For example, joining "^TestA$/^Sub1$" | "^TestA$/^Sub2$" produces the broken
// pattern "^TestA$/^Sub1$|^TestA$/^Sub2$" which splitRegexp splits into
// ["^TestA$", "^Sub1$|^TestA$", "^Sub2$"].
//
// Instead, this function groups names by their parent path and builds alternation
// within each level: "^TestA$/^(Sub1|Sub2)$".
//
// When names have multiple parent groups (e.g., "A/B/X" and "A/C/Y"), this falls
// back to per-level alternation: "^A$/^(B|C)$/^(X|Y)$". This may over-match
// (e.g., "A/C/X" would also match), but is acceptable for -test.skip patterns
// used in timeout retries.
func buildTestFilterPattern(names []string) string {
	if len(names) == 0 {
		return ""
	}

	// Group names by parent path (everything before the last "/").
	byParent := make(map[string][]string)
	for _, name := range names {
		if idx := strings.LastIndex(name, "/"); idx >= 0 {
			byParent[name[:idx]] = append(byParent[name[:idx]], name[idx+1:])
		} else {
			byParent[""] = append(byParent[""], name)
		}
	}

	if len(byParent) > 1 {
		// Multiple parent groups can't be safely joined with "|" across "/" boundaries.
		// Fall back to per-level alternation which builds a pattern like:
		//   ^A$/^(B|C)$/^(X|Y)$
		// This may over-match but is safe for -test.skip (used in timeout retries).
		return buildPerLevelPattern(names)
	}

	// Exactly one parent group — extract it directly.
	var parent string
	var leaves []string
	for p, l := range byParent {
		parent, leaves = p, l
	}
	leafPattern := regexAlternation(leaves)
	if parent == "" {
		return leafPattern
	}
	return testNameToRegex(parent) + "/" + leafPattern
}

// buildPerLevelPattern builds a regex pattern by collecting all unique values at
// each "/" level and using alternation. For example, given names:
//
//	["A/B/X", "A/B/Y", "A/C/X", "A/C/Z"]
//
// it produces: "^A$/^(B|C)$/^(X|Y|Z)$"
//
// When names have mixed depths (e.g., "A/B" and "A/C/X"), only the names at the
// maximum depth are used. Go's -test.skip matches level by level, and a deeper
// pattern (3 segments) won't accidentally skip shallower tests (2 segments) since
// all pattern levels must be consumed for a full match. The shallower names are
// simply omitted from the skip pattern, so those tests re-run on retry.
func buildPerLevelPattern(names []string) string {
	if len(names) == 0 {
		return ""
	}

	// Split all names into segments and find max depth.
	segments := make([][]string, len(names))
	maxDepth := 0
	for i, name := range names {
		segments[i] = strings.Split(name, "/")
		if len(segments[i]) > maxDepth {
			maxDepth = len(segments[i])
		}
	}

	if maxDepth <= 1 {
		// No "/" separators — all names are top-level, use simple alternation.
		return regexAlternation(names)
	}

	// Keep only names at max depth. A deeper skip pattern won't accidentally
	// match shallower tests, so omitting shorter names is safe (they just re-run).
	var maxDepthSegments [][]string
	for _, segs := range segments {
		if len(segs) == maxDepth {
			maxDepthSegments = append(maxDepthSegments, segs)
		}
	}

	// For each level, collect unique values (preserving first-seen order)
	parts := make([]string, maxDepth)
	for level := 0; level < maxDepth; level++ {
		seen := make(map[string]bool)
		var values []string
		for _, segs := range maxDepthSegments {
			if !seen[segs[level]] {
				seen[segs[level]] = true
				values = append(values, segs[level])
			}
		}
		parts[level] = regexAlternation(values)
	}

	return strings.Join(parts, "/")
}

// regexAlternation builds an anchored regex that matches any of the given names.
// For a single name: "^name$". For multiple: "^(name1|name2|...)$".
func regexAlternation(names []string) string {
	if len(names) == 1 {
		return "^" + regexp.QuoteMeta(names[0]) + "$"
	}
	escaped := make([]string, len(names))
	for i, n := range names {
		escaped[i] = regexp.QuoteMeta(n)
	}
	return "^(" + strings.Join(escaped, "|") + ")$"
}

// testNameToRegex converts a test name (possibly with subtests) to a -run flag regex.
// For example, "TestFoo/subtest" becomes "^TestFoo$/^subtest$".
func testNameToRegex(test string) string {
	parts := strings.Split(test, "/")
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteByte('^')
		sb.WriteString(regexp.QuoteMeta(p))
		sb.WriteByte('$')
	}
	return sb.String()
}
