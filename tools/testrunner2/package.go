package testrunner2

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// matchesRunFilter checks if a test name matches the -run filter pattern.
// The filter is split on "/" (like Go's test framework) and only the first
// segment is used to match top-level test names.
func matchesRunFilter(testName, filter string) bool {
	if filter == "" {
		return true
	}
	// Go's test framework splits -run on "/" and matches each level.
	// For suite filtering, we only need the top-level pattern.
	topLevel := filter
	if idx := strings.Index(filter, "/"); idx >= 0 {
		topLevel = filter[:idx]
	}
	matched, err := regexp.MatchString(topLevel, testName)
	return err == nil && matched
}

// testCase represents a single test function with its metadata.
type testCase struct {
	name     string
	attempts int // number of times this test has been attempted
}

// testFile represents a test file and its test functions.
type testFile struct {
	path  string
	pkg   string
	tests []testCase // tests to run (all tests on first attempt, only failed tests on retry)
}

// workUnit represents a schedulable unit of test work.
type workUnit struct {
	pkg       string     // package path
	files     []testFile // files containing the tests
	tests     []testCase // specific tests to run (for test mode or retries)
	label     string     // display label for progress output
	skipTests []string   // tests to skip via -test.skip (for timeout retries)
}

// testPackageConfig holds configuration for package discovery.
type testPackageConfig struct {
	log         func(format string, v ...any)
	buildTags   string
	runFilter   string // -run filter pattern to select tests
	totalShards int
	shardIndex  int
	groupBy     GroupMode // needed to determine sharding strategy
}

// testPackage discovers test packages.
type testPackage struct {
	testPackageConfig
	files []testFile
}

// newTestPackage creates a testPackage by discovering test files in the given directories.
func newTestPackage(cfg testPackageConfig, dirs []string) (*testPackage, error) {
	tp := &testPackage{testPackageConfig: cfg}

	for _, dir := range dirs {
		files, err := tp.findTestFilesInDir(dir)
		if err != nil {
			return nil, fmt.Errorf("failed to find test files in %s: %w", dir, err)
		}
		tp.files = append(tp.files, files...)
	}

	return tp, nil
}

// groupByMode returns work units based on the specified grouping mode.
func (p *testPackage) groupByMode(mode GroupMode) []workUnit {
	switch mode {
	case GroupByTest:
		return p.groupUnitsTest()
	default:
		return p.groupUnitsTest()
	}
}

// groupUnitsTest returns one work unit per TestXxx function.
// In test mode, sharding is applied at the test level rather than file level.
func (p *testPackage) groupUnitsTest() []workUnit {
	var units []workUnit
	for _, f := range p.files {
		for _, tc := range f.tests {
			// Filter by -run pattern if set
			if !matchesRunFilter(tc.name, p.runFilter) {
				continue
			}
			// In test mode, shard on pkg/testName combination
			if p.totalShards > 1 {
				shardKey := f.pkg + "/" + tc.name
				if getShardForKey(shardKey, p.totalShards) != p.shardIndex {
					continue
				}
			}
			units = append(units, workUnit{
				pkg: f.pkg,
				files: []testFile{{
					path:  f.path,
					pkg:   f.pkg,
					tests: []testCase{tc},
				}},
				tests: []testCase{tc},
				label: tc.name,
			})
		}
	}
	return units
}

// findTestFilesInDir finds all test files in a directory (non-recursive) and extracts their test function names.
// If sharding is enabled, only files belonging to the current shard are included.
func (p *testPackage) findTestFilesInDir(dir string) ([]testFile, error) {
	var files []testFile

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}

		path := filepath.Join(dir, entry.Name())

		// Filter by shard early to avoid unnecessary file reads.
		// Skip file-level sharding for test mode since it shards at the test level.
		if p.totalShards > 1 && p.groupBy != GroupByTest && getShardForKey(path, p.totalShards) != p.shardIndex {
			continue
		}

		// Extract test function names from the file
		tests, err := extractTestCases(path)
		if err != nil {
			p.log("warning: failed to extract test names from %s: %v", path, err)
			continue
		}
		if len(tests) == 0 {
			// No test functions found in this file, skip it
			continue
		}

		// Get the package path
		pkg := dir
		if !strings.HasPrefix(pkg, "./") && !strings.HasPrefix(pkg, "/") {
			pkg = "./" + pkg
		}

		files = append(files, testFile{
			path:  path,
			pkg:   pkg,
			tests: tests,
		})
	}
	return files, nil
}

// extractTestCases extracts all test functions from a test file using Go's AST parser.
func extractTestCases(path string) ([]testCase, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}

	var tests []testCase
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue // skip non-functions and methods
		}
		name := fn.Name.Name
		if !strings.HasPrefix(name, "Test") {
			continue
		}
		// Check signature: func TestXxx(t *testing.T)
		if fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
			continue
		}
		param := fn.Type.Params.List[0]
		star, ok := param.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		sel, ok := star.X.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok || ident.Name != "testing" || sel.Sel.Name != "T" {
			continue
		}
		tests = append(tests, testCase{name: name})
	}
	return tests, nil
}

// getShardForKey returns the shard index for a given key using consistent hashing.
func getShardForKey(key string, totalShards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(totalShards))
}

// testCasesFromFiles returns all test cases from the given test files.
func testCasesFromFiles(files []testFile) []testCase {
	var tests []testCase
	for _, tf := range files {
		tests = append(tests, tf.tests...)
	}
	return tests
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

	for parent, leaves := range byParent {
		leafPattern := regexAlternation(leaves)
		if parent == "" {
			return leafPattern
		}
		return testNameToRegex(parent) + "/" + leafPattern
	}
	return "" // unreachable
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
		return "" // no "/" separators, caller should use regexAlternation directly
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
