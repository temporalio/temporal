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
	tests     []testCase // specific tests to run (for suite mode or retries)
	label     string     // display label for progress output
	skipTests []string   // tests to skip via -test.skip (for timeout retries)
}

// testPackageConfig holds configuration for package discovery.
type testPackageConfig struct {
	log         func(format string, v ...any)
	buildTags   string
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

// groupByPackage groups test files by their package path.
func (p *testPackage) groupByPackage() map[string][]testFile {
	byPkg := make(map[string][]testFile)
	for _, f := range p.files {
		byPkg[f.pkg] = append(byPkg[f.pkg], f)
	}
	return byPkg
}

// groupByMode returns work units based on the specified grouping mode.
func (p *testPackage) groupByMode(mode GroupMode) []workUnit {
	switch mode {
	case GroupByPackage:
		return p.groupUnitsPackage()
	case GroupBySuite:
		return p.groupUnitsSuite()
	default:
		return p.groupUnitsFile()
	}
}

// groupUnitsPackage returns one work unit per package.
func (p *testPackage) groupUnitsPackage() []workUnit {
	byPkg := p.groupByPackage()
	units := make([]workUnit, 0, len(byPkg))
	for pkg, files := range byPkg {
		units = append(units, workUnit{
			pkg:   pkg,
			files: files,
			tests: testCasesFromFiles(files),
			label: pkg,
		})
	}
	return units
}

// groupUnitsFile returns one work unit per file.
func (p *testPackage) groupUnitsFile() []workUnit {
	units := make([]workUnit, 0, len(p.files))
	for _, f := range p.files {
		units = append(units, workUnit{
			pkg:   f.pkg,
			files: []testFile{f},
			tests: f.tests,
			label: filepath.Base(f.path),
		})
	}
	return units
}

// groupUnitsSuite returns one work unit per TestXxx function.
// For suite mode, sharding is applied at the test level rather than file level.
func (p *testPackage) groupUnitsSuite() []workUnit {
	var units []workUnit
	for _, f := range p.files {
		for _, tc := range f.tests {
			// For suite mode, shard on pkg/testName combination
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
		// Skip file-level sharding for suite mode since it shards at the test level.
		if p.totalShards > 1 && p.groupBy != GroupBySuite && getShardForFile(path, p.totalShards) != p.shardIndex {
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

// getShardForFile returns the shard index for a given file path using consistent hashing.
func getShardForFile(path string, totalShards int) int {
	return getShardForKey(path, totalShards)
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

// testCasesToRunPattern converts a list of test cases to a -run flag regex pattern.
func testCasesToRunPattern(tests []testCase) string {
	var patterns []string
	for _, tc := range tests {
		patterns = append(patterns, testNameToRegex(tc.name))
	}
	// Don't wrap in outer ^(...)$ - Go's -run splits by / first, then matches each part
	return strings.Join(patterns, "|")
}

// testNamesToSkipPattern converts a list of test names to a -skip flag regex pattern.
func testNamesToSkipPattern(names []string) string {
	if len(names) == 0 {
		return ""
	}
	var patterns []string
	for _, name := range names {
		patterns = append(patterns, testNameToRegex(name))
	}
	return strings.Join(patterns, "|")
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
