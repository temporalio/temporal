package testrunner2

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractTestCases(t *testing.T) {
	t.Parallel()

	t.Run("extracts test functions", func(t *testing.T) {
		content := `package foo

import "testing"

func TestFoo(t *testing.T) {}
func TestBar(t *testing.T) {}
`
		path := writeTestFile(t, content)
		tests, err := extractTestCases(path)
		require.NoError(t, err)
		require.Len(t, tests, 2)
		require.Equal(t, "TestFoo", tests[0].name)
		require.Equal(t, "TestBar", tests[1].name)
		require.Equal(t, 0, tests[0].attempts)
	})

	t.Run("ignores non-test functions", func(t *testing.T) {
		content := `package foo

import "testing"

func TestFoo(t *testing.T) {}
func helperFunc() {}
func BenchmarkFoo(b *testing.B) {}
func ExampleFoo() {}
`
		path := writeTestFile(t, content)
		tests, err := extractTestCases(path)
		require.NoError(t, err)
		require.Len(t, tests, 1)
		require.Equal(t, "TestFoo", tests[0].name)
	})

	t.Run("ignores methods", func(t *testing.T) {
		content := `package foo

import "testing"

type Suite struct{}

func (s *Suite) TestMethod(t *testing.T) {}
func TestFoo(t *testing.T) {}
`
		path := writeTestFile(t, content)
		tests, err := extractTestCases(path)
		require.NoError(t, err)
		require.Len(t, tests, 1)
		require.Equal(t, "TestFoo", tests[0].name)
	})

	t.Run("ignores wrong signature", func(t *testing.T) {
		content := `package foo

import "testing"

func TestNoParam() {}
func TestWrongParam(s string) {}
func TestTwoParams(t *testing.T, s string) {}
func TestFoo(t *testing.T) {}
`
		path := writeTestFile(t, content)
		tests, err := extractTestCases(path)
		require.NoError(t, err)
		require.Len(t, tests, 1)
		require.Equal(t, "TestFoo", tests[0].name)
	})

	t.Run("returns empty for no tests", func(t *testing.T) {
		content := `package foo

func helperFunc() {}
`
		path := writeTestFile(t, content)
		tests, err := extractTestCases(path)
		require.NoError(t, err)
		require.Empty(t, tests)
	})

	t.Run("handles parse error", func(t *testing.T) {
		content := `package foo

func broken( {}
`
		path := writeTestFile(t, content)
		_, err := extractTestCases(path)
		require.Error(t, err)
	})
}

func TestGetShardForKey(t *testing.T) {
	t.Parallel()

	t.Run("deterministic", func(t *testing.T) {
		shard1 := getShardForKey("foo/bar_test.go", 10)
		shard2 := getShardForKey("foo/bar_test.go", 10)
		require.Equal(t, shard1, shard2)
	})

	t.Run("distributes files", func(t *testing.T) {
		shards := make(map[int]int)
		files := []string{
			"a_test.go", "b_test.go", "c_test.go", "d_test.go",
			"e_test.go", "f_test.go", "g_test.go", "h_test.go",
		}
		for _, f := range files {
			shard := getShardForKey(f, 4)
			require.GreaterOrEqual(t, shard, 0)
			require.Less(t, shard, 4)
			shards[shard]++
		}
		// With 8 files and 4 shards, we expect some distribution
		require.Greater(t, len(shards), 1, "files should be distributed across shards")
	})

	t.Run("single shard gets all", func(t *testing.T) {
		shard := getShardForKey("any_test.go", 1)
		require.Equal(t, 0, shard)
	})
}

func TestFindTestFilesInDir(t *testing.T) {
	t.Parallel()

	t.Run("finds test files", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "foo_test.go"), `package foo
import "testing"
func TestFoo(t *testing.T) {}
`)
		writeFile(t, filepath.Join(dir, "bar_test.go"), `package foo
import "testing"
func TestBar(t *testing.T) {}
`)
		writeFile(t, filepath.Join(dir, "helper.go"), `package foo
func helper() {}
`)

		tp := &testPackage{testPackageConfig: testPackageConfig{
			log: t.Logf,
		}}
		files, err := tp.findTestFilesInDir(dir)
		require.NoError(t, err)
		require.Len(t, files, 2)

		paths := []string{files[0].path, files[1].path}
		require.Contains(t, paths, filepath.Join(dir, "foo_test.go"))
		require.Contains(t, paths, filepath.Join(dir, "bar_test.go"))
	})

	t.Run("skips files with no tests", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "foo_test.go"), `package foo
import "testing"
func TestFoo(t *testing.T) {}
`)
		writeFile(t, filepath.Join(dir, "empty_test.go"), `package foo
// no test functions
`)

		tp := &testPackage{testPackageConfig: testPackageConfig{
			log: t.Logf,
		}}
		files, err := tp.findTestFilesInDir(dir)
		require.NoError(t, err)
		require.Len(t, files, 1)
		require.Equal(t, filepath.Join(dir, "foo_test.go"), files[0].path)
	})

	t.Run("respects sharding", func(t *testing.T) {
		dir := t.TempDir()
		// Create several test files
		for _, name := range []string{"a_test.go", "b_test.go", "c_test.go", "d_test.go"} {
			writeFile(t, filepath.Join(dir, name), `package foo
import "testing"
func TestX(t *testing.T) {}
`)
		}

		// Collect files from all shards
		var allFiles []testFile
		for shard := 0; shard < 2; shard++ {
			tp := &testPackage{testPackageConfig: testPackageConfig{
				log:         t.Logf,
				totalShards: 2,
				shardIndex:  shard,
			}}
			files, err := tp.findTestFilesInDir(dir)
			require.NoError(t, err)
			allFiles = append(allFiles, files...)
		}

		// All 4 files should be covered across both shards
		require.Len(t, allFiles, 4)
	})

	t.Run("adds package prefix", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "foo_test.go"), `package foo
import "testing"
func TestFoo(t *testing.T) {}
`)

		tp := &testPackage{testPackageConfig: testPackageConfig{
			log: t.Logf,
		}}
		files, err := tp.findTestFilesInDir(dir)
		require.NoError(t, err)
		require.Len(t, files, 1)
		require.True(t, files[0].pkg == dir || files[0].pkg == "./"+dir)
	})
}

func TestTestCasesToRunPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cases    []testCase
		expected string
	}{
		{
			cases:    []testCase{{name: "TestFoo"}},
			expected: "^TestFoo$",
		},
		{
			cases:    []testCase{{name: "TestFoo"}, {name: "TestBar"}},
			expected: "^(TestFoo|TestBar)$",
		},
		{
			cases:    []testCase{{name: "TestSuite/SubTest"}},
			expected: "^TestSuite$/^SubTest$",
		},
	}

	for _, tt := range tests {
		names := make([]string, len(tt.cases))
		for i, tc := range tt.cases {
			names[i] = tc.name
		}
		t.Run(strings.Join(names, ","), func(t *testing.T) {
			result := testCasesToRunPattern(tt.cases)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildTestFilterPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		names    []string
		expected string
	}{
		{
			name:     "empty",
			names:    nil,
			expected: "",
		},
		{
			name:     "single top-level",
			names:    []string{"TestFoo"},
			expected: "^TestFoo$",
		},
		{
			name:     "multiple top-level",
			names:    []string{"TestFoo", "TestBar"},
			expected: "^(TestFoo|TestBar)$",
		},
		{
			name:     "single subtest",
			names:    []string{"TestSuite/SubTest"},
			expected: "^TestSuite$/^SubTest$",
		},
		{
			name:     "multiple subtests same parent",
			names:    []string{"TestSuite/Sub1", "TestSuite/Sub2", "TestSuite/Sub3"},
			expected: "^TestSuite$/^(Sub1|Sub2|Sub3)$",
		},
		{
			name:     "deep subtests same parent",
			names:    []string{"TestSuite/Method/Sub1", "TestSuite/Method/Sub2"},
			expected: "^TestSuite$/^Method$/^(Sub1|Sub2)$",
		},
		{
			name:     "different parents - per-level alternation",
			names:    []string{"TestSuiteA/Sub1", "TestSuiteB/Sub2"},
			expected: "^(TestSuiteA|TestSuiteB)$/^(Sub1|Sub2)$",
		},
		{
			name:     "mixed depths - uses max depth only",
			names:    []string{"TestSuite/Sub1", "TestSuite/Sub2/Deep"},
			expected: "^TestSuite$/^Sub2$/^Deep$",
		},
		{
			name:     "mixed depths - top-level and subtest uses deeper group",
			names:    []string{"TestTopLevel", "TestSuite/Sub1"},
			expected: "^TestSuite$/^Sub1$",
		},
		{
			name:     "mixed depths - suite subtests with and without sub-subtests",
			names:    []string{"TestSuite/TestA", "TestSuite/TestB/Var1", "TestSuite/TestB/Var2", "TestSuite/TestC/Var3"},
			expected: "^TestSuite$/^(TestB|TestC)$/^(Var1|Var2|Var3)$",
		},
		{
			name:     "three-level different parents - per-level alternation",
			names:    []string{"TestSuite/Enable/TestA", "TestSuite/Enable/TestB", "TestSuite/Disable/TestA", "TestSuite/Disable/TestC"},
			expected: "^TestSuite$/^(Enable|Disable)$/^(TestA|TestB|TestC)$",
		},
		{
			name:     "escapes special chars",
			names:    []string{"TestSuite/Sub(1)", "TestSuite/Sub(2)"},
			expected: `^TestSuite$/^(Sub\(1\)|Sub\(2\))$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildTestFilterPattern(tt.names)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTestNameToRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"TestFoo", "^TestFoo$"},
		{"TestSuite/SubTest", "^TestSuite$/^SubTest$"},
		{"TestSuite/Sub/Deep", "^TestSuite$/^Sub$/^Deep$"},
		{"TestFoo(special)", `^TestFoo\(special\)$`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := testNameToRegex(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGroupByMode(t *testing.T) {
	t.Parallel()

	files := []testFile{
		{path: "pkg1/a_test.go", pkg: "./pkg1", tests: []testCase{{name: "TestA1"}, {name: "TestA2"}}},
		{path: "pkg1/b_test.go", pkg: "./pkg1", tests: []testCase{{name: "TestB1"}}},
		{path: "pkg2/c_test.go", pkg: "./pkg2", tests: []testCase{{name: "TestC1"}, {name: "TestC2"}, {name: "TestC3"}}},
	}

	t.Run("test mode", func(t *testing.T) {
		tp := &testPackage{files: files}
		units := tp.groupByMode(GroupByTest)
		require.Len(t, units, 6) // 6 tests total
		for _, u := range units {
			require.Len(t, u.tests, 1)
			require.Equal(t, u.tests[0].name, u.label)
		}
	})
}

func TestMatchesRunFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		testName string
		filter   string
		expected bool
	}{
		{"empty filter matches all", "TestFoo", "", true},
		{"exact match", "TestFoo", "TestFoo", true},
		{"no match", "TestFoo", "TestBar", false},
		{"regex match", "TestFoo", "Test.*", true},
		{"alternation match first", "TestWorkflowTestSuite", "TestWorkflowTestSuite|TestSignalWorkflowTestSuite", true},
		{"alternation match second", "TestSignalWorkflowTestSuite", "TestWorkflowTestSuite|TestSignalWorkflowTestSuite", true},
		{"alternation no match", "TestOtherSuite", "TestWorkflowTestSuite|TestSignalWorkflowTestSuite", false},
		{"filter with subtest path uses only top-level", "TestActivityTestSuite", "TestActivityTestSuite/TestActivityHeartBeatWorkflow_Success", true},
		{"filter with subtest path no match", "TestOtherSuite", "TestActivityTestSuite/TestActivityHeartBeatWorkflow_Success", false},
		{"smoke test filter", "TestWorkflowTestSuite", "TestWorkflowTestSuite|TestSignalWorkflowTestSuite|TestActivityTestSuite/TestActivityHeartBeatWorkflow_Success", true},
		{"smoke test filter non-matching", "TestNDCFuncTestSuite", "TestWorkflowTestSuite|TestSignalWorkflowTestSuite|TestActivityTestSuite/TestActivityHeartBeatWorkflow_Success", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesRunFilter(tt.testName, tt.filter)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGroupUnitsTest_RunFilter(t *testing.T) {
	t.Parallel()

	files := []testFile{
		{path: "pkg/a_test.go", pkg: "./pkg", tests: []testCase{
			{name: "TestWorkflowTestSuite"},
			{name: "TestSignalWorkflowTestSuite"},
			{name: "TestActivityTestSuite"},
			{name: "TestNDCFuncTestSuite"},
			{name: "TestOtherSuite"},
		}},
	}

	t.Run("filters to matching tests only", func(t *testing.T) {
		tp := &testPackage{
			testPackageConfig: testPackageConfig{
				runFilter: "TestWorkflowTestSuite|TestSignalWorkflowTestSuite|TestActivityTestSuite",
			},
			files: files,
		}
		units := tp.groupUnitsTest()
		require.Len(t, units, 3)
		names := make(map[string]bool)
		for _, u := range units {
			names[u.tests[0].name] = true
		}
		require.True(t, names["TestWorkflowTestSuite"])
		require.True(t, names["TestSignalWorkflowTestSuite"])
		require.True(t, names["TestActivityTestSuite"])
	})

	t.Run("no filter returns all tests", func(t *testing.T) {
		tp := &testPackage{
			testPackageConfig: testPackageConfig{},
			files:             files,
		}
		units := tp.groupUnitsTest()
		require.Len(t, units, 5)
	})

	t.Run("filter with subtest path", func(t *testing.T) {
		tp := &testPackage{
			testPackageConfig: testPackageConfig{
				runFilter: "TestActivityTestSuite/TestActivityHeartBeatWorkflow_Success",
			},
			files: files,
		}
		units := tp.groupUnitsTest()
		require.Len(t, units, 1)
		require.Equal(t, "TestActivityTestSuite", units[0].tests[0].name)
	})
}

func TestGroupUnitsTest_Sharding(t *testing.T) {
	t.Parallel()

	files := []testFile{
		{path: "pkg/a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}, {name: "TestB"}, {name: "TestC"}, {name: "TestD"}}},
	}

	// Collect units from all shards
	var allUnits []workUnit
	for shard := 0; shard < 2; shard++ {
		tp := &testPackage{
			testPackageConfig: testPackageConfig{
				totalShards: 2,
				shardIndex:  shard,
			},
			files: files,
		}
		units := tp.groupUnitsTest()
		allUnits = append(allUnits, units...)
	}

	// All 4 tests should be covered across both shards
	require.Len(t, allUnits, 4)
	testNames := make(map[string]bool)
	for _, u := range allUnits {
		testNames[u.tests[0].name] = true
	}
	require.True(t, testNames["TestA"])
	require.True(t, testNames["TestB"])
	require.True(t, testNames["TestC"])
	require.True(t, testNames["TestD"])
}

func writeTestFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_file.go")
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
	return path
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
}
