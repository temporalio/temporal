package testrunner2

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractTestCases(t *testing.T) {
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

func TestGetShardForFile(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		shard1 := getShardForFile("foo/bar_test.go", 10)
		shard2 := getShardForFile("foo/bar_test.go", 10)
		require.Equal(t, shard1, shard2)
	})

	t.Run("distributes files", func(t *testing.T) {
		shards := make(map[int]int)
		files := []string{
			"a_test.go", "b_test.go", "c_test.go", "d_test.go",
			"e_test.go", "f_test.go", "g_test.go", "h_test.go",
		}
		for _, f := range files {
			shard := getShardForFile(f, 4)
			require.GreaterOrEqual(t, shard, 0)
			require.Less(t, shard, 4)
			shards[shard]++
		}
		// With 8 files and 4 shards, we expect some distribution
		require.Greater(t, len(shards), 1, "files should be distributed across shards")
	})

	t.Run("single shard gets all", func(t *testing.T) {
		shard := getShardForFile("any_test.go", 1)
		require.Equal(t, 0, shard)
	})
}

func TestFindTestFilesInDir(t *testing.T) {
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
			expected: "^TestFoo$|^TestBar$",
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

func TestTestNameToRegex(t *testing.T) {
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
	files := []testFile{
		{path: "pkg1/a_test.go", pkg: "./pkg1", tests: []testCase{{name: "TestA1"}, {name: "TestA2"}}},
		{path: "pkg1/b_test.go", pkg: "./pkg1", tests: []testCase{{name: "TestB1"}}},
		{path: "pkg2/c_test.go", pkg: "./pkg2", tests: []testCase{{name: "TestC1"}, {name: "TestC2"}, {name: "TestC3"}}},
	}

	t.Run("package mode", func(t *testing.T) {
		tp := &testPackage{files: files}
		units := tp.groupByMode(GroupByPackage)
		require.Len(t, units, 2) // 2 packages
		// Find the units
		var pkg1Unit, pkg2Unit *workUnit
		for i := range units {
			switch units[i].pkg {
			case "./pkg1":
				pkg1Unit = &units[i]
			case "./pkg2":
				pkg2Unit = &units[i]
			default:
				t.Fatalf("unexpected package: %s", units[i].pkg)
			}
		}
		require.NotNil(t, pkg1Unit)
		require.NotNil(t, pkg2Unit)
		require.Len(t, pkg1Unit.files, 2)
		require.Len(t, pkg2Unit.files, 1)
		require.Equal(t, "./pkg1", pkg1Unit.label)
		require.Equal(t, "./pkg2", pkg2Unit.label)
		// unit.tests is always populated
		require.Len(t, pkg1Unit.tests, 3) // TestA1, TestA2, TestB1
		require.Len(t, pkg2Unit.tests, 3) // TestC1, TestC2, TestC3
	})

	t.Run("file mode", func(t *testing.T) {
		tp := &testPackage{files: files}
		units := tp.groupByMode(GroupByFile)
		require.Len(t, units, 3) // 3 files
		for _, u := range units {
			require.Len(t, u.files, 1)
			// unit.tests matches the file's tests
			require.Len(t, u.tests, len(u.files[0].tests))
		}
	})

	t.Run("suite mode", func(t *testing.T) {
		tp := &testPackage{files: files}
		units := tp.groupByMode(GroupBySuite)
		require.Len(t, units, 6) // 6 tests total
		for _, u := range units {
			require.Len(t, u.tests, 1)
			require.Equal(t, u.tests[0].name, u.label)
		}
	})
}

func TestGroupUnitsSuite_Sharding(t *testing.T) {
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
		units := tp.groupUnitsSuite()
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
