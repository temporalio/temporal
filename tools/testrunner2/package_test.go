package testrunner2

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestFindTestPackages(t *testing.T) {
	t.Parallel()

	t.Run("finds directories with test files", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "foo_test.go"), "package foo\n")
		writeFile(t, filepath.Join(dir, "helper.go"), "package foo\n")

		pkgs, err := findTestPackages([]string{dir})
		require.NoError(t, err)
		require.Len(t, pkgs, 1)
	})

	t.Run("skips directories without test files", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "helper.go"), "package foo\n")

		pkgs, err := findTestPackages([]string{dir})
		require.NoError(t, err)
		require.Empty(t, pkgs)
	})

	t.Run("returns error for non-existent directory", func(t *testing.T) {
		_, err := findTestPackages([]string{"/nonexistent/path"})
		require.Error(t, err)
	})

	t.Run("adds ./ prefix for relative paths", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "foo_test.go"), "package foo\n")

		// Use a relative-looking path (no "./" or "/" prefix)
		pkgs, err := findTestPackages([]string{dir})
		require.NoError(t, err)
		require.Len(t, pkgs, 1)
		require.True(t, strings.HasPrefix(pkgs[0], "./") || strings.HasPrefix(pkgs[0], "/"))
	})

	t.Run("multiple directories", func(t *testing.T) {
		dir1 := t.TempDir()
		dir2 := t.TempDir()
		writeFile(t, filepath.Join(dir1, "a_test.go"), "package a\n")
		writeFile(t, filepath.Join(dir2, "b_test.go"), "package b\n")

		pkgs, err := findTestPackages([]string{dir1, dir2})
		require.NoError(t, err)
		require.Len(t, pkgs, 2)
	})
}

func TestBuildWorkUnits(t *testing.T) {
	t.Parallel()

	t.Run("creates one unit per test", func(t *testing.T) {
		units := buildWorkUnits("./pkg", []string{"TestA", "TestB", "TestC"}, "", 0, 0)
		require.Len(t, units, 3)
		for _, u := range units {
			require.Len(t, u.tests, 1)
			require.Equal(t, u.tests[0].name, u.label)
			require.Equal(t, "./pkg", u.pkg)
		}
	})

	t.Run("applies run filter", func(t *testing.T) {
		testNames := []string{
			"TestWorkflowTestSuite",
			"TestSignalWorkflowTestSuite",
			"TestActivityTestSuite",
			"TestNDCFuncTestSuite",
			"TestOtherSuite",
		}
		units := buildWorkUnits("./pkg", testNames, "TestWorkflowTestSuite|TestSignalWorkflowTestSuite|TestActivityTestSuite", 0, 0)
		require.Len(t, units, 3)
		names := make(map[string]bool)
		for _, u := range units {
			names[u.tests[0].name] = true
		}
		require.True(t, names["TestWorkflowTestSuite"])
		require.True(t, names["TestSignalWorkflowTestSuite"])
		require.True(t, names["TestActivityTestSuite"])
	})

	t.Run("filter with subtest path uses top-level", func(t *testing.T) {
		testNames := []string{"TestActivityTestSuite", "TestOtherSuite"}
		units := buildWorkUnits("./pkg", testNames, "TestActivityTestSuite/TestActivityHeartBeatWorkflow_Success", 0, 0)
		require.Len(t, units, 1)
		require.Equal(t, "TestActivityTestSuite", units[0].tests[0].name)
	})

	t.Run("no filter returns all", func(t *testing.T) {
		units := buildWorkUnits("./pkg", []string{"TestA", "TestB"}, "", 0, 0)
		require.Len(t, units, 2)
	})

	t.Run("sharding distributes tests", func(t *testing.T) {
		testNames := []string{"TestA", "TestB", "TestC", "TestD"}

		var allUnits []workUnit
		for shard := 0; shard < 2; shard++ {
			units := buildWorkUnits("./pkg", testNames, "", 2, shard)
			allUnits = append(allUnits, units...)
		}

		// All tests should be covered across both shards
		require.Len(t, allUnits, 4)
		names := make(map[string]bool)
		for _, u := range allUnits {
			names[u.tests[0].name] = true
		}
		require.True(t, names["TestA"])
		require.True(t, names["TestB"])
		require.True(t, names["TestC"])
		require.True(t, names["TestD"])
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

func TestCompileRunFilter(t *testing.T) {
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
			re := compileRunFilter(tt.filter)
			if re == nil {
				require.True(t, tt.expected, "nil filter should match everything")
			} else {
				require.Equal(t, tt.expected, re.MatchString(tt.testName))
			}
		})
	}
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
