package parallelize

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "adds t.Parallel to multiple tests",
			input: `package foo_test

import "testing"

func TestFoo(t *testing.T) {
	t.Log("foo")
}

func TestBar(t *testing.T) {
	t.Log("bar")
}
`,
			expected: `package foo_test

import "testing"

func TestFoo(t *testing.T) {
	t.Parallel()
	t.Log("foo")
}

func TestBar(t *testing.T) {
	t.Parallel()
	t.Log("bar")
}
`,
		},
		{
			name: "skips test that already has t.Parallel",
			input: `package foo_test

import "testing"

func TestFoo(t *testing.T) {
	t.Parallel()
	t.Log("hello")
}
`,
			expected: `package foo_test

import "testing"

func TestFoo(t *testing.T) {
	t.Parallel()
	t.Log("hello")
}
`,
		},
		{
			name: "skips test with parallelize:ignore",
			input: `package foo_test

import "testing"

//parallelize:ignore
func TestFoo(t *testing.T) {
	t.Log("hello")
}
`,
			expected: `package foo_test

import "testing"

//parallelize:ignore
func TestFoo(t *testing.T) {
	t.Log("hello")
}
`,
		},
		{
			name: "skips non-test functions",
			input: `package foo_test

import "testing"

func helperFunc(t *testing.T) {
	t.Log("helper")
}
`,
			expected: `package foo_test

import "testing"

func helperFunc(t *testing.T) {
	t.Log("helper")
}
`,
		},
		{
			name: "skips test suite methods",
			input: `package foo_test

import "testing"

func (s *MySuite) TestFoo(t *testing.T) {
	t.Log("suite test")
}
`,
			expected: `package foo_test

import "testing"

func (s *MySuite) TestFoo(t *testing.T) {
	t.Log("suite test")
}
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "example_test.go")
			require.NoError(t, os.WriteFile(path, []byte(tc.input), 0644))

			require.NoError(t, processFile(path))

			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(got))
		})
	}
}

func TestProcessDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	testFile := `package foo_test

import "testing"

func TestFoo(t *testing.T) {
	t.Log("hello")
}
`
	nonTestFile := `package foo

func Foo() {}
`

	require.NoError(t, os.WriteFile(filepath.Join(dir, "foo_test.go"), []byte(testFile), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "foo.go"), []byte(nonTestFile), 0644))

	require.NoError(t, processDir(dir))

	got, err := os.ReadFile(filepath.Join(dir, "foo_test.go"))
	require.NoError(t, err)
	require.Contains(t, string(got), "t.Parallel()")

	got, err = os.ReadFile(filepath.Join(dir, "foo.go"))
	require.NoError(t, err)
	require.Equal(t, nonTestFile, string(got))
}
