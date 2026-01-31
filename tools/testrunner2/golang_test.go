package testrunner2

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompileTest(t *testing.T) {
	var capturedName string
	var capturedArgs []string

	mockExec := func(ctx context.Context, dir, name string, args []string, output io.Writer) int {
		capturedName = name
		capturedArgs = args
		return 0
	}

	t.Run("basic compile", func(t *testing.T) {
		result := compileTest(context.Background(), mockExec, compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			output:     io.Discard,
		}, nil)
		require.Equal(t, 0, result.exitCode)
		require.Equal(t, "go", capturedName)
		require.Equal(t, []string{"test", "-c", "-o", "/tmp/foo.test", "./pkg/foo"}, capturedArgs)
	})

	t.Run("with build tags", func(t *testing.T) {
		result := compileTest(context.Background(), mockExec, compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			buildTags:  "integration",
			output:     io.Discard,
		}, nil)
		require.Equal(t, 0, result.exitCode)
		require.Contains(t, capturedArgs, "-tags=integration")
	})

	t.Run("with race flag", func(t *testing.T) {
		result := compileTest(context.Background(), mockExec, compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			baseArgs:   []string{"-race"},
			output:     io.Discard,
		}, nil)
		require.Equal(t, 0, result.exitCode)
		require.Contains(t, capturedArgs, "-race")
	})

	t.Run("compile failure", func(t *testing.T) {
		failExec := func(ctx context.Context, dir, name string, args []string, output io.Writer) int {
			return 1
		}
		result := compileTest(context.Background(), failExec, compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			output:     io.Discard,
		}, nil)
		require.Equal(t, 1, result.exitCode)
	})
}

func TestExecuteTest(t *testing.T) {
	var capturedName string
	var capturedArgs []string

	mockExec := func(ctx context.Context, dir, name string, args []string, output io.Writer) int {
		capturedName = name
		capturedArgs = args
		return 0
	}

	t.Run("basic execution", func(t *testing.T) {
		result := executeTest(context.Background(), mockExec, executeTestInput{
			binary:       "/tmp/foo.test",
			tests:        []testCase{{name: "TestFoo"}},
			coverProfile: "/tmp/cover.out",
			output:       io.Discard,
		}, nil)

		require.Equal(t, 0, result.exitCode)
		require.Equal(t, "/tmp/foo.test", capturedName)

		argsStr := strings.Join(capturedArgs, " ")
		require.Contains(t, argsStr, "-test.v")
		require.Contains(t, argsStr, "-test.run")
		require.Contains(t, argsStr, "^TestFoo$")
		require.Contains(t, argsStr, "-test.coverprofile=/tmp/cover.out")
	})

	t.Run("multiple tests", func(t *testing.T) {
		executeTest(context.Background(), mockExec, executeTestInput{
			binary:       "/tmp/foo.test",
			tests:        []testCase{{name: "TestFoo"}, {name: "TestBar"}},
			coverProfile: "/tmp/cover.out",
			output:       io.Discard,
		}, nil)

		argsStr := strings.Join(capturedArgs, " ")
		require.Contains(t, argsStr, "^TestFoo$")
		require.Contains(t, argsStr, "^TestBar$")
	})

	t.Run("with timeout", func(t *testing.T) {
		executeTest(context.Background(), mockExec, executeTestInput{
			binary:       "/tmp/foo.test",
			tests:        []testCase{{name: "TestFoo"}},
			timeout:      5 * time.Minute,
			coverProfile: "/tmp/cover.out",
			output:       io.Discard,
		}, nil)

		argsStr := strings.Join(capturedArgs, " ")
		require.Contains(t, argsStr, "-test.timeout=5m0s")
	})

	t.Run("subtest names with special chars are escaped", func(t *testing.T) {
		executeTest(context.Background(), mockExec, executeTestInput{
			binary:       "/tmp/foo.test",
			tests:        []testCase{{name: "TestFoo/sub(test)"}},
			coverProfile: "/tmp/cover.out",
			output:       io.Discard,
		}, nil)

		argsStr := strings.Join(capturedArgs, " ")
		require.Contains(t, argsStr, `sub\(test\)`)
	})

	t.Run("no cover profile", func(t *testing.T) {
		executeTest(context.Background(), mockExec, executeTestInput{
			binary: "/tmp/foo.test",
			tests:  []testCase{{name: "TestFoo"}},
			output: io.Discard,
		}, nil)

		argsStr := strings.Join(capturedArgs, " ")
		require.NotContains(t, argsStr, "-test.coverprofile")
	})
}
