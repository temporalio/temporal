package testrunner2

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockExec captures the args passed to the exec function.
func mockExec(captured *[]string) execFunc {
	return func(ctx context.Context, dir, name string, args, env []string, output io.Writer) int {
		all := append([]string{name}, args...)
		*captured = append(*captured, all...)
		return 0
	}
}

func TestExecuteTest_EscapesSpecialCharsInSubtestNames(t *testing.T) {
	t.Parallel()

	var args []string
	executeTest(context.Background(), func(ctx context.Context, dir, name string, a, env []string, output io.Writer) int {
		args = a
		return 0
	}, executeTestInput{
		binary: "/tmp/foo.test",
		tests:  []string{"TestFoo/sub(test)"},
		output: io.Discard,
	}, nil)

	argsStr := strings.Join(args, " ")
	require.Contains(t, argsStr, `sub\(test\)`)
}

func TestCompileTest(t *testing.T) {
	t.Parallel()

	t.Run("basic compile", func(t *testing.T) {
		t.Parallel()
		var captured []string
		compileTest(context.Background(), mockExec(&captured), compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			output:     io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.Contains(t, argsStr, "test -c")
		require.Contains(t, argsStr, "-o /tmp/foo.test ./pkg/foo")
	})

	t.Run("race flag", func(t *testing.T) {
		t.Parallel()
		var captured []string
		compileTest(context.Background(), mockExec(&captured), compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			baseArgs:   []string{"-race"},
			output:     io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.Contains(t, argsStr, "-race")
	})

	t.Run("build tags", func(t *testing.T) {
		t.Parallel()
		var captured []string
		compileTest(context.Background(), mockExec(&captured), compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			buildTags:  "integration",
			output:     io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.Contains(t, argsStr, "-tags=integration")
	})

	t.Run("cover flag", func(t *testing.T) {
		t.Parallel()
		var captured []string
		compileTest(context.Background(), mockExec(&captured), compileTestInput{
			pkg:        "./pkg/foo",
			binaryPath: "/tmp/foo.test",
			baseArgs:   []string{"-coverprofile=cover.out"},
			output:     io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.Contains(t, argsStr, "-cover")
	})
}

func TestExecuteTest(t *testing.T) {
	t.Parallel()

	t.Run("cover profile", func(t *testing.T) {
		t.Parallel()
		var captured []string
		executeTest(context.Background(), mockExec(&captured), executeTestInput{
			binary:       "/tmp/foo.test",
			tests:        []string{"TestFoo"},
			coverProfile: "cover.out",
			output:       io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.Contains(t, argsStr, "-test.coverprofile=cover.out")
		require.NotContains(t, argsStr, "-test.timeout")
		require.NotContains(t, argsStr, "-test.skip")
	})

	t.Run("whole package omits run filter", func(t *testing.T) {
		t.Parallel()
		var captured []string
		executeTest(context.Background(), mockExec(&captured), executeTestInput{
			binary: "/tmp/foo.test",
			output: io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.NotContains(t, argsStr, "-test.run")
	})

	t.Run("raw run pattern", func(t *testing.T) {
		t.Parallel()
		var captured []string
		executeTest(context.Background(), mockExec(&captured), executeTestInput{
			binary:     "/tmp/foo.test",
			runPattern: "TestFoo|ExampleFoo",
			output:     io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.Contains(t, argsStr, "-test.run TestFoo|ExampleFoo")
	})

	t.Run("extra args", func(t *testing.T) {
		t.Parallel()
		var captured []string
		executeTest(context.Background(), mockExec(&captured), executeTestInput{
			binary:    "/tmp/foo.test",
			tests:     []string{"TestFoo"},
			extraArgs: []string{"-persistenceType=sql"},
			output:    io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		require.Contains(t, argsStr, "-persistenceType=sql")
	})
}
