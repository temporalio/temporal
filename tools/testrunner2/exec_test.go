package testrunner2

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
		tests:  []testCase{{name: "TestFoo/sub(test)"}},
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
		assert.Contains(t, argsStr, "test -c")
		assert.Contains(t, argsStr, "-o /tmp/foo.test ./pkg/foo")
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
		assert.Contains(t, argsStr, "-race")
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
		assert.Contains(t, argsStr, "-tags=integration")
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
		assert.Contains(t, argsStr, "-cover")
	})
}

func TestRunDirectGoTest(t *testing.T) {
	t.Parallel()

	t.Run("basic args", func(t *testing.T) {
		t.Parallel()
		var captured []string
		runDirectGoTest(context.Background(), mockExec(&captured), runDirectGoTestInput{
			pkgs:   []string{"./pkg/a", "./pkg/b"},
			output: io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		assert.Contains(t, argsStr, "test -v")
		assert.Contains(t, argsStr, "./pkg/a ./pkg/b")
	})

	t.Run("run filter", func(t *testing.T) {
		t.Parallel()
		var captured []string
		runDirectGoTest(context.Background(), mockExec(&captured), runDirectGoTestInput{
			pkgs:      []string{"./pkg/a"},
			runFilter: "^TestFoo$",
			output:    io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		assert.Contains(t, argsStr, "-run ^TestFoo$")
	})

	t.Run("race and tags", func(t *testing.T) {
		t.Parallel()
		var captured []string
		runDirectGoTest(context.Background(), mockExec(&captured), runDirectGoTestInput{
			pkgs:      []string{"./pkg/a"},
			race:      true,
			buildTags: "integration",
			output:    io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		assert.Contains(t, argsStr, "-race")
		assert.Contains(t, argsStr, "-tags=integration")
	})

	t.Run("extra args", func(t *testing.T) {
		t.Parallel()
		var captured []string
		runDirectGoTest(context.Background(), mockExec(&captured), runDirectGoTestInput{
			pkgs:      []string{"./pkg/a"},
			extraArgs: []string{"-shuffle=on", "-count=1"},
			output:    io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		assert.Contains(t, argsStr, "-shuffle=on")
		assert.Contains(t, argsStr, "-count=1")
	})

	t.Run("no timeout or skip args", func(t *testing.T) {
		t.Parallel()
		var captured []string
		runDirectGoTest(context.Background(), mockExec(&captured), runDirectGoTestInput{
			pkgs:   []string{"./pkg/a"},
			output: io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		assert.NotContains(t, argsStr, "-timeout")
		assert.NotContains(t, argsStr, "-skip")
	})
}

func TestExecuteTest(t *testing.T) {
	t.Parallel()

	t.Run("cover profile", func(t *testing.T) {
		t.Parallel()
		var captured []string
		executeTest(context.Background(), mockExec(&captured), executeTestInput{
			binary:       "/tmp/foo.test",
			tests:        []testCase{{name: "TestFoo"}},
			coverProfile: "cover.out",
			output:       io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		assert.Contains(t, argsStr, "-test.coverprofile=cover.out")
		assert.NotContains(t, argsStr, "-test.timeout")
		assert.NotContains(t, argsStr, "-test.skip")
	})

	t.Run("extra args", func(t *testing.T) {
		t.Parallel()
		var captured []string
		executeTest(context.Background(), mockExec(&captured), executeTestInput{
			binary:    "/tmp/foo.test",
			tests:     []testCase{{name: "TestFoo"}},
			extraArgs: []string{"-persistenceType=sql"},
			output:    io.Discard,
		}, nil)

		argsStr := strings.Join(captured, " ")
		assert.Contains(t, argsStr, "-persistenceType=sql")
	})
}
