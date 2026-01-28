package testrunner2

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
