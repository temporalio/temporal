package mockgen_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/testutils"
	"go.temporal.io/server/tools/mockgen"
)

func TestNoSourceFile(t *testing.T) {
	t.Parallel()
	calls, err := runCommand([]string{"-destination", "foo.go"})
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"-destination", "foo.go"}}, calls)
}

func TestNoDestinationFile(t *testing.T) {
	t.Parallel()
	calls, err := runCommand([]string{"-source", "foo.go"})
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"-source", "foo.go"}}, calls)
}

func TestFileNotExists(t *testing.T) {
	t.Parallel()
	calls, err := runCommand([]string{
		// Random file names that shouldn't exist
		"-source", "2dc53c2850878438af974704", "-destination", "7433393f1ff8c1373e15c2ce",
	})
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.Empty(t, calls)
}

func TestSourceFileNotStale(t *testing.T) {
	t.Parallel()
	src := createTemp(t)
	dest := createTemp(t)
	chModTime(t, src.Name(), time.Now())
	chModTime(t, dest.Name(), time.Now())

	calls, err := runCommand([]string{"-source", src.Name(), "-destination", dest.Name()})
	require.NoError(t, err)
	assert.Empty(t, calls)
}

func TestSourceFileStale(t *testing.T) {
	t.Parallel()
	src := createTemp(t)
	dest := createTemp(t)
	chModTime(t, src.Name(), time.Now())
	chModTime(t, dest.Name(), time.Now().Add(-time.Second))

	calls, err := runCommand([]string{"-source", src.Name(), "-destination", dest.Name()})
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"-source", src.Name(), "-destination", dest.Name()}}, calls)
}

func runCommand(args []string) ([][]string, error) {
	var calls [][]string
	err := mockgen.Run(args, mockgen.WithExecFn(func(args []string) error {
		calls = append(calls, args)
		return nil
	}))
	return calls, err
}

func createTemp(t *testing.T) *os.File {
	return testutils.CreateTemp(t, "", "")
}

func chModTime(t *testing.T, path string, ts time.Time) {
	require.NoError(t, os.Chtimes(path, time.Time{}, ts))
}
