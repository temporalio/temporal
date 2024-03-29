// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package mocksync_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/testutils"
	"go.temporal.io/server/tools/mocksync"
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

func TestSourceFileNotExists(t *testing.T) {
	t.Parallel()
	dest := createTemp(t)
	calls, err := runCommand([]string{
		// Random file names that shouldn't exist
		"-source", "2dc53c2850878438af974704", "-destination", dest.Name(),
	})
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.Empty(t, calls)
}

func TestDestFileNotExists(t *testing.T) {
	t.Parallel()
	source := createTemp(t)
	calls, err := runCommand([]string{
		// Random file names that shouldn't exist
		"-source", source.Name(), "-destination", "7433393f1ff8c1373e15c2ce",
	})
	require.NoError(t, err)
	assert.Len(t, calls, 1)
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
	err := mocksync.Run(func(args []string) error {
		calls = append(calls, args)
		return nil
	}, args)
	return calls, err
}

func createTemp(t *testing.T) *os.File {
	return testutils.CreateTemp(t, "", "")
}

func chModTime(t *testing.T, path string, ts time.Time) {
	require.NoError(t, os.Chtimes(path, time.Time{}, ts))
}
