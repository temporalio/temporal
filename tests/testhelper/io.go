// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package testhelper

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// CreateTemp is a helper function which creates a temporary file,
// and it is automatically closed after test is completed
func CreateTemp(t testing.TB, dir, pattern string) *os.File {
	t.Helper()

	tempFile, err := os.CreateTemp(dir, pattern)
	require.NoError(t, err)
	require.NotNil(t, tempFile)

	t.Cleanup(func() { require.NoError(t, os.Remove(tempFile.Name())) })
	return tempFile
}

// MkdirTemp is a helper function which creates a temporary directory,
// and they are automatically removed after test is completed
func MkdirTemp(t testing.TB, dir, pattern string) string {
	t.Helper()

	tempDir, err := os.MkdirTemp(dir, pattern)
	require.NoError(t, err)
	require.NotNil(t, tempDir)

	t.Cleanup(func() { require.NoError(t, os.RemoveAll(tempDir)) })
	return tempDir
}
