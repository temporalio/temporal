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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetRepoRootDirectory(t *testing.T) {
	t.Run("env var set", func(t *testing.T) {
		directory, err := GetRepoRootDirectory(WithGetenv(func(s string) string {
			assert.Equal(t, "TEMPORAL_ROOT", s)
			return "/tmp/temporal"
		}))
		require.NoError(t, err)
		assert.Equal(t, "/tmp/temporal", directory)
	})

	t.Run("env var not set", func(t *testing.T) {
		t.Run("getwd returns error", func(t *testing.T) {
			_, err := GetRepoRootDirectory(WithGetenv(func(s string) string {
				return ""
			}), WithGetwd(func() (string, error) {
				return "", errors.New("some error")
			}))
			require.Error(t, err)
			assert.ErrorContains(t, err, "some error")
		})

		t.Run("getwd returns path not ending in temporal", func(t *testing.T) {
			_, err := GetRepoRootDirectory(WithGetenv(func(s string) string {
				return ""
			}), WithGetwd(func() (string, error) {
				return "/temp/tempura/", nil
			}))
			require.Error(t, err)
			assert.ErrorContains(t, err, "unable to find repo path")
			assert.ErrorContains(t, err, "temporal")
			assert.ErrorContains(t, err, "tempura")
		})

		t.Run("getwd returns path ending with temporal", func(t *testing.T) {
			directory, err := GetRepoRootDirectory(WithGetenv(func(s string) string {
				return ""
			}), WithGetwd(func() (string, error) {
				return "/tmp/temporal/", nil
			}))
			require.NoError(t, err)
			assert.Equal(t, "/tmp/temporal/", directory)
		})
	})
}
