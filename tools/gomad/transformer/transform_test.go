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

package transformer_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"

	"go.temporal.io/server/tools/gomad/transformer"
)

func TestTransform(t *testing.T) {
	var testCount int
	ignoreFunc := func(pkg *packages.Package) bool {
		return !strings.HasSuffix(pkg.PkgPath, "/testdata")
	}
	dir, _ := filepath.Abs(filepath.Join("testdata"))
	t.Log("fixtures: " + dir)
	cfg := &transformer.Config{
		Dir:        filepath.Join(dir, "test_inputs"),
		BuildFlags: []string{"-tags=fixture"},
		Skip:       ignoreFunc,
		ResultFunc: func(pkg *packages.Package, files map[string]string) string {
			if ignoreFunc(pkg) {
				return ""
			}

			for srcPath, transformed := range files {
				name := filepath.Base(srcPath)
				t.Run(name, func(t *testing.T) {
					expected, err := os.ReadFile(filepath.Join(dir, "test_outputs", name))
					if err != nil {
						t.Fatal(err)
					}
					require.Equal(t, string(expected), transformed)
				})
				testCount += 1
			}

			return ""
		},
	}
	_, err := transformer.Run(cfg)
	require.NoError(t, err)
	require.Equal(t, 25, testCount)
}
