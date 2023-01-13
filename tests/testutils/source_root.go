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

package testutils

import (
	"os"
	"path/filepath"
	"runtime"
)

var (
	_, fileName, _, _ = runtime.Caller(0) // should be .../temporal/tests/testhelper/source_root.go
	rootDirectory     = filepath.Dir(filepath.Dir(filepath.Dir(fileName)))
)

// GetRepoRootDirectory returns the root directory of the temporal repo.
func GetRepoRootDirectory(opts ...Option) string {
	p := &osParams{
		Getenv: os.Getenv,
	}
	for _, opt := range opts {
		opt(p)
	}
	if customRootDirectory := p.Getenv("TEMPORAL_ROOT"); customRootDirectory != "" {
		return customRootDirectory
	}
	return rootDirectory
}

// region Options for GetRepoRootDirectory.
// Used for testing.

type osParams struct {
	Getenv func(string) string
}

type Option func(os *osParams)

func WithGetenv(getenv func(string) string) Option {
	return func(os *osParams) {
		os.Getenv = getenv
	}
}

// endregion
