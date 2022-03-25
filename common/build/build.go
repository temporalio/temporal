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

package build

import (
	"runtime/debug"
	"time"
)

type (
	Info struct {
		Available bool
		GoVersion string
		GoArch    string
		GoOs      string
		// CgoEnabled indicates whether cgo was enabled when this build was created.
		CgoEnabled bool

		// GitRevision is the git revision associated with this build.
		GitRevision string
		// GitTime is the git revision time.
		GitTime     time.Time
		GitModified bool
	}
)

var (
	InfoData Info
)

func init() {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	InfoData.Available = true
	InfoData.GoVersion = buildInfo.GoVersion

	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "GOARCH":
			InfoData.GoArch = setting.Value
		case "GOOS":
			InfoData.GoOs = setting.Value
		case "CGO_ENABLED":
			InfoData.CgoEnabled = setting.Value == "1"
		case "vcs.revision":
			InfoData.GitRevision = setting.Value
		case "vcs.time":
			if gitTime, err := time.Parse(time.RFC3339, setting.Value); err == nil {
				InfoData.GitTime = gitTime
			}
		case "vcs.modified":
			InfoData.GitModified = setting.Value == "true"
		}
	}
}
