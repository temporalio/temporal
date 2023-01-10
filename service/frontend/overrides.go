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

package frontend

import (
	"context"

	"github.com/blang/semver/v4"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
)

// Overrides defines a set of special case behaviors like compensating for buggy
// SDK implementations
type Overrides struct {
	minTypeScriptEagerActivitySupportedVersion semver.Version
}

func NewOverrides() *Overrides {
	return &Overrides{
		minTypeScriptEagerActivitySupportedVersion: semver.MustParse("1.4.4"),
	}
}

func (o *Overrides) shouldForceDisableEagerDispatch(sdkName, sdkVersion string) bool {
	if sdkName == headers.ClientNamePythonSDK && (sdkVersion == "0.1a1" || sdkVersion == "0.1a2" || sdkVersion == "0.1b1" || sdkVersion == "0.1b2") {
		return true
	} else if sdkName == headers.ClientNameTypeScriptSDK {
		ver, err := semver.Parse(sdkVersion)
		// Don't bother with non semver
		if err != nil {
			return false
		}
		return ver.LT(o.minTypeScriptEagerActivitySupportedVersion)
	}
	return false
}

func (o *Overrides) disableEagerDispatch(
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	for _, cmd := range request.GetCommands() {
		attrs := cmd.GetScheduleActivityTaskCommandAttributes()
		if attrs != nil {
			attrs.RequestEagerExecution = false
		}
	}
}

// DisableEagerActivityDispatchForBuggyClients compensates for SDK versions
// that have buggy implementations of eager activity dispatch
func (o *Overrides) DisableEagerActivityDispatchForBuggyClients(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	sdkName, sdkVersion := headers.GetClientNameAndVersion(ctx)
	if o.shouldForceDisableEagerDispatch(sdkName, sdkVersion) {
		o.disableEagerDispatch(request)
	}
}
