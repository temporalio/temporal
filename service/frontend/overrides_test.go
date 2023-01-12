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
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/api/command/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
)

func TestDisableEagerActivityDispatchForBuggyClients(t *testing.T) {
	overrides := NewOverrides()

	type Case struct {
		sdkVersion   string
		sdkName      string
		eagerAllowed bool
	}

	cases := []Case{
		{sdkName: headers.ClientNameGoSDK, sdkVersion: "1.18.1", eagerAllowed: true},
		{sdkName: headers.ClientNameTypeScriptSDK, sdkVersion: "1.4.1", eagerAllowed: false},
		{sdkName: headers.ClientNameTypeScriptSDK, sdkVersion: "1.4.4", eagerAllowed: true},
		{sdkName: headers.ClientNamePythonSDK, sdkVersion: "0.1a1", eagerAllowed: false},
		{sdkName: headers.ClientNamePythonSDK, sdkVersion: "0.1b2", eagerAllowed: false},
		{sdkName: headers.ClientNamePythonSDK, sdkVersion: "0.1b3", eagerAllowed: true},
	}
	for _, testCase := range cases {
		ctx := headers.SetVersionsForTests(context.Background(), testCase.sdkVersion, testCase.sdkName, headers.SupportedServerVersions, headers.AllFeatures)
		req := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*command.Command{
				{Attributes: &command.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &command.ScheduleActivityTaskCommandAttributes{RequestEagerExecution: true}}},
			},
		}
		overrides.DisableEagerActivityDispatchForBuggyClients(ctx, req)

		assert.Equal(t, req.GetCommands()[0].GetScheduleActivityTaskCommandAttributes().GetRequestEagerExecution(), testCase.eagerAllowed)
	}
}
