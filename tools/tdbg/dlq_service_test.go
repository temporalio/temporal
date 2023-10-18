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

package tdbg_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tools/tdbg"
)

type (
	dlqTestParams struct {
		dlqVersion      string
		command         string
		dlqType         string
		sourceCluster   string
		targetCluster   string
		maxMessageCount string
		lastMessageID   string
		outputFileName  string
		clientFactory   tdbg.ClientFactory
		// expectedErrSubstrings is a list of substrings that are expected to be found in the error message. We don't use an
		// exact error message to prevent this from becoming a change-detector test.
		expectedErrSubstrings []string
	}
	dlqTestCase struct {
		name     string
		override func(p *dlqTestParams)
	}
	faultyClientFactory struct {
		err error
	}
	faultyAdminClient struct {
		adminservice.AdminServiceClient
		err error
	}
)

func (f faultyAdminClient) DescribeCluster(
	context.Context,
	*adminservice.DescribeClusterRequest,
	...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	return nil, f.err
}

func (f faultyAdminClient) GetDLQTasks(
	context.Context,
	*adminservice.GetDLQTasksRequest,
	...grpc.CallOption,
) (*adminservice.GetDLQTasksResponse, error) {
	return nil, f.err
}

func (f faultyClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("not implemented")
}

func (f faultyClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return faultyAdminClient{err: f.err}
}

func appendArg(args []string, name string, val string) []string {
	if val == "" {
		return args
	}
	return append(args, "--"+name, val)
}

func (tc *dlqTestCase) Run(t *testing.T, firstAppRun chan struct{}) {
	p := dlqTestParams{
		dlqVersion:            "v2",
		dlqType:               strconv.Itoa(int(tasks.CategoryTransfer.ID())),
		sourceCluster:         "test-source-cluster",
		targetCluster:         "test-target-cluster",
		maxMessageCount:       "",
		lastMessageID:         "",
		outputFileName:        "",
		clientFactory:         tdbg.NewClientFactory(),
		expectedErrSubstrings: nil,
	}
	tc.override(&p)
	app := tdbg.NewCliApp(p.clientFactory)
	app.ExitErrHandler = func(c *cli.Context, err error) {
		return
	}
	runArgs := []string{
		"tdbg", "dlq",
	}
	runArgs = appendArg(runArgs, tdbg.FlagDLQVersion, p.dlqVersion)
	runArgs = append(runArgs, p.command)
	runArgs = appendArg(runArgs, tdbg.FlagDLQType, p.dlqType)
	runArgs = appendArg(runArgs, tdbg.FlagCluster, p.sourceCluster)
	runArgs = appendArg(runArgs, tdbg.FlagTargetCluster, p.targetCluster)
	runArgs = appendArg(runArgs, tdbg.FlagMaxMessageCount, p.maxMessageCount)
	runArgs = appendArg(runArgs, tdbg.FlagLastMessageID, p.lastMessageID)
	runArgs = appendArg(runArgs, tdbg.FlagOutputFilename, p.outputFileName)

	// TODO: this is a hack to make sure that the first app.Run() call is finished before the second one starts because
	// there is a race condition in the CLI app where all apps share the same help command pointer and try to initialize
	// it at the same time. This workaround only protects the first call because it's ok if subsequent calls happen in
	// parallel since the help command is already initialized.
	_, isFirstRun := <-firstAppRun
	err := app.Run(runArgs)
	if isFirstRun {
		close(firstAppRun)
	}
	if len(p.expectedErrSubstrings) > 0 {
		for _, s := range p.expectedErrSubstrings {
			assert.ErrorContains(t, err, s)
		}
	} else {
		assert.NoError(t, err)
	}
}

func TestDLQCommands(t *testing.T) {
	t.Parallel()

	firstAppRun := make(chan struct{}, 1)
	firstAppRun <- struct{}{}
	for _, tc := range []dlqTestCase{
		{
			name: "v2 merge",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "merge"
				p.expectedErrSubstrings = []string{"merge", "v2", "not", "implemented"}
			},
		},
		{
			name: "v2 purge",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "purge"
				p.expectedErrSubstrings = []string{"purge", "v2", "not", "implemented"}
			},
		},
		{
			name: "v2 read no source cluster",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.sourceCluster = ""
				p.expectedErrSubstrings = []string{tdbg.FlagCluster}
			},
		},
		{
			name: "v2 read no target cluster with faulty admin client",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.targetCluster = ""
				p.clientFactory = faultyClientFactory{err: errors.New("some error")}
				p.expectedErrSubstrings = []string{tdbg.FlagTargetCluster, "DescribeCluster", "some error"}
			},
		},
		{
			name: "v2 read category is not an integer",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.dlqType = "my-string"
				p.expectedErrSubstrings = []string{"category", "integer", "my-string"}
			},
		},
		{
			name: "v2 read invalid category",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.dlqType = "-1"
				p.expectedErrSubstrings = []string{"category", "-1"}
			},
		},
		{
			name: "zero max message count",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.maxMessageCount = "0"
				p.expectedErrSubstrings = []string{tdbg.FlagMaxMessageCount, "positive", "0"}
			},
		},
		{
			name: "last message ID is less than first possible message ID",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "last message ID is less than first possible message ID",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "invalid output file name",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.outputFileName = "\\0/"
				p.expectedErrSubstrings = []string{"output file", "\\0/"}
			},
		},
		{
			name: "GetDLQTasks error",
			override: func(p *dlqTestParams) {
				p.dlqVersion = "v2"
				p.command = "read"
				p.clientFactory = faultyClientFactory{err: errors.New("some error")}
				p.expectedErrSubstrings = []string{"some error", "GetDLQTasks"}
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.Run(t, firstAppRun)
		})
	}
}
