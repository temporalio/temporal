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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
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
		adminClient     *fakeAdminClient
		clientFactory   tdbg.ClientFactory
		// expectedErrSubstrings is a list of substrings that are expected to be found in the error message. We don't use an
		// exact error message to prevent this from becoming a change-detector test.
		expectedErrSubstrings []string
	}
	dlqTestCase struct {
		name           string
		version        string
		override       func(p *dlqTestParams)
		validateStdout func(t *testing.T, b *bytes.Buffer)
		validateStderr func(t *testing.T, b *bytes.Buffer)
	}
	fakeClientFactory struct {
		adminClient adminservice.AdminServiceClient
	}
	fakeAdminClient struct {
		adminservice.AdminServiceClient
		err error

		// ListQueues
		nextListQueueResponse int
		previousPageToken     []byte
		listQueueResponses    []*adminservice.ListQueuesResponse
	}
)

func (tc *dlqTestCase) Run(t *testing.T, firstAppRun chan struct{}) {
	faultyAdminClient := &fakeAdminClient{err: errors.New("did not expect client to be used")}
	// v2 by default
	if tc.version == "" {
		tc.version = "v2"
	}
	p := dlqTestParams{
		dlqVersion:            tc.version,
		dlqType:               strconv.Itoa(tasks.CategoryTransfer.ID()),
		sourceCluster:         "test-source-cluster",
		targetCluster:         "test-target-cluster",
		maxMessageCount:       "",
		lastMessageID:         "",
		outputFileName:        "",
		expectedErrSubstrings: nil,
		adminClient:           faultyAdminClient,
		clientFactory:         fakeClientFactory{adminClient: faultyAdminClient},
	}
	tc.override(&p)
	var stdout, stderr bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = p.clientFactory
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	runArgs := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
	}
	runArgs = appendArg(runArgs, tdbg.FlagDLQVersion, p.dlqVersion)
	runArgs = append(runArgs, p.command)
	if p.command != "list" {
		runArgs = appendArg(runArgs, tdbg.FlagDLQType, p.dlqType)
		runArgs = appendArg(runArgs, tdbg.FlagCluster, p.sourceCluster)
		runArgs = appendArg(runArgs, tdbg.FlagTargetCluster, p.targetCluster)
		runArgs = appendArg(runArgs, tdbg.FlagMaxMessageCount, p.maxMessageCount)
		runArgs = appendArg(runArgs, tdbg.FlagLastMessageID, p.lastMessageID)
	}
	runArgs = appendArg(runArgs, tdbg.FlagOutputFilename, p.outputFileName)

	// TODO: this is a hack to make sure that the first app.Run() call is finished before the second one starts because
	// there is a race condition in the CLI app where all apps share the same help command pointer and try to initialize
	// it at the same time. This workaround only protects the first call because it's ok if subsequent calls happen in
	// parallel since the help command is already initialized.
	_, isFirstRun := <-firstAppRun
	t.Logf("Running %v", runArgs)
	err := app.Run(runArgs)
	if isFirstRun {
		close(firstAppRun)
	}
	if len(p.expectedErrSubstrings) > 0 {
		assert.Error(t, err, "Expected error to contain %v", p.expectedErrSubstrings)
		for _, s := range p.expectedErrSubstrings {
			assert.ErrorContains(t, err, s)
		}
	} else {
		assert.NoError(t, err)
	}

	if tc.validateStdout != nil {
		tc.validateStdout(t, &stdout)
	}
	if tc.validateStderr != nil {
		tc.validateStderr(t, &stderr)
	}
}

func TestDLQCommand_V2(t *testing.T) {
	t.Parallel()

	firstAppRun := make(chan struct{}, 1)
	firstAppRun <- struct{}{}
	for _, tc := range []dlqTestCase{
		{
			name: "read no target cluster with faulty admin client",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.targetCluster = ""
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{tdbg.FlagTargetCluster, "DescribeCluster", "some error"}
			},
		},
		{
			name: "read no source cluster for replication task",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.sourceCluster = ""
				p.dlqType = strconv.Itoa(tasks.CategoryReplication.ID())
				p.expectedErrSubstrings = []string{tdbg.FlagCluster, "replication", "source cluster"}
			},
		},
		{
			name: "read category is not an integer",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.dlqType = "my-string"
				p.expectedErrSubstrings = []string{"category", "integer", "my-string"}
			},
		},
		{
			name: "read invalid category",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.dlqType = "-1"
				p.expectedErrSubstrings = []string{"category", "-1"}
			},
		},
		{
			name: "zero max message count",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.maxMessageCount = "0"
				p.expectedErrSubstrings = []string{tdbg.FlagMaxMessageCount, "positive", "0"}
			},
		},
		{
			name: "last message ID is less than first possible message ID",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "last message ID is less than first possible message ID",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "invalid output file name",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.outputFileName = "\\0/"
				p.expectedErrSubstrings = []string{"output file", "\\0/"}
			},
		},
		{
			name: "GetDLQTasks error",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{"some error", "GetDLQTasks"}
			},
		},
		{
			name: "purge invalid last message ID",
			override: func(p *dlqTestParams) {
				p.command = "purge"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "purge client err",
			override: func(p *dlqTestParams) {
				p.command = "purge"
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{"some error", "PurgeDLQTasks"}
			},
		},
		{
			name: "merge invalid last message ID",
			override: func(p *dlqTestParams) {
				p.command = "merge"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "merge client err",
			override: func(p *dlqTestParams) {
				p.command = "merge"
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{"some error", "MergeDLQTasks"}
			},
		},
		{
			name: "list no queues",
			override: func(p *dlqTestParams) {
				p.adminClient.err = nil
				p.command = "list"
			},
		},
		{
			name: "list queues, paginated",
			override: func(p *dlqTestParams) {
				p.adminClient.err = nil
				p.command = "list"
				p.adminClient.listQueueResponses = []*adminservice.ListQueuesResponse{
					{
						Queues: []*adminservice.ListQueuesResponse_QueueInfo{
							{
								QueueName:    "queueOne",
								MessageCount: 13,
							}, {
								QueueName:    "queueTwo",
								MessageCount: 42,
							},
						},
						NextPageToken: []byte{0x41, 0x41, 0x41},
					}, {
						Queues: []*adminservice.ListQueuesResponse_QueueInfo{
							{
								QueueName:    "queueThree",
								MessageCount: 0,
							},
						},
						NextPageToken: nil,
					},
				}
			},
			validateStdout: func(t *testing.T, b *bytes.Buffer) {
				// One line per queue. For my own sanity we just expect them all to exist; I'm not validating formatting or order here
				patterns := []string{`queueOne\s.*?|\s*13\b`, `queueTwo\s.*?|\s*42\b`, `queueThree\s.*?|\s*0\b`}
				for _, p := range patterns {
					assert.Regexp(t, p, b.String())
				}

			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.version = "v2"
			tc.Run(t, firstAppRun)
		})
	}
}

func (f fakeClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("not implemented")
}

func (f fakeClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return f.adminClient
}

// The fake admin client accepts and returns canned responses
// It tracks whether the correct page token is passed in the request when iterating
func (f *fakeAdminClient) ListQueues(_ context.Context, req *adminservice.ListQueuesRequest, _ ...grpc.CallOption) (*adminservice.ListQueuesResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.nextListQueueResponse >= len(f.listQueueResponses) {
		return &adminservice.ListQueuesResponse{}, nil
	}
	if bytes.Compare(f.previousPageToken, req.NextPageToken) != 0 {
		return nil, fmt.Errorf("expected page token %v, got %v", f.previousPageToken, req.NextPageToken)
	}

	resp := f.listQueueResponses[f.nextListQueueResponse]
	f.nextListQueueResponse++
	f.previousPageToken = resp.NextPageToken
	return resp, nil
}

func (f *fakeAdminClient) DescribeCluster(
	context.Context,
	*adminservice.DescribeClusterRequest,
	...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	// We _must_ return a non-nil result if the error isn't nil
	return &adminservice.DescribeClusterResponse{}, nil
}

func (f *fakeAdminClient) GetDLQTasks(
	context.Context,
	*adminservice.GetDLQTasksRequest,
	...grpc.CallOption,
) (*adminservice.GetDLQTasksResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &adminservice.GetDLQTasksResponse{}, nil
}

func (f *fakeAdminClient) PurgeDLQTasks(
	context.Context,
	*adminservice.PurgeDLQTasksRequest,
	...grpc.CallOption,
) (*adminservice.PurgeDLQTasksResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &adminservice.PurgeDLQTasksResponse{}, nil
}

func (f *fakeAdminClient) MergeDLQTasks(
	context.Context,
	*adminservice.MergeDLQTasksRequest,
	...grpc.CallOption,
) (*adminservice.MergeDLQTasksResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &adminservice.MergeDLQTasksResponse{}, nil
}

func appendArg(args []string, name string, val string) []string {
	if val == "" {
		return args
	}
	return append(args, "--"+name, val)
}
