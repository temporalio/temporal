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

package tdbgtest_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
)

type (
	testClient struct {
		adminservice.AdminServiceClient
		getDLQTasksFn func(request *adminservice.GetDLQTasksRequest) (*adminservice.GetDLQTasksResponse, error)
	}
)

// TestParseDLQMessages tests that [tdbgtest.ParseDLQMessages] can parse a JSONL file containing serialized tasks.
func TestParseDLQMessages(t *testing.T) {
	t.Parallel()

	task := &tasks.WorkflowTask{
		WorkflowKey: definition.WorkflowKey{
			WorkflowID: tests.WorkflowID,
		},
		TaskID: 13,
	}
	blob, err := serialization.NewTaskSerializer().SerializeTask(task)
	require.NoError(t, err)
	client := &testClient{
		getDLQTasksFn: func(request *adminservice.GetDLQTasksRequest) (*adminservice.GetDLQTasksResponse, error) {
			return &adminservice.GetDLQTasksResponse{
				DlqTasks: []*commonspb.HistoryDLQTask{{
					Metadata: &commonspb.HistoryDLQTaskMetadata{
						MessageId: 21,
					},
					Payload: &commonspb.HistoryTask{
						ShardId: 34,
						Blob:    blob,
					},
				},
				},
			}, nil
		},
	}
	var b bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = client
		params.Writer = &b
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	err = app.RunContext(ctx, []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"--" + tdbg.FlagDLQVersion, "v2",
		"read",
		"--" + tdbg.FlagDLQType, "1",
		"--" + tdbg.FlagTargetCluster, "test-target-cluster",
	})
	require.NoError(t, err)
	output := b.String()
	t.Log("output:", output)
	messages, err := tdbgtest.ParseDLQMessages(&b, func() *persistencespb.TransferTaskInfo {
		return new(persistencespb.TransferTaskInfo)
	})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	message := messages[0]
	assert.Equal(t, 13, int(message.Payload.TaskId))
	assert.Equal(t, 21, int(message.MessageID))
	assert.Equal(t, 34, int(message.ShardID))
}

func (t *testClient) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return t
}

func (t *testClient) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("unimplemented")
}

func (t *testClient) GetDLQTasks(
	_ context.Context,
	request *adminservice.GetDLQTasksRequest,
	_ ...grpc.CallOption,
) (*adminservice.GetDLQTasksResponse, error) {
	return t.getDLQTasksFn(request)
}
