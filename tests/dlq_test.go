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

package tests

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tools/tdbg"
)

type (
	dlqSuite struct {
		FunctionalTestBase
		*require.Assertions
		dlq persistence.HistoryTaskQueueManager
	}
	dlqTestCase struct {
		name string
		dlqTestParams
		configure func(*dlqTestParams)
	}
	dlqTestParams struct {
		maxMessageCount     string
		lastMessageID       string
		targetCluster       string
		expectedNumMessages int
	}
)

func (s *dlqSuite) SetupSuite() {
	s.setupSuite(
		"testdata/cluster.yaml",
		WithFxOptionsForService(primitives.HistoryService, fx.Populate(&s.dlq)),
	)
}

func (s *dlqSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *dlqSuite) SetupTest() {
	if TestFlags.PersistenceType == "sql" {
		s.T().Skip("skipping DLQ tests for SQL persistence")
	}
	s.Assertions = require.New(s.T())
}

func TestDLQSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(dlqSuite))
}

func (s *dlqSuite) TestTDBG() {
	ctx := context.Background()

	namespaceID := "test-namespace"
	workflowID := "test-workflow-id"
	workflowKey := definition.NewWorkflowKey(namespaceID, workflowID, "test-run-id")

	category := tasks.CategoryTransfer
	sourceCluster := "test-source-cluster-" + s.T().Name()
	// Note: it's ok that this isn't unique across tests because the queue name will still be unique due to the source
	// cluster name being included in the queue name. We use the current cluster name because that's what the default
	// is if the target cluster flag isn't specified.
	targetCluster := "active"
	queueKey := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      category,
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
	}
	_, err := s.dlq.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	s.NoError(err)
	for i := 0; i < 4; i++ {
		task := &tasks.WorkflowTask{
			WorkflowKey: workflowKey,
			TaskID:      int64(42 + i),
		}
		_, err := s.dlq.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
			QueueType:     queueKey.QueueType,
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
			Task:          task,
		})
		s.NoError(err)
	}
	file, err := os.CreateTemp("", "*")
	s.NoError(err)
	s.T().Cleanup(func() {
		s.NoError(os.Remove(file.Name()))
	})
	app := tdbg.NewCliApp(tdbg.NewClientFactory(tdbg.WithFrontendAddress("membership://frontend")))
	app.ExitErrHandler = func(c *cli.Context, err error) {
		s.Fail("TDBG command failed", err.Error())
	}

	for _, tc := range []dlqTestCase{
		{
			name: "max message count exceeded",
			configure: func(params *dlqTestParams) {
				params.maxMessageCount = "2"
				params.lastMessageID = "999"
				params.expectedNumMessages = 2
			},
		},
		{
			name: "last message ID exceeded",
			configure: func(params *dlqTestParams) {
				params.maxMessageCount = "999"
				params.lastMessageID = "2" // first message is 0, so this should return 3 messages: 0, 1, 2
				params.expectedNumMessages = 3
			},
		},
		{
			name: "target cluster specified",
			configure: func(params *dlqTestParams) {
			},
		},
		{
			name: "target cluster not specified",
			configure: func(params *dlqTestParams) {
				params.targetCluster = ""
			},
		},
	} {
		tc := tc
		s.Run(tc.name, func() {
			tc.maxMessageCount = "999"
			tc.lastMessageID = "999"
			tc.expectedNumMessages = 4
			tc.targetCluster = targetCluster
			tc.configure(&tc.dlqTestParams)
			cmd := []string{
				"tdbg",
				"dlq",
				"--" + tdbg.FlagDLQVersion, "v2",
				"read",
				"--" + tdbg.FlagDLQType, strconv.Itoa(int(tasks.CategoryTransfer.ID())),
				"--" + tdbg.FlagCluster, sourceCluster,
				"--" + tdbg.FlagPageSize, "1",
				"--" + tdbg.FlagMaxMessageCount, tc.maxMessageCount,
				"--" + tdbg.FlagLastMessageID, tc.lastMessageID,
				"--" + tdbg.FlagOutputFilename, file.Name(),
			}
			if tc.targetCluster != "" {
				cmd = append(cmd, "--"+tdbg.FlagTargetCluster, tc.targetCluster)
			}
			cmdString := strings.Join(cmd, " ")
			s.T().Log("TDBG command:", cmdString)
			err = app.Run(cmd)
			s.NoError(err)

			s.T().Log("TDBG output:")
			s.T().Log("========================================")
			bytes, err := io.ReadAll(file)
			s.NoError(err)
			s.T().Log(string(bytes))
			_, err = file.Seek(0, io.SeekStart)
			s.NoError(err)
			s.T().Log("========================================")
			s.verifyOutputContainsTasks(file, tc.expectedNumMessages)
		})
	}
}

func (s *dlqSuite) verifyOutputContainsTasks(file *os.File, expectedNumTasks int) {
	decoder := json.NewDecoder(file)
	var unmarshaler jsonpb.Unmarshaler

	for i := 0; i < expectedNumTasks; i++ {
		var task commonspb.HistoryDLQTask
		err := unmarshaler.UnmarshalNext(decoder, &task)
		s.NoError(err, "encountered error on task at index %d", i)
		s.Equal(int64(persistence.FirstQueueMessageID+i), task.Metadata.MessageId)

		var taskInfo persistencespb.TransferTaskInfo
		err = taskInfo.Unmarshal(task.Task.Task.Data)
		s.NoError(err)
		s.Equal(enums.TASK_TYPE_TRANSFER_WORKFLOW_TASK, taskInfo.TaskType)
		s.Equal("test-namespace", taskInfo.NamespaceId)
		s.Equal("test-workflow-id", taskInfo.WorkflowId)
		s.Equal("test-run-id", taskInfo.RunId)
		s.Equal(int64(42+i), taskInfo.TaskId)
	}
	err := unmarshaler.UnmarshalNext(decoder, new(commonspb.HistoryDLQTask))
	s.ErrorContains(err, "EOF", "should not print anything after the last task")
}
