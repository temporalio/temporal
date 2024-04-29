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
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tools/tdbg"
)

type (
	// nilDeserializer is a [tdbg.TaskBlobProtoDeserializer] that returns nil [proto.Message] objects.
	nilDeserializer struct{}
	// faultyDeserializer is a [tdbg.TaskBlobProtoDeserializer] that returns an error when you try to deserialize a blob.
	faultyDeserializer struct{}
	// faultyWriter is an [io.Writer] that returns an error when you try to write to it.
	faultyWriter       struct{}
	exampleAdminClient struct {
		adminservice.AdminServiceClient
	}
)

var (
	customCategory = tasks.Category{}
)

func ExampleTaskBlobEncoder() {
	var output bytes.Buffer
	app := tdbg.NewCliApp(func(params *tdbg.Params) {
		params.Writer = &output
		stockEncoder := params.TaskBlobEncoder
		params.TaskBlobEncoder = tdbg.TaskBlobEncoderFn(func(
			writer io.Writer,
			taskCategoryID int,
			blob *commonpb.DataBlob,
		) error {
			if taskCategoryID == customCategory.ID() {
				_, err := writer.Write(append([]byte("hello, "), blob.Data...))
				return err
			}
			return stockEncoder.Encode(writer, taskCategoryID, blob)
		})
	})
	file, err := os.CreateTemp("", "*")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := os.Remove(file.Name()); err != nil {
			panic(err)
		}
	}()
	_, err = file.Write([]byte("\"world\""))
	if err != nil {
		panic(err)
	}
	err = app.Run([]string{
		"tdbg", "decode", "task",
		"--" + tdbg.FlagEncoding, enumspb.ENCODING_TYPE_JSON.String(),
		"--" + tdbg.FlagTaskCategoryID, strconv.Itoa(customCategory.ID()),
		"--" + tdbg.FlagBinaryFile, file.Name(),
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(output.String())

	// Output:
	// hello, "world"
}

// Tests the [tdbg.NewPredefinedTaskBlobDeserializer] function.
func TestPredefinedTasks(t *testing.T) {
	t.Parallel()
	encoder := tdbg.NewProtoTaskBlobEncoder(tdbg.NewPredefinedTaskBlobDeserializer())
	var buf bytes.Buffer
	historyTasks := []tasks.Task{
		&tasks.WorkflowTask{},
		&tasks.UserTimerTask{},
		&tasks.CloseExecutionVisibilityTask{},
		&tasks.HistoryReplicationTask{},
		&tasks.ArchiveExecutionTask{},
		&tasks.StateMachineOutboundTask{},
	}
	serializer := serialization.NewTaskSerializer()
	expectedTaskTypes := make([]string, len(historyTasks))
	for i, task := range historyTasks {
		expectedTaskTypes[i] = enumsspb.TaskType_name[int32(task.GetType())]
		blob, err := serializer.SerializeTask(task)
		require.NoError(t, err)
		err = encoder.Encode(&buf, task.GetCategory().ID(), blob)
		require.NoError(t, err)
		buf.WriteString("\n")
	}
	output := buf.String()
	t.Log("output:")
	t.Log(output)
	for _, taskType := range expectedTaskTypes {
		assert.Contains(t, output, taskType)
	}

	// Test an unsupported task category.
	err := encoder.Encode(io.Discard, -1, &commonpb.DataBlob{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "unsupported task category")
}

// Tests the [tdbg.ProtoTaskBlobEncoder.Encode] function with a [tdbg.TaskBlobProtoDeserializer] that returns an error.
func TestProtoTaskBlobEncoder_DeserializeFailed(t *testing.T) {
	t.Parallel()
	encoder := tdbg.NewProtoTaskBlobEncoder(faultyDeserializer{})
	err := encoder.Encode(io.Discard, 0, &commonpb.DataBlob{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to deserialize task blob")
}

// Tests the [tdbg.ProtoTaskBlobEncoder.Encode] function with an [io.Writer] that returns an error.
func TestProtoTaskBlobEncoder_WriteFailed(t *testing.T) {
	t.Parallel()
	encoder := tdbg.NewProtoTaskBlobEncoder(nilDeserializer{})
	err := encoder.Encode(faultyWriter{}, 0, &commonpb.DataBlob{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to write marshalled task blob")
}

func (t nilDeserializer) Deserialize(int, *commonpb.DataBlob) (proto.Message, error) {
	return nil, nil
}

func (f faultyDeserializer) Deserialize(int, *commonpb.DataBlob) (proto.Message, error) {
	return nil, assert.AnError
}

func (w faultyWriter) Write(_ []byte) (int, error) {
	return 0, assert.AnError
}
