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

package persistence_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// TestQueueKey_Determinism tests that the queue name generated from a QueueKey is deterministic. This is important to
// test for because we don't want to accidentally change the queue name generation algorithm and break the mapping of
// queue keys to queue names.
func TestQueueKey_Determinism(t *testing.T) {
	name := persistence.QueueKey{
		Category:      tasks.CategoryTransfer,
		SourceCluster: "a",
		TargetCluster: "b",
	}.GetQueueName()
	assert.Equal(t, name, "1_a_b_5aAf7hTg")
}

// TestQueueKey_Conflicts tests that unique tuples of cluster names containing the delimiter character will not produce
// names with conflicts when used to form queue names.
func TestQueueKey_Conflicts(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		explanation string
		source1     string
		target1     string
		source2     string
		target2     string
	}{
		{
			name: "(a,b_c) and (a_b,c)",
			explanation: "If we just concatenate the cluster names with the queue name delimiter, both of these would" +
				" produce the same queue name: 1_a_b_c",
			source1: "a",
			target1: "b_c",
			source2: "a_b",
			target2: "c",
		},
		{
			name: "(x_,x) and (x,_x)",
			explanation: "If we concatenate the cluster names with the queue name delimiter and a hash of the" +
				" concatenated cluster names, both of these would produce the same queue name: 1_x__x_<hash(x_x)>",
			source1: "x_",
			target1: "x",
			source2: "x",
			target2: "_x",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k1 := persistence.QueueKey{
				Category:      tasks.CategoryTransfer,
				SourceCluster: tc.source1,
				TargetCluster: tc.target1,
			}.GetQueueName()
			k2 := persistence.QueueKey{
				Category:      tasks.CategoryTransfer,
				SourceCluster: tc.source2,
				TargetCluster: tc.target2,
			}.GetQueueName()

			// This test would fail if we did something naive to form the queue key like <category>_<source>_<target>.
			assert.NotEqual(t, k1, k2,
				"Two pairs of cluster names which are the same when concatenated with the queue name "+
					"delimiter should not have the same queue name.", tc.explanation)
		})
	}
}

func TestHistoryTaskQueueManager_ErrSerializeTaskToEnqueue(t *testing.T) {
	t.Parallel()

	task := tasks.NewFakeTask(definition.WorkflowKey{}, tasks.Category{}, time.Time{})
	m := persistence.NewHistoryTaskQueueManager(nil, serialization.NewSerializer())
	_, err := m.EnqueueTask(context.Background(), &persistence.EnqueueTaskRequest{
		Task:          task,
		SourceShardID: 1,
	})
	assert.ErrorContains(t, err, persistence.ErrMsgSerializeTaskToEnqueue, "EnqueueTask should return "+
		"ErrMsgSerializeTaskToEnqueue when the task cannot be serialized due to an invalid task category")
}

func TestHistoryTaskQueueManager_InvalidShardID(t *testing.T) {
	t.Parallel()

	task := &tasks.WorkflowTask{}
	m := persistence.NewHistoryTaskQueueManager(nil, serialization.NewSerializer())
	_, err := m.EnqueueTask(context.Background(), &persistence.EnqueueTaskRequest{
		Task:          task,
		SourceShardID: 0,
	})
	assert.ErrorIs(t, err, persistence.ErrShardIDInvalid)
}

// corruptQueue is a QueueV2 implementation that returns a single message that cannot be deserialized into a task.
type corruptQueue struct {
	persistence.QueueV2
}

func (f corruptQueue) ReadMessages(
	context.Context,
	*persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	return &persistence.InternalReadMessagesResponse{
		Messages: []persistence.QueueV2Message{
			{
				Data: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte("some bytes that cannot be deserialized into a task"),
				},
			},
		},
		NextPageToken: nil,
	}, nil
}

func TestHistoryTaskQueueManager_ReadTasks_ErrDeserializeRawHistoryTask(t *testing.T) {
	t.Parallel()

	m := persistence.NewHistoryTaskQueueManager(corruptQueue{}, serialization.NewSerializer())
	_, err := m.ReadTasks(context.Background(), &persistence.ReadTasksRequest{
		QueueKey: persistence.QueueKey{
			Category: tasks.CategoryTransfer,
		},
		PageSize: 1,
	})
	assert.ErrorContains(t, err, persistence.ErrMsgDeserializeRawHistoryTask,
		"ReadTasks should return ErrMsgDeserializeRawHistoryTask when the raw task cannot be deserialized"+
			" due to an error in the persistence layer")
}

func TestHistoryTaskQueueManager_ReadTasks_NonPositivePageSize(t *testing.T) {
	t.Parallel()

	m := persistence.NewHistoryTaskQueueManager(corruptQueue{}, serialization.NewSerializer())
	for _, pageSize := range []int{0, -1} {
		_, err := m.ReadTasks(context.Background(), &persistence.ReadTasksRequest{
			QueueKey: persistence.QueueKey{
				Category: tasks.Category{},
			},
			PageSize: pageSize,
		})
		assert.ErrorIs(t, err, persistence.ErrReadTasksNonPositivePageSize, "ReadTasks should return "+
			"ErrReadTasksNonPositivePageSize when the request's page size is: "+strconv.Itoa(pageSize))
	}
}
