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

package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	// clusterNamesHashSuffixLength is the number of characters to use from the hash of the cluster names when forming
	// the queue name. This is used to avoid name collisions when a cluster name contains the separator character.
	clusterNamesHashSuffixLength = 8

	ErrMsgSerializeTaskToEnqueue = "failed to serialize history task for task queue"
	// ErrMsgDeserializeRawHistoryTask is returned when the raw task cannot be deserialized from the task queue. This error
	// is returned when this whole top-level proto cannot be deserialized.
	//  Raw Task (a proto): <-- when this cannot be deserialized
	//	- ShardID
	//	- Blob (a serialized task)
	ErrMsgDeserializeRawHistoryTask = "failed to deserialize raw history task from task queue"
	// ErrMsgDeserializeHistoryTask is returned when the history task cannot be deserialized from the task queue. This
	// error is returned when the blob inside the raw task cannot be deserialized.
	//  Raw Task (a proto):
	//	- ShardID
	//	- Blob (a serialized task) <-- when this cannot be deserialized
	ErrMsgDeserializeHistoryTask = "failed to deserialize history task blob"
)

var (
	ErrReadTasksNonPositivePageSize = errors.New("page size to read history tasks must be positive")
	ErrHistoryTaskBlobIsNil         = errors.New("history task from queue has nil blob")
	ErrEnqueueTaskRequestTaskIsNil  = errors.New("enqueue task request task is nil")
	ErrQueueAlreadyExists           = errors.New("queue already exists")
	ErrShardIDInvalid               = errors.New("shard ID must be greater than 0")
)

func NewHistoryTaskQueueManager(queue QueueV2, serializer serialization.Serializer) *HistoryTaskQueueManagerImpl {
	return &HistoryTaskQueueManagerImpl{
		queue:      queue,
		serializer: serializer,
	}
}

func (m *HistoryTaskQueueManagerImpl) EnqueueTask(
	ctx context.Context,
	request *EnqueueTaskRequest,
) (*EnqueueTaskResponse, error) {
	if request.Task == nil {
		return nil, ErrEnqueueTaskRequestTaskIsNil
	}
	blob, err := m.serializer.SerializeTask(request.Task)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrMsgSerializeTaskToEnqueue, err)
	}
	if request.SourceShardID <= 0 {
		return nil, fmt.Errorf("%w: shardID = %d", ErrShardIDInvalid, request.SourceShardID)
	}

	taskCategory := request.Task.GetCategory()
	task := persistencespb.HistoryTask{
		ShardId: int32(request.SourceShardID),
		Blob:    blob,
	}
	taskBytes, _ := task.Marshal()
	blob = &commonpb.DataBlob{
		EncodingType: enums.ENCODING_TYPE_PROTO3,
		Data:         taskBytes,
	}
	queueKey := QueueKey{
		QueueType:     request.QueueType,
		Category:      taskCategory,
		SourceCluster: request.SourceCluster,
		TargetCluster: request.TargetCluster,
	}

	message, err := m.queue.EnqueueMessage(ctx, &InternalEnqueueMessageRequest{
		QueueType: request.QueueType,
		QueueName: queueKey.GetQueueName(),
		Blob:      blob,
	})
	if err != nil {
		return nil, err
	}

	return &EnqueueTaskResponse{
		Metadata: message.Metadata,
	}, nil
}

// ReadRawTasks returns a page of "raw" tasks from the queue. Here's a quick disambiguation of the different types of
// tasks:
//
//   - [go.temporal.io/server/api/history/v1.Task]: the proto that is serialized and stored in the database which
//     contains a shard ID and a blob of the serialized history task. This is also called a "raw" task.
//   - [go.temporal.io/server/service/history/tasks.Task]: the interface that is implemented by all history tasks.
//     This is the primary type used in code to represent a history task since it is the most structured.
func (m *HistoryTaskQueueManagerImpl) ReadRawTasks(
	ctx context.Context,
	request *ReadRawTasksRequest,
) (*ReadRawTasksResponse, error) {
	if request.PageSize <= 0 {
		return nil, fmt.Errorf("%w: %v", ErrReadTasksNonPositivePageSize, request.PageSize)
	}

	response, err := m.queue.ReadMessages(ctx, &InternalReadMessagesRequest{
		QueueType:     request.QueueKey.QueueType,
		QueueName:     request.QueueKey.GetQueueName(),
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
	})
	if err != nil {
		return nil, err
	}

	responseTasks := make([]RawHistoryTask, len(response.Messages))
	for i, message := range response.Messages {
		var task persistencespb.HistoryTask
		err := serialization.Proto3Decode(message.Data.Data, message.Data.EncodingType, &task)
		if err != nil {
			return nil, fmt.Errorf("%v: %w", ErrMsgDeserializeRawHistoryTask, err)
		}
		responseTasks[i].MessageMetadata = message.MetaData
		responseTasks[i].Payload = &task
	}

	return &ReadRawTasksResponse{
		Tasks:         responseTasks,
		NextPageToken: response.NextPageToken,
	}, nil
}

// ReadTasks is a convenience method on top of ReadRawTasks that deserializes the tasks into the [tasks.Task] type.
func (m *HistoryTaskQueueManagerImpl) ReadTasks(ctx context.Context, request *ReadTasksRequest) (*ReadTasksResponse, error) {
	response, err := m.ReadRawTasks(ctx, request)
	if err != nil {
		return nil, err
	}

	resTasks := make([]HistoryTask, len(response.Tasks))

	for i, rawTask := range response.Tasks {
		blob := rawTask.Payload.Blob
		if blob == nil {
			return nil, serialization.NewDeserializationError(enums.ENCODING_TYPE_PROTO3, ErrHistoryTaskBlobIsNil)
		}

		task, err := m.serializer.DeserializeTask(request.QueueKey.Category, blob)
		if err != nil {
			return nil, fmt.Errorf("%v: %w", ErrMsgDeserializeHistoryTask, err)
		}

		resTasks[i] = HistoryTask{
			MessageMetadata: rawTask.MessageMetadata,
			Task:            task,
		}
	}

	return &ReadTasksResponse{
		Tasks:         resTasks,
		NextPageToken: response.NextPageToken,
	}, nil
}

func (m *HistoryTaskQueueManagerImpl) CreateQueue(
	ctx context.Context,
	request *CreateQueueRequest,
) (*CreateQueueResponse, error) {
	_, err := m.queue.CreateQueue(ctx, &InternalCreateQueueRequest{
		QueueType: request.QueueKey.QueueType,
		QueueName: request.QueueKey.GetQueueName(),
	})
	if err != nil {
		return nil, err
	}
	return &CreateQueueResponse{}, nil
}

func (m *HistoryTaskQueueManagerImpl) DeleteTasks(
	ctx context.Context,
	request *DeleteTasksRequest,
) (*DeleteTasksResponse, error) {
	resp, err := m.queue.RangeDeleteMessages(ctx, &InternalRangeDeleteMessagesRequest{
		QueueType:                   request.QueueKey.QueueType,
		QueueName:                   request.QueueKey.GetQueueName(),
		InclusiveMaxMessageMetadata: request.InclusiveMaxMessageMetadata,
	})
	if err != nil {
		return nil, err
	}
	return &DeleteTasksResponse{MessagesDeleted: resp.MessagesDeleted}, nil
}

func (m HistoryTaskQueueManagerImpl) ListQueues(
	ctx context.Context,
	request *ListQueuesRequest,
) (*ListQueuesResponse, error) {
	resp, err := m.queue.ListQueues(ctx, &InternalListQueuesRequest{
		QueueType:     request.QueueType,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
	})
	if err != nil {
		return nil, err
	}
	return &ListQueuesResponse{
		Queues:        resp.Queues,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (m HistoryTaskQueueManagerImpl) Close() {
}

// combineUnique combines the given strings into a single string by hashing the length of each string and the string
// itself. This is used to generate a unique suffix for the queue name.
func combineUnique(strs ...string) string {
	h := sha256.New()
	for _, str := range strs {
		b := sha256.Sum256([]byte(str))
		_, _ = h.Write(b[:])
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func (k QueueKey) GetQueueName() string {
	return GetHistoryTaskQueueName(k.Category.ID(), k.SourceCluster, k.TargetCluster)
}

func GetHistoryTaskQueueName(
	categoryID int,
	sourceCluster string,
	targetCluster string,
) string {
	hash := combineUnique(sourceCluster, targetCluster)[:clusterNamesHashSuffixLength]
	return fmt.Sprintf("%d_%s_%s_%s", categoryID, sourceCluster, targetCluster, hash)
}
