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

package elasticsearch

import (
	"fmt"

	"github.com/golang/mock/gomock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
)

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStarted() {
	// test non-empty request fields match
	request := &persistence.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &persistence.InternalVisibilityRequestBase{
			NamespaceID:        "namespaceID",
			WorkflowID:         "wid",
			RunID:              "rid",
			WorkflowTypeName:   "wfType",
			StartTimestamp:     int64(123),
			ExecutionTimestamp: int64(321),
			TaskID:             int64(111),
			ShardID:            2208,
			Memo:               persistence.NewDataBlob([]byte("test bytes"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Status:             enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:          "task-queue-name",
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					searchattribute.CustomStringField: payload.EncodeString("alex"),
				},
			},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("2208~111", visibilityTaskKey)

			body := bulkRequest.Doc

			s.Equal(request.NamespaceID, body[searchattribute.NamespaceID])
			s.Equal(request.WorkflowID, body[searchattribute.WorkflowID])
			s.Equal(request.RunID, body[searchattribute.RunID])
			s.Equal(request.WorkflowTypeName, body[searchattribute.WorkflowType])
			s.EqualValues(request.StartTimestamp, body[searchattribute.StartTime])
			s.EqualValues(request.ExecutionTimestamp, body[searchattribute.ExecutionTime])
			s.Equal(request.TaskQueue, body[searchattribute.TaskQueue])
			s.EqualValues(request.Status, body[searchattribute.ExecutionStatus])

			s.Equal(request.Memo.Data, body[searchattribute.Memo])
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[searchattribute.Encoding])

			searchAttributes := body[searchattribute.Attr].(map[string]interface{})
			// %q because request has JSON encoded string.
			s.EqualValues(request.SearchAttributes.GetIndexedFields()[searchattribute.CustomStringField].Data, fmt.Sprintf("%q", searchAttributes[searchattribute.CustomStringField]))

			s.Equal(BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionStarted(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStarted_EmptyRequest() {
	// test empty request
	request := &persistence.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &persistence.InternalVisibilityRequestBase{
			Memo: &commonpb.DataBlob{},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("0~0", visibilityTaskKey)

			body := bulkRequest.Doc

			_, ok := body[searchattribute.Memo]
			s.False(ok)
			_, ok = body[searchattribute.Encoding]
			s.False(ok)

			s.Equal(BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionStarted(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosed() {
	// test non-empty request fields match
	request := &persistence.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &persistence.InternalVisibilityRequestBase{
			NamespaceID:        "namespaceID",
			WorkflowID:         "wid",
			RunID:              "rid",
			WorkflowTypeName:   "wfType",
			StartTimestamp:     int64(123),
			ExecutionTimestamp: int64(321),
			TaskID:             int64(111),
			ShardID:            2208,
			Memo:               persistence.NewDataBlob([]byte("test bytes"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Status:             enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			TaskQueue:          "task-queue-name",
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					searchattribute.CustomStringField: payload.EncodeString("alex"),
				},
			},
		},
		CloseTimestamp: int64(1978),
		HistoryLength:  int64(20),
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("2208~111", visibilityTaskKey)

			body := bulkRequest.Doc

			s.Equal(request.NamespaceID, body[searchattribute.NamespaceID])
			s.Equal(request.WorkflowID, body[searchattribute.WorkflowID])
			s.Equal(request.RunID, body[searchattribute.RunID])
			s.Equal(request.WorkflowTypeName, body[searchattribute.WorkflowType])
			s.EqualValues(request.StartTimestamp, body[searchattribute.StartTime])
			s.EqualValues(request.ExecutionTimestamp, body[searchattribute.ExecutionTime])
			s.Equal(request.Memo.Data, body[searchattribute.Memo])
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[searchattribute.Encoding])
			s.EqualValues(request.CloseTimestamp, body[searchattribute.CloseTime])
			s.EqualValues(request.Status, body[searchattribute.ExecutionStatus])
			s.EqualValues(request.HistoryLength, body[searchattribute.HistoryLength])

			s.Equal(BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionClosed(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosed_EmptyRequest() {
	// test empty request
	request := &persistence.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &persistence.InternalVisibilityRequestBase{
			Memo: &commonpb.DataBlob{},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("0~0", visibilityTaskKey)

			body := bulkRequest.Doc

			_, ok := body[searchattribute.Memo]
			s.False(ok)
			_, ok = body[searchattribute.Encoding]
			s.False(ok)

			s.Equal(BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionClosed(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestDeleteExecution() {
	// test non-empty request fields match
	request := &persistence.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: "namespaceID",
		RunID:       "rid",
		WorkflowID:  "wid",
		TaskID:      int64(111),
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("wid~rid", visibilityTaskKey)

			s.Equal(BulkableRequestTypeDelete, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.DeleteWorkflowExecution(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestDeleteExecution_EmptyRequest() {
	// test empty request
	request := &persistence.VisibilityDeleteWorkflowExecutionRequest{}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("~", visibilityTaskKey)

			s.Equal(BulkableRequestTypeDelete, bulkRequest.RequestType)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.DeleteWorkflowExecution(request)
	s.NoError(err)
}
