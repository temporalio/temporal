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

	"go.temporal.io/server/common/definition"
	es "go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/payload"
	p "go.temporal.io/server/common/persistence"
)

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStartedV2() {
	// test non-empty request fields match
	request := &p.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &p.InternalVisibilityRequestBase{
			NamespaceID:        "namespaceID",
			WorkflowID:         "wid",
			RunID:              "rid",
			WorkflowTypeName:   "wfType",
			StartTimestamp:     int64(123),
			ExecutionTimestamp: int64(321),
			TaskID:             int64(111),
			ShardID:            2208,
			Memo:               p.NewDataBlob([]byte("test bytes"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Status:             enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:          "task-queue-name",
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					definition.CustomStringField: payload.EncodeString("alex"),
				},
			},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *es.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("2208~111", visibilityTaskKey)

			body := bulkRequest.Doc

			s.Equal(request.NamespaceID, body[definition.NamespaceID])
			s.Equal(request.WorkflowID, body[definition.WorkflowID])
			s.Equal(request.RunID, body[definition.RunID])
			s.Equal(request.WorkflowTypeName, body[definition.WorkflowType])
			s.EqualValues(request.StartTimestamp, body[definition.StartTime])
			s.EqualValues(request.ExecutionTimestamp, body[definition.ExecutionTime])
			s.Equal(request.TaskQueue, body[definition.TaskQueue])
			s.EqualValues(request.Status, body[definition.ExecutionStatus])

			s.Equal(request.Memo.Data, body[definition.Memo])
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[definition.Encoding])

			searchAttributes := body[definition.Attr].(map[string]interface{})
			// %q because request has JSON encoded string.
			s.EqualValues(request.SearchAttributes.GetIndexedFields()[definition.CustomStringField].Data, fmt.Sprintf("%q", searchAttributes[definition.CustomStringField]))

			s.Equal(es.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionStartedV2(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStartedV2_EmptyRequest() {
	// test empty request
	request := &p.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &p.InternalVisibilityRequestBase{
			Memo: &commonpb.DataBlob{},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *es.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("0~0", visibilityTaskKey)

			body := bulkRequest.Doc

			_, ok := body[es.Memo]
			s.False(ok)
			_, ok = body[es.Encoding]
			s.False(ok)

			s.Equal(es.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionStartedV2(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosedV2() {
	// test non-empty request fields match
	request := &p.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &p.InternalVisibilityRequestBase{
			NamespaceID:        "namespaceID",
			WorkflowID:         "wid",
			RunID:              "rid",
			WorkflowTypeName:   "wfType",
			StartTimestamp:     int64(123),
			ExecutionTimestamp: int64(321),
			TaskID:             int64(111),
			ShardID:            2208,
			Memo:               p.NewDataBlob([]byte("test bytes"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Status:             enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			TaskQueue:          "task-queue-name",
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					definition.CustomStringField: payload.EncodeString("alex"),
				},
			},
		},
		CloseTimestamp: int64(1978),
		HistoryLength:  int64(20),
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *es.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("2208~111", visibilityTaskKey)

			body := bulkRequest.Doc

			s.Equal(request.NamespaceID, body[definition.NamespaceID])
			s.Equal(request.WorkflowID, body[definition.WorkflowID])
			s.Equal(request.RunID, body[definition.RunID])
			s.Equal(request.WorkflowTypeName, body[definition.WorkflowType])
			s.EqualValues(request.StartTimestamp, body[definition.StartTime])
			s.EqualValues(request.ExecutionTimestamp, body[definition.ExecutionTime])
			s.Equal(request.Memo.Data, body[definition.Memo])
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[definition.Encoding])
			s.EqualValues(request.CloseTimestamp, body[definition.CloseTime])
			s.EqualValues(request.Status, body[definition.ExecutionStatus])
			s.EqualValues(request.HistoryLength, body[definition.HistoryLength])

			s.Equal(es.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionClosedV2(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosedV2_EmptyRequest() {
	// test empty request
	request := &p.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &p.InternalVisibilityRequestBase{
			Memo: &commonpb.DataBlob{},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *es.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("0~0", visibilityTaskKey)

			body := bulkRequest.Doc

			_, ok := body[es.Memo]
			s.False(ok)
			_, ok = body[es.Encoding]
			s.False(ok)

			s.Equal(es.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.RecordWorkflowExecutionClosedV2(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestDeleteExecutionV2() {
	// test non-empty request fields match
	request := &p.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: "namespaceID",
		RunID:       "rid",
		WorkflowID:  "wid",
		TaskID:      int64(111),
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *es.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("wid~rid", visibilityTaskKey)

			s.Equal(es.BulkableRequestTypeDelete, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.DeleteWorkflowExecutionV2(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestDeleteExecutionV2_EmptyRequest() {
	// test empty request
	request := &p.VisibilityDeleteWorkflowExecutionRequest{}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest *es.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("~", visibilityTaskKey)

			s.Equal(es.BulkableRequestTypeDelete, bulkRequest.RequestType)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)
		})

	err := s.visibilityStore.DeleteWorkflowExecutionV2(request)
	s.NoError(err)
}
