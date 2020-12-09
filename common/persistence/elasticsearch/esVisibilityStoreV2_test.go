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
	"encoding/base64"
	"encoding/json"

	"github.com/golang/mock/gomock"
	"github.com/olivere/elastic"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/definition"
	es "go.temporal.io/server/common/elasticsearch"
	p "go.temporal.io/server/common/persistence"
)

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStartedV2() {
	// test non-empty request fields match
	request := &p.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &p.InternalVisibilityRequestBase{},
	}
	request.NamespaceID = "namespaceID"
	request.WorkflowID = "wid"
	request.RunID = "rid"
	request.WorkflowTypeName = "wfType"
	request.StartTimestamp = int64(123)
	request.ExecutionTimestamp = int64(321)
	request.TaskID = int64(111)
	request.ShardID = 2208
	memoBytes := []byte(`test bytes`)
	request.Memo = p.NewDataBlob(memoBytes, enumspb.ENCODING_TYPE_PROTO3.String())

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest elastic.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("2208~111", visibilityTaskKey)

			req, err := bulkRequest.Source()
			s.NoError(err)
			var body map[string]interface{}
			err = json.Unmarshal([]byte(req[1]), &body)
			s.NoError(err)

			s.Equal(request.NamespaceID, body[definition.NamespaceID])
			s.Equal(request.WorkflowID, body[definition.WorkflowID])
			s.Equal(request.RunID, body[definition.RunID])
			s.Equal(request.WorkflowTypeName, body[definition.WorkflowType])
			s.Equal(request.StartTimestamp, int64(body[definition.StartTime].(float64)))
			s.Equal(request.ExecutionTimestamp, int64(body[definition.ExecutionTime].(float64)))
			memoFromBody, err := base64.StdEncoding.DecodeString(body[definition.Memo].(string))
			s.NoError(err)
			s.Equal(memoBytes, memoFromBody)
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[definition.Encoding])

			var opBody map[string]map[string]interface{}
			err = json.Unmarshal([]byte(req[0]), &opBody)
			s.NoError(err)
			opMap := opBody["index"]
			s.Equal(request.TaskID, int64(opMap["version"].(float64)))
			s.Equal(versionTypeExternal, opMap["version_type"])
			s.Equal(docType, opMap["_type"])
			s.Equal("wid~rid", opMap["_id"])
			s.Equal("test-index", opMap["_index"])
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
		Do(func(bulkRequest elastic.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("0~0", visibilityTaskKey)

			req, err := bulkRequest.Source()
			s.NoError(err)
			var body map[string]interface{}
			err = json.Unmarshal([]byte(req[1]), &body)
			s.NoError(err)

			_, ok := body[es.Memo]
			s.False(ok)
			_, ok = body[es.Encoding]
			s.False(ok)

			var opBody map[string]map[string]interface{}
			err = json.Unmarshal([]byte(req[0]), &opBody)
			s.NoError(err)
			opMap := opBody["index"]
			s.Equal(request.TaskID, int64(opMap["version"].(float64)))
			s.Equal(versionTypeExternal, opMap["version_type"])
			s.Equal(docType, opMap["_type"])
			s.Equal("~", opMap["_id"])
			s.Equal("test-index", opMap["_index"])
		})

	err := s.visibilityStore.RecordWorkflowExecutionStartedV2(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosedV2() {
	// test non-empty request fields match
	request := &p.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &p.InternalVisibilityRequestBase{},
	}
	request.NamespaceID = "namespaceID"
	request.WorkflowID = "wid"
	request.RunID = "rid"
	request.WorkflowTypeName = "wfType"
	request.StartTimestamp = int64(123)
	request.ExecutionTimestamp = int64(321)
	request.TaskID = int64(111)
	request.ShardID = 2208
	memoBytes := []byte(`test bytes`)
	request.Memo = p.NewDataBlob(memoBytes, enumspb.ENCODING_TYPE_PROTO3.String())
	request.CloseTimestamp = int64(999)
	request.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	request.HistoryLength = int64(20)

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bulkRequest elastic.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("2208~111", visibilityTaskKey)

			req, err := bulkRequest.Source()
			s.NoError(err)
			var body map[string]interface{}
			err = json.Unmarshal([]byte(req[1]), &body)
			s.NoError(err)

			s.Equal(request.NamespaceID, body[definition.NamespaceID])
			s.Equal(request.WorkflowID, body[definition.WorkflowID])
			s.Equal(request.RunID, body[definition.RunID])
			s.Equal(request.WorkflowTypeName, body[definition.WorkflowType])
			s.Equal(request.StartTimestamp, int64(body[definition.StartTime].(float64)))
			s.Equal(request.ExecutionTimestamp, int64(body[definition.ExecutionTime].(float64)))
			memoFromBody, err := base64.StdEncoding.DecodeString(body[definition.Memo].(string))
			s.NoError(err)
			s.Equal(memoBytes, memoFromBody)
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[definition.Encoding])
			s.Equal(request.CloseTimestamp, int64(body[definition.CloseTime].(float64)))
			s.EqualValues(request.Status, body[definition.ExecutionStatus])
			s.Equal(request.HistoryLength, int64(body[definition.HistoryLength].(float64)))

			var opBody map[string]map[string]interface{}
			err = json.Unmarshal([]byte(req[0]), &opBody)
			s.NoError(err)
			opMap := opBody["index"]
			s.Equal(request.TaskID, int64(opMap["version"].(float64)))
			s.Equal(versionTypeExternal, opMap["version_type"])
			s.Equal(docType, opMap["_type"])
			s.Equal("wid~rid", opMap["_id"])
			s.Equal("test-index", opMap["_index"])
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
		Do(func(bulkRequest elastic.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
			s.NotNil(ackCh)
			s.Equal(1, cap(ackCh))
			s.Equal(0, len(ackCh))
			ackCh <- true

			s.Equal("0~0", visibilityTaskKey)

			req, err := bulkRequest.Source()
			s.NoError(err)
			var body map[string]interface{}
			err = json.Unmarshal([]byte(req[1]), &body)
			s.NoError(err)

			_, ok := body[es.Memo]
			s.False(ok)
			_, ok = body[es.Encoding]
			s.False(ok)

			var opBody map[string]map[string]interface{}
			err = json.Unmarshal([]byte(req[0]), &opBody)
			s.NoError(err)
			opMap := opBody["index"]
			s.Equal(request.TaskID, int64(opMap["version"].(float64)))
			s.Equal(versionTypeExternal, opMap["version_type"])
			s.Equal(docType, opMap["_type"])
			s.Equal("~", opMap["_id"])
			s.Equal("test-index", opMap["_index"])
		})

	err := s.visibilityStore.RecordWorkflowExecutionClosedV2(request)
	s.NoError(err)
}
