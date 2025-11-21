package elasticsearch

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStarted() {
	// test non-empty request fields match
	request := &store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID:      "namespaceID",
			WorkflowID:       "wid",
			RunID:            "rid",
			WorkflowTypeName: "wfType",
			StartTime:        time.Unix(0, 123).UTC(),
			ExecutionTime:    time.Unix(0, 321).UTC(),
			TaskID:           int64(111),
			ShardID:          2208,
			Memo:             persistence.NewDataBlob([]byte("test bytes"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        "task-queue-name",
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomTextField": payload.EncodeString("alex"),
				},
			},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any()).
		DoAndReturn(func(bulkRequest *client.BulkableRequest, visibilityTaskKey string) future.Future[bool] {
			s.Equal("2208~111", visibilityTaskKey)

			body := bulkRequest.Doc

			s.Equal(request.NamespaceID, body[sadefs.NamespaceID])
			s.Equal(request.WorkflowID, body[sadefs.WorkflowID])
			s.Equal(request.RunID, body[sadefs.RunID])
			s.Equal(request.WorkflowTypeName, body[sadefs.WorkflowType])
			s.EqualValues(request.StartTime, body[sadefs.StartTime])
			s.EqualValues(request.ExecutionTime, body[sadefs.ExecutionTime])
			s.Equal(request.TaskQueue, body[sadefs.TaskQueue])
			s.EqualValues(request.Status.String(), body[sadefs.ExecutionStatus])

			s.Equal(request.Memo.Data, body[sadefs.Memo])
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[sadefs.MemoEncoding])

			CustomTextField := body["CustomTextField"].(string)
			// %q because request has JSON encoded string.
			s.EqualValues(request.SearchAttributes.GetIndexedFields()["CustomTextField"].Data, fmt.Sprintf("%q", CustomTextField))

			s.Equal(client.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)

			f := future.NewFuture[bool]()
			f.Set(true, nil)
			return f
		})

	err := s.visibilityStore.RecordWorkflowExecutionStarted(context.Background(), request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStarted_EmptyRequest() {
	// test empty request
	request := &store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			Memo: &commonpb.DataBlob{},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any()).
		DoAndReturn(func(bulkRequest *client.BulkableRequest, visibilityTaskKey string) future.Future[bool] {
			s.Equal("0~0", visibilityTaskKey)

			body := bulkRequest.Doc

			_, ok := body[sadefs.Memo]
			s.False(ok)
			_, ok = body[sadefs.MemoEncoding]
			s.False(ok)

			s.Equal(client.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)

			f := future.NewFuture[bool]()
			f.Set(true, nil)
			return f
		})

	err := s.visibilityStore.RecordWorkflowExecutionStarted(context.Background(), request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosed() {
	// test non-empty request fields match
	request := &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID:      "namespaceID",
			WorkflowID:       "wid",
			RunID:            "rid",
			WorkflowTypeName: "wfType",
			StartTime:        time.Date(2020, 8, 2, 1, 2, 3, 4, time.UTC),
			ExecutionTime:    time.Date(2020, 8, 2, 2, 2, 3, 4, time.UTC),
			TaskID:           int64(111),
			ShardID:          2208,
			Memo:             persistence.NewDataBlob([]byte("test bytes"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			TaskQueue:        "task-queue-name",
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomTextField": payload.EncodeString("alex"),
				},
			},
		},
		CloseTime:     time.Unix(0, 1978).UTC(),
		HistoryLength: int64(20),
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any()).
		DoAndReturn(func(bulkRequest *client.BulkableRequest, visibilityTaskKey string) future.Future[bool] {
			s.Equal("2208~111", visibilityTaskKey)

			body := bulkRequest.Doc

			s.Equal(request.NamespaceID, body[sadefs.NamespaceID])
			s.Equal(request.WorkflowID, body[sadefs.WorkflowID])
			s.Equal(request.RunID, body[sadefs.RunID])
			s.Equal(request.WorkflowTypeName, body[sadefs.WorkflowType])
			s.EqualValues(request.StartTime, body[sadefs.StartTime])
			s.EqualValues(request.ExecutionTime, body[sadefs.ExecutionTime])
			s.Equal(request.Memo.Data, body[sadefs.Memo])
			s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), body[sadefs.MemoEncoding])
			s.EqualValues(request.CloseTime, body[sadefs.CloseTime])
			s.EqualValues(request.Status.String(), body[sadefs.ExecutionStatus])
			s.EqualValues(request.HistoryLength, body[sadefs.HistoryLength])

			s.Equal(client.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)

			f := future.NewFuture[bool]()
			f.Set(true, nil)
			return f
		})

	err := s.visibilityStore.RecordWorkflowExecutionClosed(context.Background(), request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosed_EmptyRequest() {
	// test empty request
	request := &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			Memo: &commonpb.DataBlob{},
		},
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any()).
		DoAndReturn(func(bulkRequest *client.BulkableRequest, visibilityTaskKey string) future.Future[bool] {
			s.Equal("0~0", visibilityTaskKey)

			body := bulkRequest.Doc

			_, ok := body[sadefs.Memo]
			s.False(ok)
			_, ok = body[sadefs.MemoEncoding]
			s.False(ok)

			s.Equal(client.BulkableRequestTypeIndex, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)

			f := future.NewFuture[bool]()
			f.Set(true, nil)
			return f
		})

	err := s.visibilityStore.RecordWorkflowExecutionClosed(context.Background(), request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestDeleteExecution() {
	// test non-empty request fields match
	request := &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: "namespaceID",
		RunID:       "rid",
		WorkflowID:  "wid",
		TaskID:      int64(111),
	}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any()).
		DoAndReturn(func(bulkRequest *client.BulkableRequest, visibilityTaskKey string) future.Future[bool] {
			s.Equal("wid~rid", visibilityTaskKey)

			s.Equal(client.BulkableRequestTypeDelete, bulkRequest.RequestType)
			s.EqualValues(request.TaskID, bulkRequest.Version)
			s.Equal("wid~rid", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)

			f := future.NewFuture[bool]()
			f.Set(true, nil)
			return f
		})

	err := s.visibilityStore.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestDeleteExecution_EmptyRequest() {
	// test empty request
	request := &manager.VisibilityDeleteWorkflowExecutionRequest{}

	s.mockProcessor.EXPECT().Add(gomock.Any(), gomock.Any()).
		DoAndReturn(func(bulkRequest *client.BulkableRequest, visibilityTaskKey string) future.Future[bool] {
			s.Equal("~", visibilityTaskKey)

			s.Equal(client.BulkableRequestTypeDelete, bulkRequest.RequestType)
			s.Equal("~", bulkRequest.ID)
			s.Equal("test-index", bulkRequest.Index)

			f := future.NewFuture[bool]()
			f.Set(true, nil)
			return f
		})

	err := s.visibilityStore.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) Test_GetDocID() {
	s.Equal("wid~rid", GetDocID("wid", "rid"))

	s.Equal(strings.Repeat("a", 512), GetDocID("wid", strings.Repeat("a", 1000)))
	s.Equal(strings.Repeat("a", 512), GetDocID("wid", strings.Repeat("a", 513)))
	s.Equal(strings.Repeat("a", 512), GetDocID("wid", strings.Repeat("a", 512)))
	s.Equal(strings.Repeat("a", 511), GetDocID("wid", strings.Repeat("a", 511)))
	s.Equal("w~"+strings.Repeat("a", 510), GetDocID("wid", strings.Repeat("a", 510)))
	s.Equal("wi~"+strings.Repeat("a", 509), GetDocID("wid", strings.Repeat("a", 509)))
	s.Equal("wid~"+strings.Repeat("a", 508), GetDocID("wid", strings.Repeat("a", 508)))
	s.Equal("wid~"+strings.Repeat("a", 507), GetDocID("wid", strings.Repeat("a", 507)))

	s.Equal(strings.Repeat("a", 512-1-36)+"~fd86a520-741e-4fd3-a788-165c445ea6f3", GetDocID(strings.Repeat("a", 1000), "fd86a520-741e-4fd3-a788-165c445ea6f3"))
	s.Equal(strings.Repeat("a", 512-1-36)+"~fd86a520-741e-4fd3-a788-165c445ea6f3", GetDocID(strings.Repeat("a", 477), "fd86a520-741e-4fd3-a788-165c445ea6f3"))
	s.Equal(strings.Repeat("a", 512-1-36)+"~fd86a520-741e-4fd3-a788-165c445ea6f3", GetDocID(strings.Repeat("a", 476), "fd86a520-741e-4fd3-a788-165c445ea6f3"))
	s.Equal(strings.Repeat("a", 475)+"~fd86a520-741e-4fd3-a788-165c445ea6f3", GetDocID(strings.Repeat("a", 475), "fd86a520-741e-4fd3-a788-165c445ea6f3"))
	s.Equal(strings.Repeat("a", 474)+"~fd86a520-741e-4fd3-a788-165c445ea6f3", GetDocID(strings.Repeat("a", 474), "fd86a520-741e-4fd3-a788-165c445ea6f3"))
	s.Equal(strings.Repeat("a", 400)+"~fd86a520-741e-4fd3-a788-165c445ea6f3", GetDocID(strings.Repeat("a", 400), "fd86a520-741e-4fd3-a788-165c445ea6f3"))

	// construct a workflowID that contains valid utf8 prefix with multi-bytes unicode.
	// the prefix length is exactly that it will cut on the next multi-bytes unicode.
	// this test case is to verify that we don't produce invalid docID that consists of invalid utf8 string
	rid := "rid"
	prefix := strings.Repeat("a", 512-len(rid)-len(delimiter)-1)
	wid := prefix + "中文字符"
	docId := GetDocID(wid, rid)
	s.True(utf8.ValidString(docId))
	s.Equal(prefix+"~rid", docId)
}

func (s *ESVisibilitySuite) Test_GetVisibilityTaskKey() {
	s.Equal("22~8", GetVisibilityTaskKey(22, 8))
	s.Equal("228~1978", GetVisibilityTaskKey(228, 1978))
}
