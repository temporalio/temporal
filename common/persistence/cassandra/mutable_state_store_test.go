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

package cassandra

import (
	"context"
	"testing"

	cassandra_gocql "github.com/gocql/gocql"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type mutableStateStoreSuite struct {
	suite.Suite
	controller        *gomock.Controller
	mockSession       *gocql.MockSession
	mutableStateStore *MutableStateStore
}

func TestMutableStateStoreSuite(t *testing.T) {
	s := new(mutableStateStoreSuite)
	suite.Run(t, s)
}

func (s *mutableStateStoreSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockSession = gocql.NewMockSession(s.controller)
	s.mutableStateStore = NewMutableStateStore(s.mockSession)
}

func (s *mutableStateStoreSuite) TestGetWorkflowExecution_WithRunID() {
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     1234,
		RunID:       "TEST-RUN-ID",
		NamespaceID: "TEST-NAMESPACE-ID",
		WorkflowID:  "TEST-WORKFLOW-ID",
	}

	// ensure that current execution query is NOT called.
	s.mockSession.EXPECT().Query(templateGetCurrentExecutionQuery, gomock.Any()).Times(0)

	// get execution data query
	mockGetWorkflowExecutionQuery := gocql.NewMockQuery(s.controller)
	mockGetWorkflowExecutionQuery.EXPECT().WithContext(gomock.Any()).Return(mockGetWorkflowExecutionQuery).AnyTimes()
	mockGetWorkflowExecutionQuery.EXPECT().MapScan(gomock.Any()).DoAndReturn(
		func(res map[string]interface{}) error {
			setTestExecutionStoreData(res)
			return nil
		}).Times(1)

	// mock get workflow execution query to return test data.
	s.mockSession.EXPECT().Query(
		templateGetWorkflowExecutionQuery,
		request.ShardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
	).Return(mockGetWorkflowExecutionQuery).Times(1)

	resp, err := s.mutableStateStore.GetWorkflowExecution(context.Background(), request)
	s.NoError(err)
	s.NotNil(resp)
}

func (s *mutableStateStoreSuite) TestGetWorkflowExecution_WithoutRunID() {
	// current run ID query
	mockCurrentRunIDQuery := gocql.NewMockQuery(s.controller)
	mockCurrentRunIDQuery.EXPECT().WithContext(gomock.Any()).Return(mockCurrentRunIDQuery).AnyTimes()
	mockCurrentRunIDQuery.EXPECT().MapScan(gomock.Any()).Do(
		func(res map[string]interface{}) error {
			res["current_run_id"] = cassandra_gocql.MustRandomUUID()
			return nil
		})
	// mock get current execution query (to get current run ID)
	s.mockSession.EXPECT().Query(templateGetCurrentExecutionQuery, gomock.Any()).Return(mockCurrentRunIDQuery).Times(1)

	// get execution data query
	mockGetWorkflowExecutionQuery := gocql.NewMockQuery(s.controller)
	mockGetWorkflowExecutionQuery.EXPECT().WithContext(gomock.Any()).Return(mockGetWorkflowExecutionQuery).AnyTimes()
	mockGetWorkflowExecutionQuery.EXPECT().MapScan(gomock.Any()).DoAndReturn(
		func(res map[string]interface{}) error {
			setTestExecutionStoreData(res)
			return nil
		}).Times(1)
	// mock get workflow execution query to return test data.
	s.mockSession.EXPECT().Query(templateGetWorkflowExecutionQuery, gomock.Any()).Return(mockGetWorkflowExecutionQuery).Times(1)

	resp, err := s.mutableStateStore.GetWorkflowExecution(context.Background(), &persistence.GetWorkflowExecutionRequest{})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *mutableStateStoreSuite) TestGetWorkflowExecution_CurrentRunIDNotFound() {
	// current run ID query. Return not found in MapScan.
	mockCurrentRunIDQuery := gocql.NewMockQuery(s.controller)
	mockCurrentRunIDQuery.EXPECT().WithContext(gomock.Any()).Return(mockCurrentRunIDQuery).AnyTimes()
	mockCurrentRunIDQuery.EXPECT().MapScan(gomock.Any()).Return(cassandra_gocql.ErrNotFound).Times(1)

	// mock get current execution query (to get current run ID)
	s.mockSession.EXPECT().Query(templateGetCurrentExecutionQuery, gomock.Any()).Return(mockCurrentRunIDQuery).Times(1)

	// ensure that get workflow execution query is NOT called.
	s.mockSession.EXPECT().Query(templateGetWorkflowExecutionQuery, gomock.Any()).Times(0)

	resp, err := s.mutableStateStore.GetWorkflowExecution(context.Background(), &persistence.GetWorkflowExecutionRequest{})
	s.ErrorContains(err, "operation GetWorkflowExecution encountered not found")
	s.Nil(resp)
}

func setTestExecutionStoreData(res map[string]interface{}) {
	res["execution"] = []byte{}
	res["execution_encoding"] = "ENCODING_TYPE_PROTO3"
	res["next_event_id"] = int64(1111)
	res["execution_state"] = []byte{}
	res["execution_state_encoding"] = "ENCODING_TYPE_PROTO3"
	res["activity_map"] = map[int64][]byte{}
	res["activity_map_encoding"] = "ENCODING_TYPE_PROTO3"
	res["timer_map"] = map[string][]byte{}
	res["timer_map_encoding"] = "ENCODING_TYPE_PROTO3"
	res["child_executions_map"] = map[int64][]byte{}
	res["child_executions_map_encoding"] = "ENCODING_TYPE_PROTO3"
	res["request_cancel_map"] = map[int64][]byte{}
	res["request_cancel_map_encoding"] = "ENCODING_TYPE_PROTO3"
	res["signal_map"] = map[int64][]byte{}
	res["signal_map_encoding"] = "ENCODING_TYPE_PROTO3"
	res["signal_requested"] = []cassandra_gocql.UUID{}
	res["buffered_events_list"] = []map[string]interface{}{}
	res["checksum"] = []byte{}
	res["checksum_encoding"] = "ENCODING_TYPE_PROTO3"
	res["db_record_version"] = int64(2222)
}
