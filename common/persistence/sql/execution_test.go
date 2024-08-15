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

package sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type sqlExecutionStoreSuite struct {
	suite.Suite
	controller       *gomock.Controller
	mockDB           *sqlplugin.MockDB
	sqlExecuionStore *sqlExecutionStore

	testShardID     int32
	testRunID       primitives.UUID
	testNamespaceID primitives.UUID
	testWorkflowID  primitives.UUID
}

// TestSQLExecutionStoreSuite tests the SQL execution store methods.
func TestSQLExecutionStoreSuite(t *testing.T) {
	s := new(sqlExecutionStoreSuite)
	suite.Run(t, s)
}

func (s *sqlExecutionStoreSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockDB = sqlplugin.NewMockDB(s.controller)
	sqlStore, err := NewSQLExecutionStore(s.mockDB, nil)
	s.NoError(err)
	s.sqlExecuionStore = sqlStore
	s.testShardID = int32(1234)
	s.testRunID = primitives.NewUUID()
	s.testNamespaceID = primitives.NewUUID()
	s.testWorkflowID = primitives.NewUUID()

	// Record some default return values.
	s.mockDB.EXPECT().SelectAllFromActivityInfoMaps(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockDB.EXPECT().SelectAllFromTimerInfoMaps(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockDB.EXPECT().SelectAllFromChildExecutionInfoMaps(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockDB.EXPECT().SelectAllFromRequestCancelInfoMaps(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockDB.EXPECT().SelectAllFromSignalInfoMaps(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockDB.EXPECT().SelectFromBufferedEvents(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockDB.EXPECT().SelectAllFromSignalsRequestedSets(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
}

func (s *sqlExecutionStoreSuite) TestGetWorkflowExecution_WithRunID() {
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.testShardID,
		NamespaceID: s.testNamespaceID.String(),
		WorkflowID:  s.testWorkflowID.String(),
		RunID:       s.testRunID.String(),
	}
	// Ensure that SelectFromCurrentExecutions is not called when a runID is provided.
	s.mockDB.EXPECT().SelectFromCurrentExecutions(gomock.Any(), gomock.Any()).Times(0)

	// mock SelectFromExecutions query.
	s.mockDB.EXPECT().SelectFromExecutions(gomock.Any(), sqlplugin.ExecutionsFilter{
		ShardID:     s.testShardID,
		NamespaceID: s.testNamespaceID,
		WorkflowID:  s.testWorkflowID.String(),
		RunID:       s.testRunID,
	}).Return(newTestExecutionsRow(), nil).Times(1)

	resp, err := s.sqlExecuionStore.GetWorkflowExecution(context.Background(), request)
	s.NoError(err)
	s.NotNil(resp)
}

func (s *sqlExecutionStoreSuite) TestGetWorkflowExecution_WithoutRunID() {
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.testShardID,
		NamespaceID: s.testNamespaceID.String(),
		WorkflowID:  s.testWorkflowID.String(),
	}
	mockedCurrentRunID := primitives.NewUUID()

	// mock SelectFromCurrentExecutions to return a currentRunID.
	s.mockDB.EXPECT().
		SelectFromCurrentExecutions(gomock.Any(), sqlplugin.CurrentExecutionsFilter{
			ShardID:     s.testShardID,
			NamespaceID: s.testNamespaceID,
			WorkflowID:  s.testWorkflowID.String(),
		}).
		Return(&sqlplugin.CurrentExecutionsRow{RunID: mockedCurrentRunID}, nil).
		Times(1)

	// mock SelectFromExecutions query.
	s.mockDB.EXPECT().SelectFromExecutions(gomock.Any(), sqlplugin.ExecutionsFilter{
		ShardID:     s.testShardID,
		NamespaceID: s.testNamespaceID,
		WorkflowID:  s.testWorkflowID.String(),
		RunID:       mockedCurrentRunID, // ensure that the newly fetched runID is passed to SelectFromExecutions()
	}).Return(newTestExecutionsRow(), nil).Times(1)

	resp, err := s.sqlExecuionStore.GetWorkflowExecution(context.Background(), request)
	s.NoError(err)
	s.NotNil(resp)
}

func (s *sqlExecutionStoreSuite) TestGetWorkflowExecution_CurrentRunIDNotFound() {
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.testShardID,
		NamespaceID: s.testNamespaceID.String(),
		WorkflowID:  s.testWorkflowID.String(),
	}

	// mock SelectFromCurrentExecutions to return a not-found error
	s.mockDB.EXPECT().
		SelectFromCurrentExecutions(gomock.Any(), sqlplugin.CurrentExecutionsFilter{
			ShardID:     s.testShardID,
			NamespaceID: s.testNamespaceID,
			WorkflowID:  s.testWorkflowID.String(),
		}).
		Return(nil, sql.ErrNoRows).
		Times(1)

	// Ensure that SelectFromExecutions is not called when a current runID is not found.
	s.mockDB.EXPECT().SelectFromExecutions(gomock.Any(), gomock.Any()).Times(0)

	resp, err := s.sqlExecuionStore.GetWorkflowExecution(context.Background(), request)
	notFoundErr := &serviceerror.NotFound{}
	s.ErrorAs(err, &notFoundErr)
	s.Nil(resp)
}

func newTestExecutionsRow() *sqlplugin.ExecutionsRow {
	return &sqlplugin.ExecutionsRow{
		ShardID:          1234,
		NamespaceID:      []byte{},
		WorkflowID:       "",
		RunID:            []byte{},
		NextEventID:      0,
		LastWriteVersion: 0,
		Data:             []byte{},
		DataEncoding:     "",
		State:            []byte{},
		StateEncoding:    "",
		DBRecordVersion:  0,
	}
}
