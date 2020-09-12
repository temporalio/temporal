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
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	protoConverterSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestProtoConverterSuite(t *testing.T) {
	s := new(protoConverterSuite)
	suite.Run(t, s)
}

func (s *protoConverterSuite) SetupSuite() {
}

func (s *protoConverterSuite) TearDownSuite() {

}

func (s *protoConverterSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *protoConverterSuite) TearDownTest() {

}

func (s *protoConverterSuite) TestConvertWorkflowExecution_Success() {
	namespaceID := uuid.New()
	workflowID := "convert-workflow-test-success"
	runID := "3969fae6-6b75-4c2a-b74b-4054edd29612"
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(17)
	startVersion := int64(333)
	stats := &persistenceblobs.ExecutionStats{HistorySize: 127}
	vh := &history.VersionHistories{
		CurrentVersionHistoryIndex: 18,
		Histories:                  nil,
	}
	wei := &WorkflowExecutionInfo{
		NamespaceId:                namespaceID,
		WorkflowId:                 workflowID,
		TaskQueue:                  taskqueue,
		WorkflowTypeName:           workflowType,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		LastFirstEventId:           common.FirstEventID,
		LastProcessedEvent:         lastProcessedEventID,
		ExecutionState: &persistenceblobs.WorkflowExecutionState{
			RunId:                      runID,
			CreateRequestId:            uuid.New(),
			State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		ExecutionStats: stats,
	}

	protoWei, protoState, err := WorkflowExecutionToProto(wei, startVersion, vh)
	s.NoError(err)
	s.NotNil(protoWei)
	s.NotNil(protoState)
	s.Equal(protoWei.VersionHistories, vh)

	weiBytes, err := serialization.WorkflowExecutionInfoToBlob(protoWei)
	s.NoError(err)
	stateBytes, err := serialization.WorkflowExecutionStateToBlob(protoState)
	s.NoError(err)

	newProtoWei, err := serialization.WorkflowExecutionInfoFromBlob(weiBytes.Data, weiBytes.Encoding.String())
	s.NoError(err)

	newProtoState, err := serialization.WorkflowExecutionStateFromBlob(stateBytes.Data, stateBytes.Encoding.String())
	s.NoError(err)

	newNextEventId := int64(10)
	decodedWei := WorkflowExecutionFromProto(newProtoWei, newProtoState, newNextEventId)
	s.EqualValues(newNextEventId, decodedWei.NextEventId)
	wei.NextEventId = newNextEventId
	s.EqualValues(wei, decodedWei)
}

func (s *protoConverterSuite) TestConvertWorkflowExecution_HistorySizeMigration() {
	namespaceID := uuid.New()
	workflowID := "convert-workflow-test-success"
	runID := "3969fae6-6b75-4c2a-b74b-4054edd29612"
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(17)
	startVersion := int64(333)
	stats := &persistenceblobs.ExecutionStats{HistorySize: 127}
	vh := &history.VersionHistories{
		CurrentVersionHistoryIndex: 18,
		Histories:                  nil,
	}
	wei := &WorkflowExecutionInfo{
		NamespaceId:                namespaceID,
		WorkflowId:                 workflowID,
		TaskQueue:                  taskqueue,
		WorkflowTypeName:           workflowType,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		LastFirstEventId:           common.FirstEventID,
		LastProcessedEvent:         lastProcessedEventID,
		ExecutionState: &persistenceblobs.WorkflowExecutionState{
			RunId:                      runID,
			CreateRequestId:            uuid.New(),
			State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		ExecutionStats: stats,
	}

	protoWei, protoState, err := WorkflowExecutionToProto(wei, startVersion, vh)
	s.NoError(err)
	s.NotNil(protoWei)
	s.NotNil(protoState)
	s.Equal(protoWei.VersionHistories, vh)

	// Validate dual write of execution stats
	s.Equal(protoWei.HistorySize, wei.ExecutionStats.HistorySize)
	s.Equal(protoWei.ExecutionStats.HistorySize, wei.ExecutionStats.HistorySize)

	// Emulate old proto
	protoWei.ExecutionStats = nil

	weiBytes, err := serialization.WorkflowExecutionInfoToBlob(protoWei)
	s.NoError(err)
	stateBytes, err := serialization.WorkflowExecutionStateToBlob(protoState)
	s.NoError(err)

	newProtoWei, err := serialization.WorkflowExecutionInfoFromBlob(weiBytes.Data, weiBytes.Encoding.String())
	s.NoError(err)

	newProtoState, err := serialization.WorkflowExecutionStateFromBlob(stateBytes.Data, stateBytes.Encoding.String())
	s.NoError(err)

	newNextEventId := int64(10)
	decodedWei := WorkflowExecutionFromProto(newProtoWei, newProtoState, newNextEventId)
	s.EqualValues(newNextEventId, decodedWei.NextEventId)
	wei.NextEventId = newNextEventId
	s.EqualValues(wei, decodedWei)
}
