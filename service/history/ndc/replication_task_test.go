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

package ndc

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
)

type (
	replicationTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata
	}
)

func TestReplicationTaskSuite(t *testing.T) {
	s := new(replicationTaskSuite)
	suite.Run(t, s)
}

func (s *replicationTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return("some random cluster name").AnyTimes()
}

func (s *replicationTaskSuite) TearDownSuite() {

}

func (s *replicationTaskSuite) TestResetInfo() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId:   13,
			EventType: enums.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		},
		{
			EventId: 14,
		},
	}

	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		nil,
	)
	info := task.getBaseWorkflowInfo()
	s.Nil(info)
	s.False(task.isWorkflowReset())
}
