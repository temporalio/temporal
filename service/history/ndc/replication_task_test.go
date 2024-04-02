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

func (s *replicationTaskSuite) TestValidateEventsSlice() {
	eS1 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}
	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 4,
			Version: 2,
		},
	}

	v, err := validateEventsSlice(eS1, eS2)
	s.Equal(int64(2), v)
	s.Nil(err)

	v, err = validateEventsSlice(eS1, eS3)
	s.Equal(int64(0), v)
	s.IsType(ErrEventSlicesNotConsecutive, err)

	v, err = validateEventsSlice(eS1, nil)
	s.Equal(int64(0), v)
	s.IsType(ErrEmptyEventSlice, err)
}

func (s *replicationTaskSuite) TestValidateEvents() {
	eS1 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}

	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}

	v, err := validateEvents(eS1)
	s.Nil(err)
	s.Equal(int64(2), v)

	v, err = validateEvents(eS2)
	s.Equal(int64(0), v)
	s.IsType(ErrEventIDMismatch, err)

	v, err = validateEvents(eS3)
	s.Equal(int64(0), v)
	s.IsType(ErrEventVersionMismatch, err)
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_ValidInput_SkipEvents() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
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
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
	)
	err := task.skipDuplicatedEvents(1)
	s.NoError(err)
	s.Equal(1, len(task.getEvents()))
	s.Equal(slice2, task.getEvents()[0])
	s.Equal(int64(13), task.getFirstEvent().EventId)
	s.Equal(int64(14), task.getLastEvent().EventId)
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_InvalidInput_ErrorOut() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
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
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
	)
	err := task.skipDuplicatedEvents(2)
	s.Error(err)
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_ZeroInput_DoNothing() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
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
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
	)
	err := task.skipDuplicatedEvents(0)
	s.NoError(err)
	s.Equal(2, len(task.getEvents()))
	s.Equal(slice1, task.getEvents()[0])
	s.Equal(slice2, task.getEvents()[1])
}
