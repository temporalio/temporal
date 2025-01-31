// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	TaskQueueUserDataSuite struct {
		suite.Suite
		*require.Assertions

		namespaceID string

		taskManager p.TaskManager
		logger      log.Logger

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func NewTaskQueueUserDataSuite(
	t *testing.T,
	taskStore p.TaskStore,
	logger log.Logger,
) *TaskQueueUserDataSuite {
	return &TaskQueueUserDataSuite{
		Assertions: require.New(t),
		taskManager: p.NewTaskManager(
			taskStore,
			serialization.NewSerializer(),
		),
		logger: logger,
	}
}

func (s *TaskQueueUserDataSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	s.namespaceID = uuid.New().String()
}

func (s *TaskQueueUserDataSuite) TearDownTest() {
	s.cancel()
}

func (s *TaskQueueUserDataSuite) TestSetInitialAndIncrement() {
	tq1 := "tq1"
	version := int64(0) // initial version must be 0

	// check that get returns not found
	_, err := s.taskManager.GetTaskQueueUserData(s.ctx, &p.GetTaskQueueUserDataRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   tq1,
	})
	s.Error(err)

	// set initial user data
	d1 := s.makeData(hlc.Zero(12345), version)
	err = s.taskManager.UpdateTaskQueueUserData(s.ctx, &p.UpdateTaskQueueUserDataRequest{
		NamespaceID: s.namespaceID,
		Updates: map[string]*p.SingleTaskQueueUserDataUpdate{
			tq1: &p.SingleTaskQueueUserDataUpdate{
				UserData: d1,
			},
		},
	})
	s.NoError(err)
	version++

	// check that we can get it back
	res, err := s.taskManager.GetTaskQueueUserData(s.ctx, &p.GetTaskQueueUserDataRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   tq1,
	})
	s.NoError(err)
	s.Equal(version, res.UserData.Version)
	s.True(hlc.Equal(d1.Data.Clock, res.UserData.Data.Clock))

	// increment it
	d2 := s.makeData(d1.Data.Clock, version)
	err = s.taskManager.UpdateTaskQueueUserData(s.ctx, &p.UpdateTaskQueueUserDataRequest{
		NamespaceID: s.namespaceID,
		Updates: map[string]*p.SingleTaskQueueUserDataUpdate{
			tq1: &p.SingleTaskQueueUserDataUpdate{
				UserData: d2,
			},
		},
	})
	s.NoError(err)
	version++

	// check that we can get it back
	res, err = s.taskManager.GetTaskQueueUserData(s.ctx, &p.GetTaskQueueUserDataRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   tq1,
	})
	s.NoError(err)
	s.Equal(version, res.UserData.Version)
	s.True(hlc.Equal(d2.Data.Clock, res.UserData.Data.Clock))
}

func (s *TaskQueueUserDataSuite) TestUpdateConflict() {
	tq1, tq2, tq3 := "tq1", "tq2", "tq3"

	// set up three task queues
	data := s.makeData(hlc.Zero(12345), 0)
	for range 3 {
		err := s.taskManager.UpdateTaskQueueUserData(s.ctx, &p.UpdateTaskQueueUserDataRequest{
			NamespaceID: s.namespaceID,
			Updates: map[string]*p.SingleTaskQueueUserDataUpdate{
				tq1: &p.SingleTaskQueueUserDataUpdate{UserData: data},
				tq2: &p.SingleTaskQueueUserDataUpdate{UserData: data},
				tq3: &p.SingleTaskQueueUserDataUpdate{UserData: data},
			},
		})
		s.NoError(err)
		data.Version++
	}

	// get all and verify
	for _, tq := range []string{tq1, tq2, tq3} {
		res, err := s.taskManager.GetTaskQueueUserData(s.ctx, &p.GetTaskQueueUserDataRequest{
			NamespaceID: s.namespaceID,
			TaskQueue:   tq,
		})
		s.NoError(err)
		s.Equal(int64(3), res.UserData.Version)
		s.True(hlc.Equal(data.Data.Clock, res.UserData.Data.Clock))
	}

	// do update where one conflicts
	d4 := s.makeData(data.Data.Clock, 4)
	var conflict1, conflict2, conflict3 bool
	err := s.taskManager.UpdateTaskQueueUserData(s.ctx, &p.UpdateTaskQueueUserDataRequest{
		NamespaceID: s.namespaceID,
		Updates: map[string]*p.SingleTaskQueueUserDataUpdate{
			tq1: &p.SingleTaskQueueUserDataUpdate{UserData: data, Conflicting: &conflict1},
			tq2: &p.SingleTaskQueueUserDataUpdate{UserData: d4, Conflicting: &conflict2},
			tq3: &p.SingleTaskQueueUserDataUpdate{UserData: data, Conflicting: &conflict3},
		},
	})
	s.Error(err)
	s.True(p.IsConflictErr(err))
	s.False(conflict1)
	s.True(conflict2)
	s.False(conflict3)

	// verify that none were updated
	for _, tq := range []string{tq1, tq2, tq3} {
		res, err := s.taskManager.GetTaskQueueUserData(s.ctx, &p.GetTaskQueueUserDataRequest{
			NamespaceID: s.namespaceID,
			TaskQueue:   tq,
		})
		s.NoError(err)
		s.Equal(int64(3), res.UserData.Version)
		s.True(hlc.Equal(data.Data.Clock, res.UserData.Data.Clock))
	}
}

func (s *TaskQueueUserDataSuite) makeData(prev *hlc.Clock, ver int64) *persistencespb.VersionedTaskQueueUserData {
	return &persistencespb.VersionedTaskQueueUserData{
		Data: &persistencespb.TaskQueueUserData{
			Clock: hlc.Next(prev, clock.NewRealTimeSource()),
		},
		Version: ver,
	}
}
