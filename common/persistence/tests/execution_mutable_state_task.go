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

package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

type (
	ExecutionMutableStateTaskSuite struct {
		suite.Suite
		*require.Assertions

		ShardID int32
		RangeID int64

		ShardManager     p.ShardManager
		ExecutionManager p.ExecutionManager
		Logger           log.Logger
	}
)

func NewExecutionMutableStateTaskSuite(
	t *testing.T,
	shardStore p.ShardStore,
	executionStore p.ExecutionStore,
	logger log.Logger,
) *ExecutionMutableStateTaskSuite {
	return &ExecutionMutableStateTaskSuite{
		Assertions: require.New(t),
		ShardManager: p.NewShardManager(
			shardStore,
			serialization.NewSerializer(),
		),
		ExecutionManager: p.NewExecutionManager(
			executionStore,
			serialization.NewSerializer(),
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		Logger: logger,
	}
}

func (s *ExecutionMutableStateTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.ShardID = 1 + rand.Int31n(16)
	resp, err := s.ShardManager.GetOrCreateShard(&p.GetOrCreateShardRequest{
		ShardID: s.ShardID,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: s.ShardID,
			RangeId: 1,
		},
	})
	s.NoError(err)
	previousRangeID := resp.ShardInfo.RangeId
	resp.ShardInfo.RangeId += 1
	err = s.ShardManager.UpdateShard(&p.UpdateShardRequest{
		ShardInfo:       resp.ShardInfo,
		PreviousRangeID: previousRangeID,
	})
	s.NoError(err)
	s.RangeID = resp.ShardInfo.RangeId
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetTransferTasks_Multiple() {
	transferTasks := []tasks.Task{
		&tasks.ActivityTask{},
	}
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetVisibilityTasks_Multiple() {

}
