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

package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
)

type (
	executableUnknownTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		clientBean              *client.MockBean
		shardController         *shard.MockController
		namespaceCache          *namespace.MockRegistry
		ndcHistoryResender      *xdc.MockNDCHistoryResender
		metricsHandler          metrics.Handler
		logger                  log.Logger
		eagerNamespaceRefresher *MockEagerNamespaceRefresher

		taskID int64
		task   *ExecutableUnknownTask
	}
)

func TestExecutableUnknownTaskSuite(t *testing.T) {
	s := new(executableUnknownTaskSuite)
	suite.Run(t, s)
}

func (s *executableUnknownTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableUnknownTaskSuite) TearDownSuite() {

}

func (s *executableUnknownTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)

	s.taskID = rand.Int63()
	s.task = NewExecutableUnknownTask(
		ProcessToolBox{
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			NDCHistoryResender:      s.ndcHistoryResender,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,
			DLQWriter:               NoopDLQWriter{},
		},
		s.taskID,
		time.Unix(0, rand.Int63()),
		nil,
	)
}

func (s *executableUnknownTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableUnknownTaskSuite) TestExecute() {
	err := s.task.Execute()
	s.IsType(serviceerror.NewInvalidArgument(""), err)
}

func (s *executableUnknownTaskSuite) TestHandleErr() {
	err := errors.New("OwO")
	s.Equal(err, s.task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableUnknownTaskSuite) TestMarkPoisonPill() {
	err := s.task.MarkPoisonPill()
	s.NoError(err)
}
