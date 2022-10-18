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

package history

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/definition"
)

type (
	shardedCacheSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		shardContext *definition.MockShardContext
	}
)

func TestShardedCacheSuite(t *testing.T) {
	s := new(shardedCacheSuite)
	suite.Run(t, s)
}

func (s *shardedCacheSuite) SetupSuite() {
}

func (s *shardedCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.shardContext = definition.NewMockShardContext(s.controller)
}

func (s *shardedCacheSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *shardedCacheSuite) TestInit() {
	numShards := int32(1)
	shardedCache := NewCache(numShards)
	config := configs.NewConfig(dynamicconfig.NewNoopCollection(), numShards, false, "")
	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler

	s.shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(logger).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()

	checker := shardedCache.GetOrCreate(s.shardContext)
	s.NotNil(checker)

}

func (s *shardedCacheSuite) TestGet() {
	numShards := int32(1)
	shardedCache := NewCache(numShards)
	config := configs.NewConfig(dynamicconfig.NewNoopCollection(), numShards, false, "")
	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler

	s.shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(logger).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()

	checker0 := shardedCache.GetOrCreate(s.shardContext)

	s.shardContext.EXPECT().IsValid().Return(true)
	checker1 := shardedCache.GetOrCreate(s.shardContext)
	s.True(checker0 == checker1)
}

func (s *shardedCacheSuite) TestRecreate() {
	numShards := int32(1)
	shardedCache := NewCache(numShards)
	config := configs.NewConfig(dynamicconfig.NewNoopCollection(), numShards, false, "")
	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler

	s.shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(logger).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()

	checker0 := shardedCache.GetOrCreate(s.shardContext)
	s.shardContext.EXPECT().IsValid().Return(false)

	newShardContext := definition.NewMockShardContext(s.controller)
	newShardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	newShardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	newShardContext.EXPECT().GetLogger().Return(logger).AnyTimes()
	newShardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	checker1 := shardedCache.GetOrCreate(newShardContext)
	s.True(checker0 != checker1)
}

func (s *shardedCacheSuite) TestConcurrency_Init() {
	numShards := int32(1)
	shardedCache := NewCache(numShards)
	config := configs.NewConfig(dynamicconfig.NewNoopCollection(), numShards, false, "")
	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler

	s.shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(logger).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	s.shardContext.EXPECT().IsValid().Return(true).AnyTimes()

	concurrency := 128
	var startWG sync.WaitGroup
	var endWG sync.WaitGroup
	startWG.Add(concurrency)
	endWG.Add(concurrency)

	var lock sync.Mutex
	var cache api.WorkflowConsistencyChecker

	for i := 0; i < concurrency; i++ {
		go func() {
			startWG.Wait()
			defer endWG.Done()

			actualCache := shardedCache.GetOrCreate(s.shardContext)
			lock.Lock()
			defer lock.Unlock()
			if cache == nil {
				cache = actualCache
			} else {
				s.True(cache == actualCache)
			}
		}()
		startWG.Done()
	}
	endWG.Wait()
}

func (s *shardedCacheSuite) TestConcurrency_Recreate() {
	numShards := int32(1)
	shardedCache := NewCache(numShards)
	config := configs.NewConfig(dynamicconfig.NewNoopCollection(), numShards, false, "")
	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler

	s.shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(logger).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	invalidCache := shardedCache.GetOrCreate(s.shardContext)
	s.shardContext.EXPECT().IsValid().Return(false).AnyTimes()

	newShardContext := definition.NewMockShardContext(s.controller)
	newShardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	newShardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	newShardContext.EXPECT().GetLogger().Return(logger).AnyTimes()
	newShardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	newShardContext.EXPECT().IsValid().Return(true).AnyTimes()

	concurrency := 128
	var startWG sync.WaitGroup
	var endWG sync.WaitGroup
	startWG.Add(concurrency)
	endWG.Add(concurrency)

	var lock sync.Mutex
	var cache api.WorkflowConsistencyChecker

	for i := 0; i < concurrency; i++ {
		go func() {
			startWG.Wait()
			defer endWG.Done()

			actualCache := shardedCache.GetOrCreate(newShardContext)
			s.True(invalidCache != actualCache)

			lock.Lock()
			defer lock.Unlock()
			if cache == nil {
				cache = actualCache
			} else {
				s.True(cache == actualCache)
			}
		}()
		startWG.Done()
	}
	endWG.Wait()
}
