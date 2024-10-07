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

package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/tqid"
)

type (
	taskQueueInternalInfoCacheSuite struct {
		suite.Suite
		*require.Assertions
		taskQueueInternalInfoCache taskQueueInternalInfoCache
		timeSource                 *clock.EventTimeSource
	}
)

const taskQueueInternalInfoCacheTTL = 10 * time.Second

func createTaskQueueInternalInfoCache(options *cache.Options) taskQueueInternalInfoCache {
	return newTaskQueueInternalInfoCache(options)
}

func TestTaskQueueInternalInfoSuite(t *testing.T) {
	s := new(taskQueueInternalInfoCacheSuite)
	suite.Run(t, s)
}

func (s *taskQueueInternalInfoCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.timeSource = clock.NewEventTimeSource()
	currentTime := time.Now()
	s.timeSource.Update(currentTime)

	options := cache.Options{TTL: taskQueueInternalInfoCacheTTL, TimeSource: s.timeSource}
	s.taskQueueInternalInfoCache = createTaskQueueInternalInfoCache(&options)
}

func (s *taskQueueInternalInfoCacheSuite) TearDownTest() {}

func (s *taskQueueInternalInfoCacheSuite) TestGetPutWithTTL() {
	physicalInfoByBuildId := make(map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)

	buildID := "buildID123"
	taskQueueTypeMap := make(map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
	physicalInfo := &taskqueuespb.PhysicalTaskQueueInfo{}
	taskQueueTypeMap[enumspb.TASK_QUEUE_TYPE_WORKFLOW] = physicalInfo
	// Add the taskQueueTypeMap to the outer map using buildID as the key
	physicalInfoByBuildId[buildID] = taskQueueTypeMap

	nsid := "my-namespace"
	normalName := "very-normal"
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	proto := &taskqueuepb.TaskQueue{
		Name: normalName,
	}

	p, err := tqid.PartitionFromProto(proto, nsid, taskType)
	s.NoError(err)

	// adding it to our cache
	s.taskQueueInternalInfoCache.Put(p.Key(), physicalInfoByBuildId)
	cachedInternalInfo := s.taskQueueInternalInfoCache.Get(p.Key())
	s.Equal(physicalInfoByBuildId, cachedInternalInfo)

	// ensuring cache evicts after TTL has expired
	s.timeSource.Advance(11 * time.Second)
	cachedInternalInfo = s.taskQueueInternalInfoCache.Get(p.Key())
	s.Nil(cachedInternalInfo)
}
