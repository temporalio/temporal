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

package persistencetests

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
)

type (
	DynamicRateLimitSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func (s *DynamicRateLimitSuite) SetupSuite() {
	healthSignals := aggregate.NewPerShardPerNsHealthSignalAggregator(
		dynamicconfig.GetDurationPropertyFn(3*time.Second),
		dynamicconfig.GetIntPropertyFn(100),
		metrics.NoopMetricsHandler,
	)

	rateLimiter := client.NewHealthRequestRateLimiterImpl(
		healthSignals,
		3*time.Second,
		func() float64 { return float64(200) },
		100,
		0.5,
		0.3,
		0.1,
	)

	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{FaultInjection: &config.FaultInjection{Rate: 0.0}})
	s.TestBase.PersistenceHealthSignals = healthSignals
	s.TestBase.PersistenceRateLimiter = rateLimiter

	s.TestBase.Setup(nil)
}

func (s *DynamicRateLimitSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *DynamicRateLimitSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
}

func (s *DynamicRateLimitSuite) TearDownTest() {
	s.cancel()
}

func (s *DynamicRateLimitSuite) TestNamespaceReplicationQueue() {
	s.TestBase.FaultInjection.UpdateRate(0.5)

	numMessages := 100
	concurrentSenders := 10
	maxAttempts := 5

	messageChan := make(chan *replicationspb.ReplicationTask)

	taskType := enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK
	go func() {
		for i := 0; i < numMessages; i++ {
			messageChan <- &replicationspb.ReplicationTask{
				TaskType: taskType,
				Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
					NamespaceTaskAttributes: &replicationspb.NamespaceTaskAttributes{
						Id: fmt.Sprintf("message-%v", i),
					},
				},
			}
		}
		close(messageChan)
	}()

	wg := sync.WaitGroup{}
	wg.Add(concurrentSenders)

	for i := 0; i < concurrentSenders; i++ {
		go func(senderNum int) {
			defer wg.Done()
			for message := range messageChan {
				var err error
				for n := 0; n < maxAttempts; n++ {
					err = s.Publish(s.ctx, message)
					if err == nil {
						break
					}
					time.Sleep(5 * time.Second)
				}

				id := message.Attributes.(*replicationspb.ReplicationTask_NamespaceTaskAttributes).NamespaceTaskAttributes.Id
				s.Nil(err, "Enqueue message failed when sender %d tried to send %s", senderNum, id)
			}
		}(i)
	}

	wg.Wait()

	result, lastRetrievedMessageID, err := s.GetReplicationMessages(s.ctx, persistence.EmptyQueueMessageID, numMessages)
	s.Nil(err, "GetReplicationMessages failed.")
	s.Len(result, numMessages)
	s.Equal(int64(numMessages-1), lastRetrievedMessageID)
}
