// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"os"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/replication"
)

type (
	// QueuePersistenceSuite contains queue persistence tests
	QueuePersistenceSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// SetupSuite implementation
func (s *QueuePersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// SetupTest implementation
func (s *QueuePersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TearDownSuite implementation
func (s *QueuePersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// TestDomainReplicationQueue tests domain replication queue operations
func (s *QueuePersistenceSuite) TestDomainReplicationQueue() {
	numMessages := 100
	concurrentSenders := 10

	messageChan := make(chan interface{})

	taskType := enums.ReplicationTaskTypeDomain
	go func() {
		for i := 0; i < numMessages; i++ {
			messageChan <- &replication.ReplicationTask{
				TaskType: taskType,
				Attributes: &replication.ReplicationTask_DomainTaskAttributes{
					DomainTaskAttributes: &replication.DomainTaskAttributes{
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
		go func() {
			defer wg.Done()
			for message := range messageChan {
				err := s.Publish(message)
				s.Nil(err, "Enqueue message failed.")
			}
		}()
	}

	wg.Wait()

	result, lastRetrievedMessageID, err := s.GetReplicationMessages(-1, numMessages)
	s.Nil(err, "GetReplicationMessages failed.")
	s.Len(result, numMessages)
	s.Equal(numMessages-1, lastRetrievedMessageID)
}

// TestQueueMetadataOperations tests queue metadata operations
func (s *QueuePersistenceSuite) TestQueueMetadataOperations() {
	clusterAckLevels, err := s.GetAckLevels()
	s.Require().NoError(err)
	s.Assert().Len(clusterAckLevels, 0)

	err = s.UpdateAckLevel(10, "test1")
	s.Require().NoError(err)

	clusterAckLevels, err = s.GetAckLevels()
	s.Require().NoError(err)
	s.Assert().Len(clusterAckLevels, 1)
	s.Assert().Equal(10, clusterAckLevels["test1"])

	err = s.UpdateAckLevel(20, "test1")
	s.Require().NoError(err)

	clusterAckLevels, err = s.GetAckLevels()
	s.Require().NoError(err)
	s.Assert().Len(clusterAckLevels, 1)
	s.Assert().Equal(20, clusterAckLevels["test1"])

	err = s.UpdateAckLevel(25, "test2")
	s.Require().NoError(err)

	clusterAckLevels, err = s.GetAckLevels()
	s.Require().NoError(err)
	s.Assert().Len(clusterAckLevels, 2)
	s.Assert().Equal(20, clusterAckLevels["test1"])
	s.Assert().Equal(25, clusterAckLevels["test2"])
}

// TestDomainReplicationDLQ tests domain DLQ operations
func (s *QueuePersistenceSuite) TestDomainReplicationDLQ() {
	numMessages := 100
	concurrentSenders := 10

	messageChan := make(chan interface{})

	taskType := enums.ReplicationTaskTypeDomain
	go func() {
		for i := 0; i < numMessages; i++ {
			messageChan <- &replication.ReplicationTask{
				TaskType: taskType,
				Attributes: &replication.ReplicationTask_DomainTaskAttributes{
					DomainTaskAttributes: &replication.DomainTaskAttributes{
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
		go func() {
			defer wg.Done()
			for message := range messageChan {
				err := s.PublishToDomainDLQ(message)
				s.Nil(err, "Enqueue message failed.")
			}
		}()
	}

	wg.Wait()

	result1, token, err := s.GetMessagesFromDomainDLQ(-1, numMessages, numMessages/2, nil)
	s.Nil(err, "GetReplicationMessages failed.")
	s.NotNil(token)
	result2, token, err := s.GetMessagesFromDomainDLQ(-1, numMessages, numMessages, token)
	s.Nil(err, "GetReplicationMessages failed.")
	s.Equal(len(token), 0)
	s.Equal(len(result1)+len(result2), numMessages)

	lastMessageID := result2[len(result2)-1].SourceTaskId
	err = s.DeleteMessageFromDomainDLQ(int(lastMessageID))
	s.NoError(err)
	result3, token, err := s.GetMessagesFromDomainDLQ(-1, numMessages, numMessages, token)
	s.Nil(err, "GetReplicationMessages failed.")
	s.Equal(len(token), 0)
	s.Equal(len(result3), numMessages-1)

	err = s.RangeDeleteMessagesFromDomainDLQ(-1, int(lastMessageID))
	s.NoError(err)
	result4, token, err := s.GetMessagesFromDomainDLQ(-1, numMessages, numMessages, token)
	s.Nil(err, "GetReplicationMessages failed.")
	s.Equal(len(token), 0)
	s.Equal(len(result4), 0)
}

// TestDomainDLQMetadataOperations tests queue metadata operations
func (s *QueuePersistenceSuite) TestDomainDLQMetadataOperations() {
	ackLevel, err := s.GetDomainDLQAckLevel()
	s.Require().NoError(err)
	s.Equal(-1, ackLevel)

	err = s.UpdateDomainDLQAckLevel(10)
	s.NoError(err)

	ackLevel, err = s.GetDomainDLQAckLevel()
	s.Require().NoError(err)
	s.Equal(10, ackLevel)

	err = s.UpdateDomainDLQAckLevel(1)
	s.NoError(err)

	ackLevel, err = s.GetDomainDLQAckLevel()
	s.Require().NoError(err)
	s.Equal(10, ackLevel)
}
