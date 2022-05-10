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
	"reflect"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
)

type (
	taskFetcherSuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		mockResource   *resource.Test
		frontendClient *adminservicemock.MockAdminServiceClient

		config *configs.Config
		logger log.Logger

		replicationTaskFetcher *taskFetcherImpl
	}

	getReplicationMessagesRequestMatcher struct {
		clusterName string
		tokens      map[int32]*replicationspb.ReplicationToken
	}
)

func TestTaskFetcherSuite(t *testing.T) {
	s := new(taskFetcherSuite)
	suite.Run(t, s)
}

func (s *taskFetcherSuite) SetupSuite() {

}

func (s *taskFetcherSuite) TearDownSuite() {

}

func (s *taskFetcherSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.frontendClient = s.mockResource.RemoteAdminClient
	s.logger = log.NewNoopLogger()
	s.config = tests.NewDynamicConfig()
	s.config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(1)

	s.replicationTaskFetcher = newReplicationTaskFetcher(
		s.logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		s.config,
		s.mockResource.ClientBean,
	)
}

func (s *taskFetcherSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *taskFetcherSuite) TestBufferRequests_NoDuplicate() {
	shardID := int32(1)

	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan,
	}

	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest)

	select {
	case <-respChan:
		s.Fail("new request channel should not be closed")
	default:
		// noop
	}

	s.Equal(map[int32]*replicationTaskRequest{
		shardID: shardRequest,
	}, s.replicationTaskFetcher.workers[0].requestByShard)
}

func (s *taskFetcherSuite) TestBufferRequests_Duplicate() {
	shardID := int32(1)

	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}

	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}

	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest1)
	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest2)

	_, ok := <-respChan1
	s.False(ok)

	select {
	case <-respChan2:
		s.Fail("new request channel should not be closed")
	default:
		// noop
	}

	s.Equal(map[int32]*replicationTaskRequest{
		shardID: shardRequest2,
	}, s.replicationTaskFetcher.workers[0].requestByShard)
}

func (s *taskFetcherSuite) TestGetMessages_All() {
	shardID := int32(1)
	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID: shardRequest,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	responseByShard := map[int32]*replicationspb.ReplicationMessages{
		shardID: {},
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(&adminservice.GetReplicationMessagesResponse{ShardMessages: responseByShard}, nil)
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	s.NoError(err)
	s.Equal(responseByShard[shardID], <-respChan)
}

func (s *taskFetcherSuite) TestGetMessages_Partial() {
	shardID1 := int32(1)
	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID1,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}
	shardID2 := int32(2)
	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID2,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID1: shardRequest1,
		shardID2: shardRequest2,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest1.token,
			shardRequest2.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	responseByShard := map[int32]*replicationspb.ReplicationMessages{
		shardID1: {},
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(&adminservice.GetReplicationMessagesResponse{ShardMessages: responseByShard}, nil)
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	s.NoError(err)
	s.Equal(responseByShard[shardID1], <-respChan1)
	s.Equal((*replicationspb.ReplicationMessages)(nil), <-respChan2)
}

func (s *taskFetcherSuite) TestGetMessages_Error() {
	shardID1 := int32(1)
	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID1,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}
	shardID2 := int32(2)
	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID2,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID1: shardRequest1,
		shardID2: shardRequest2,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest1.token,
			shardRequest2.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(nil, errors.New("random error"))
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	s.Error(err)
	s.Equal((*replicationspb.ReplicationMessages)(nil), <-respChan1)
	s.Equal((*replicationspb.ReplicationMessages)(nil), <-respChan2)
}

func (s *taskFetcherSuite) TestConcurrentFetchAndProcess_Success() {
	numShards := 1024

	s.config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(8)

	s.replicationTaskFetcher = newReplicationTaskFetcher(
		s.logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		s.config,
		s.mockResource.ClientBean,
	)

	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		gomock.Any(),
	).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	s.replicationTaskFetcher.Start()
	defer s.replicationTaskFetcher.Stop()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numShards)
	for i := 0; i < numShards; i++ {
		shardID := int32(i)
		go func() {
			defer waitGroup.Done()
			respChan := make(chan *replicationspb.ReplicationMessages, 1)
			shardRequest := &replicationTaskRequest{
				token: &replicationspb.ReplicationToken{
					ShardId:                shardID,
					LastProcessedMessageId: 1,
					LastRetrievedMessageId: 2,
				},
				respChan: respChan,
			}

			s.replicationTaskFetcher.getRequestChan() <- shardRequest
			<-respChan
		}()
	}
	waitGroup.Wait()
}

func (s *taskFetcherSuite) TestConcurrentFetchAndProcess_Error() {
	numShards := 1024

	s.config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(8)

	s.replicationTaskFetcher = newReplicationTaskFetcher(
		s.logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		s.config,
		s.mockResource.ClientBean,
	)

	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		gomock.Any(),
	).Return(nil, errors.New("random error")).AnyTimes()

	s.replicationTaskFetcher.Start()
	defer s.replicationTaskFetcher.Stop()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numShards)
	for i := 0; i < numShards; i++ {
		shardID := int32(i)
		go func() {
			defer waitGroup.Done()
			respChan := make(chan *replicationspb.ReplicationMessages, 1)
			shardRequest := &replicationTaskRequest{
				token: &replicationspb.ReplicationToken{
					ShardId:                shardID,
					LastProcessedMessageId: 1,
					LastRetrievedMessageId: 2,
				},
				respChan: respChan,
			}

			s.replicationTaskFetcher.getRequestChan() <- shardRequest
			<-respChan
		}()
	}
	waitGroup.Wait()
}

func newGetReplicationMessagesRequestMatcher(
	req *adminservice.GetReplicationMessagesRequest,
) *getReplicationMessagesRequestMatcher {
	tokens := make(map[int32]*replicationspb.ReplicationToken)
	for _, token := range req.Tokens {
		tokens[token.ShardId] = token
	}
	return &getReplicationMessagesRequestMatcher{
		clusterName: req.ClusterName,
		tokens:      tokens,
	}
}

func (m *getReplicationMessagesRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*adminservice.GetReplicationMessagesRequest)
	if !ok {
		return false
	}
	return reflect.DeepEqual(m, newGetReplicationMessagesRequestMatcher(req))
}

func (m *getReplicationMessagesRequestMatcher) String() string {
	// noop, not used
	return ""
}
