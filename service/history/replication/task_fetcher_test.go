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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/admin/adminservicetest"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/service/history/config"
)

type (
	taskFetcherSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockResource   *resource.Test
		config         *config.Config
		frontendClient *adminservicetest.MockClient
		taskFetcher    *taskFetcherImpl
	}
)

func TestReplicationTaskFetcherSuite(t *testing.T) {
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
	logger := log.NewNoop()
	s.config = config.NewForTest()

	s.taskFetcher = newReplicationTaskFetcher(
		logger,
		"standby",
		"active",
		s.config,
		s.frontendClient,
	).(*taskFetcherImpl)
}

func (s *taskFetcherSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
}

func (s *taskFetcherSuite) TestGetMessages() {
	requestByShard := make(map[int32]*request)
	token := &replicator.ReplicationToken{
		ShardID:                common.Int32Ptr(0),
		LastProcessedMessageId: common.Int64Ptr(1),
		LastRetrievedMessageId: common.Int64Ptr(2),
	}
	requestByShard[0] = &request{
		token: token,
	}
	replicationMessageRequest := &replicator.GetReplicationMessagesRequest{
		Tokens: []*replicator.ReplicationToken{
			token,
		},
		ClusterName: common.StringPtr("active"),
	}
	messageByShared := make(map[int32]*replicator.ReplicationMessages)
	messageByShared[0] = &replicator.ReplicationMessages{}
	expectedResponse := &replicator.GetReplicationMessagesResponse{
		MessagesByShard: messageByShared,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), replicationMessageRequest).Return(expectedResponse, nil)
	response, err := s.taskFetcher.getMessages(requestByShard)
	s.NoError(err)
	s.Equal(messageByShared, response)
}

func (s *taskFetcherSuite) TestFetchAndDistributeTasks() {
	requestByShard := make(map[int32]*request)
	token := &replicator.ReplicationToken{
		ShardID:                common.Int32Ptr(0),
		LastProcessedMessageId: common.Int64Ptr(1),
		LastRetrievedMessageId: common.Int64Ptr(2),
	}
	respChan := make(chan *replicator.ReplicationMessages, 1)
	requestByShard[0] = &request{
		token:    token,
		respChan: respChan,
	}
	replicationMessageRequest := &replicator.GetReplicationMessagesRequest{
		Tokens: []*replicator.ReplicationToken{
			token,
		},
		ClusterName: common.StringPtr("active"),
	}
	messageByShared := make(map[int32]*replicator.ReplicationMessages)
	messageByShared[0] = &replicator.ReplicationMessages{}
	expectedResponse := &replicator.GetReplicationMessagesResponse{
		MessagesByShard: messageByShared,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), replicationMessageRequest).Return(expectedResponse, nil)
	err := s.taskFetcher.fetchAndDistributeTasks(requestByShard)
	s.NoError(err)
	respToken := <-respChan
	s.Equal(messageByShared[0], respToken)
}
