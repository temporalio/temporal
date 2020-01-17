// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.temporal.io/temporal-proto/workflowservicemock"

	"github.com/temporalio/temporal/common/log"
)

type (
	replicationTaskFetcherSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		config                 *Config
		frontendClient         *workflowservicemock.MockWorkflowServiceYARPCClient
		replicationTaskFetcher *ReplicationTaskFetcherImpl
	}
)

func TestReplicationTaskFetcherSuite(t *testing.T) {
	s := new(replicationTaskFetcherSuite)
	suite.Run(t, s)
}

func (s *replicationTaskFetcherSuite) SetupSuite() {

}

func (s *replicationTaskFetcherSuite) TearDownSuite() {

}

func (s *replicationTaskFetcherSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	logger := log.NewNoop()
	s.config = NewDynamicConfigForTest()
	s.frontendClient = workflowservicemock.NewMockWorkflowServiceYARPCClient(s.controller)

	s.replicationTaskFetcher = newReplicationTaskFetcher(
		logger,
		"standby",
		"active",
		s.config,
		s.frontendClient,
	)
}

func (s *replicationTaskFetcherSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *replicationTaskFetcherSuite) TestGetMessages() {
	requestByShard := make(map[int32]*request)
	token := &commonproto.ReplicationToken{
		ShardID:                0,
		LastProcessedMessageId: 1,
		LastRetrievedMessageId: 2,
	}
	requestByShard[0] = &request{
		token: token,
	}
	replicationMessageRequest := &workflowservice.GetReplicationMessagesRequest{
		Tokens: []*commonproto.ReplicationToken{
			token,
		},
		ClusterName: "active",
	}
	messageByShared := make(map[int32]*commonproto.ReplicationMessages)
	messageByShared[0] = &commonproto.ReplicationMessages{}
	expectedResponse := &workflowservice.GetReplicationMessagesResponse{
		MessagesByShard: messageByShared,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), replicationMessageRequest).Return(expectedResponse, nil)
	response, err := s.replicationTaskFetcher.getMessages(requestByShard)
	s.NoError(err)
	s.Equal(messageByShared, response)
}

func (s *replicationTaskFetcherSuite) TestFetchAndDistributeTasks() {
	requestByShard := make(map[int32]*request)
	token := &commonproto.ReplicationToken{
		ShardID:                0,
		LastProcessedMessageId: 1,
		LastRetrievedMessageId: 2,
	}
	respChan := make(chan *commonproto.ReplicationMessages, 1)
	requestByShard[0] = &request{
		token:    token,
		respChan: respChan,
	}
	replicationMessageRequest := &workflowservice.GetReplicationMessagesRequest{
		Tokens: []*commonproto.ReplicationToken{
			token,
		},
		ClusterName: "active",
	}
	messageByShared := make(map[int32]*commonproto.ReplicationMessages)
	messageByShared[0] = &commonproto.ReplicationMessages{}
	expectedResponse := &workflowservice.GetReplicationMessagesResponse{
		MessagesByShard: messageByShared,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), replicationMessageRequest).Return(expectedResponse, nil)
	err := s.replicationTaskFetcher.fetchAndDistributeTasks(requestByShard)
	s.NoError(err)
	respToken := <-respChan
	s.Equal(messageByShared[0], respToken)
}
