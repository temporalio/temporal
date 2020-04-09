package history

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/resource"
)

type (
	replicationTaskFetcherSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockResource           *resource.Test
		config                 *Config
		frontendClient         *adminservicemock.MockAdminServiceClient
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

	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.frontendClient = s.mockResource.RemoteAdminClient
	logger := log.NewNoop()
	s.config = NewDynamicConfigForTest()

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
	token := &replicationgenpb.ReplicationToken{
		ShardId:                0,
		LastProcessedMessageId: 1,
		LastRetrievedMessageId: 2,
	}
	requestByShard[0] = &request{
		token: token,
	}
	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationgenpb.ReplicationToken{
			token,
		},
		ClusterName: "active",
	}
	messageByShared := make(map[int32]*replicationgenpb.ReplicationMessages)
	messageByShared[0] = &replicationgenpb.ReplicationMessages{}
	expectedResponse := &adminservice.GetReplicationMessagesResponse{
		MessagesByShard: messageByShared,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), replicationMessageRequest).Return(expectedResponse, nil)
	response, err := s.replicationTaskFetcher.getMessages(requestByShard)
	s.NoError(err)
	s.Equal(messageByShared, response)
}

func (s *replicationTaskFetcherSuite) TestFetchAndDistributeTasks() {
	requestByShard := make(map[int32]*request)
	token := &replicationgenpb.ReplicationToken{
		ShardId:                0,
		LastProcessedMessageId: 1,
		LastRetrievedMessageId: 2,
	}
	respChan := make(chan *replicationgenpb.ReplicationMessages, 1)
	requestByShard[0] = &request{
		token:    token,
		respChan: respChan,
	}
	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationgenpb.ReplicationToken{
			token,
		},
		ClusterName: "active",
	}
	messageByShared := make(map[int32]*replicationgenpb.ReplicationMessages)
	messageByShared[0] = &replicationgenpb.ReplicationMessages{}
	expectedResponse := &adminservice.GetReplicationMessagesResponse{
		MessagesByShard: messageByShared,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), replicationMessageRequest).Return(expectedResponse, nil)
	err := s.replicationTaskFetcher.fetchAndDistributeTasks(requestByShard)
	s.NoError(err)
	respToken := <-respChan
	s.Equal(messageByShared[0], respToken)
}
