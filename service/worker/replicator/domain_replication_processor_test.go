// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package replicator

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/admin/adminservicetest"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/resource"
)

type domainReplicationSuite struct {
	suite.Suite
	*require.Assertions
	controller *gomock.Controller

	sourceCluster          string
	taskExecutor           *domain.MockReplicationTaskExecutor
	domainReplicationQueue *persistence.MockDomainReplicationQueue
	remoteClient           *adminservicetest.MockClient
	replicationProcessor   *domainReplicationProcessor
}

func TestDomainReplicationSuite(t *testing.T) {
	s := new(domainReplicationSuite)
	suite.Run(t, s)
}

func (s *domainReplicationSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	resource := resource.NewTest(s.controller, metrics.Worker)

	s.sourceCluster = "active"
	s.taskExecutor = domain.NewMockReplicationTaskExecutor(s.controller)
	s.domainReplicationQueue = persistence.NewMockDomainReplicationQueue(s.controller)
	s.remoteClient = resource.RemoteAdminClient
	serviceResolver := resource.WorkerServiceResolver
	serviceResolver.EXPECT().Lookup(s.sourceCluster).Return(resource.GetHostInfo(), nil).AnyTimes()
	s.replicationProcessor = newDomainReplicationProcessor(
		s.sourceCluster,
		resource.GetLogger(),
		s.remoteClient,
		resource.GetMetricsClient(),
		s.taskExecutor,
		resource.GetHostInfo(),
		serviceResolver,
		s.domainReplicationQueue,
	)
	retryPolicy := backoff.NewExponentialRetryPolicy(time.Nanosecond)
	retryPolicy.SetMaximumAttempts(1)
	s.replicationProcessor.retryPolicy = retryPolicy
}

func (s *domainReplicationSuite) TearDownTest() {
}

func (s *domainReplicationSuite) TestHandleDomainReplicationTask() {
	domainID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
		DomainTaskAttributes: &replicator.DomainTaskAttributes{
			ID: common.StringPtr(domainID),
		},
	}

	s.taskExecutor.EXPECT().Execute(task.DomainTaskAttributes).Return(nil).Times(1)
	err := s.replicationProcessor.handleDomainReplicationTask(task)
	s.NoError(err)

	s.taskExecutor.EXPECT().Execute(task.DomainTaskAttributes).Return(errors.New("test")).Times(1)
	err = s.replicationProcessor.handleDomainReplicationTask(task)
	s.Error(err)

}

func (s *domainReplicationSuite) TestPutDomainReplicationTaskToDLQ() {
	domainID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
	}

	err := s.replicationProcessor.putDomainReplicationTaskToDLQ(task)
	s.Error(err)

	task.DomainTaskAttributes = &replicator.DomainTaskAttributes{
		ID: common.StringPtr(domainID),
	}

	s.domainReplicationQueue.EXPECT().PublishToDLQ(task).Return(nil).Times(1)
	err = s.replicationProcessor.putDomainReplicationTaskToDLQ(task)
	s.NoError(err)

	s.domainReplicationQueue.EXPECT().PublishToDLQ(task).Return(errors.New("test")).Times(1)
	err = s.replicationProcessor.putDomainReplicationTaskToDLQ(task)
	s.Error(err)
}

func (s *domainReplicationSuite) TestFetchDomainReplicationTasks() {
	domainID1 := uuid.New()
	domainID2 := uuid.New()
	lastMessageID := int64(1000)
	resp := &replicator.GetDomainReplicationMessagesResponse{
		Messages: &replicator.ReplicationMessages{
			ReplicationTasks: []*replicator.ReplicationTask{
				{
					TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
					DomainTaskAttributes: &replicator.DomainTaskAttributes{
						ID: common.StringPtr(domainID1),
					},
				},
				{
					TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
					DomainTaskAttributes: &replicator.DomainTaskAttributes{
						ID: common.StringPtr(domainID2),
					},
				},
			},
			LastRetrievedMessageId: common.Int64Ptr(lastMessageID),
		},
	}
	s.remoteClient.EXPECT().GetDomainReplicationMessages(gomock.Any(), gomock.Any()).Return(resp, nil)
	s.taskExecutor.EXPECT().Execute(resp.Messages.ReplicationTasks[0].DomainTaskAttributes).Return(nil).Times(1)
	s.taskExecutor.EXPECT().Execute(resp.Messages.ReplicationTasks[1].DomainTaskAttributes).Return(nil).Times(1)

	s.replicationProcessor.fetchDomainReplicationTasks()
	s.Equal(lastMessageID, s.replicationProcessor.lastProcessedMessageID)
	s.Equal(lastMessageID, s.replicationProcessor.lastRetrievedMessageID)
}

func (s *domainReplicationSuite) TestFetchDomainReplicationTasks_Failed() {
	lastMessageID := int64(1000)
	s.remoteClient.EXPECT().GetDomainReplicationMessages(gomock.Any(), gomock.Any()).Return(nil, errors.New("test"))
	s.replicationProcessor.fetchDomainReplicationTasks()
	s.NotEqual(lastMessageID, s.replicationProcessor.lastProcessedMessageID)
	s.NotEqual(lastMessageID, s.replicationProcessor.lastRetrievedMessageID)
}

func (s *domainReplicationSuite) TestFetchDomainReplicationTasks_FailedOnExecution() {
	domainID1 := uuid.New()
	domainID2 := uuid.New()
	lastMessageID := int64(1000)
	resp := &replicator.GetDomainReplicationMessagesResponse{
		Messages: &replicator.ReplicationMessages{
			ReplicationTasks: []*replicator.ReplicationTask{
				{
					TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
					DomainTaskAttributes: &replicator.DomainTaskAttributes{
						ID: common.StringPtr(domainID1),
					},
				},
				{
					TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
					DomainTaskAttributes: &replicator.DomainTaskAttributes{
						ID: common.StringPtr(domainID2),
					},
				},
			},
			LastRetrievedMessageId: common.Int64Ptr(lastMessageID),
		},
	}
	s.remoteClient.EXPECT().GetDomainReplicationMessages(gomock.Any(), gomock.Any()).Return(resp, nil)
	s.taskExecutor.EXPECT().Execute(gomock.Any()).Return(errors.New("test")).AnyTimes()
	s.domainReplicationQueue.EXPECT().PublishToDLQ(gomock.Any()).Return(nil).Times(2)

	s.replicationProcessor.fetchDomainReplicationTasks()
	s.Equal(lastMessageID, s.replicationProcessor.lastProcessedMessageID)
	s.Equal(lastMessageID, s.replicationProcessor.lastRetrievedMessageID)
}

func (s *domainReplicationSuite) TestFetchDomainReplicationTasks_FailedOnDLQ() {
	domainID1 := uuid.New()
	domainID2 := uuid.New()
	lastMessageID := int64(1000)
	resp := &replicator.GetDomainReplicationMessagesResponse{
		Messages: &replicator.ReplicationMessages{
			ReplicationTasks: []*replicator.ReplicationTask{
				{
					TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
					DomainTaskAttributes: &replicator.DomainTaskAttributes{
						ID: common.StringPtr(domainID1),
					},
				},
				{
					TaskType: replicator.ReplicationTaskTypeDomain.Ptr(),
					DomainTaskAttributes: &replicator.DomainTaskAttributes{
						ID: common.StringPtr(domainID2),
					},
				},
			},
			LastRetrievedMessageId: common.Int64Ptr(lastMessageID),
		},
	}
	s.remoteClient.EXPECT().GetDomainReplicationMessages(gomock.Any(), gomock.Any()).Return(resp, nil)
	s.taskExecutor.EXPECT().Execute(gomock.Any()).Return(nil).AnyTimes()
	s.domainReplicationQueue.EXPECT().PublishToDLQ(gomock.Any()).Return(errors.New("test")).Times(1)

	s.replicationProcessor.fetchDomainReplicationTasks()
	s.Equal(lastMessageID, s.replicationProcessor.lastProcessedMessageID)
	s.Equal(lastMessageID, s.replicationProcessor.lastRetrievedMessageID)
}
