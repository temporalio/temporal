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

package domain

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/persistence"
)

type (
	dlqMessageHandlerSuite struct {
		suite.Suite

		*require.Assertions
		controller *gomock.Controller

		mockReplicationTaskExecutor *MockReplicationTaskExecutor
		mockReplicationQueue        *persistence.MockDomainReplicationQueue
		dlqMessageHandler           *dlqMessageHandlerImpl
	}
)

func TestDLQMessageHandlerSuite(t *testing.T) {
	s := new(dlqMessageHandlerSuite)
	suite.Run(t, s)
}

func (s *dlqMessageHandlerSuite) SetupSuite() {
}

func (s *dlqMessageHandlerSuite) TearDownSuite() {

}

func (s *dlqMessageHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	s.mockReplicationTaskExecutor = NewMockReplicationTaskExecutor(s.controller)
	s.mockReplicationQueue = persistence.NewMockDomainReplicationQueue(s.controller)

	logger := loggerimpl.NewLogger(zapLogger)
	s.dlqMessageHandler = NewDLQMessageHandler(
		s.mockReplicationTaskExecutor,
		s.mockReplicationQueue,
		logger,
	).(*dlqMessageHandlerImpl)
}

func (s *dlqMessageHandlerSuite) TearDownTest() {
}

func (s *dlqMessageHandlerSuite) TestReadMessages() {
	ackLevel := 10
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}

	tasks := []*replicator.ReplicationTask{
		{
			TaskType:     replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId: common.Int64Ptr(1),
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil).Times(1)

	resp, token, err := s.dlqMessageHandler.Read(lastMessageID, pageSize, pageToken)

	s.NoError(err)
	s.Equal(tasks, resp)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestReadMessages_ThrowErrorOnGetDLQAckLevel() {
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}

	tasks := []*replicator.ReplicationTask{
		{
			TaskType:     replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId: common.Int64Ptr(1),
		},
	}
	testError := fmt.Errorf("test")
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(-1, testError).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(tasks, nil, nil).Times(0)

	_, _, err := s.dlqMessageHandler.Read(lastMessageID, pageSize, pageToken)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestReadMessages_ThrowErrorOnReadMessages() {
	ackLevel := 10
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}

	testError := fmt.Errorf("test")
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(ackLevel, lastMessageID, pageSize, pageToken).
		Return(nil, nil, testError).Times(1)

	_, _, err := s.dlqMessageHandler.Read(lastMessageID, pageSize, pageToken)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestPurgeMessages() {
	ackLevel := 10
	lastMessageID := 20

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(ackLevel, lastMessageID).Return(nil).Times(1)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(lastMessageID).Return(nil).Times(1)
	err := s.dlqMessageHandler.Purge(lastMessageID)

	s.NoError(err)
}

func (s *dlqMessageHandlerSuite) TestPurgeMessages_ThrowErrorOnGetDLQAckLevel() {
	lastMessageID := 20
	testError := fmt.Errorf("test")

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(-1, testError).Times(1)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(gomock.Any(), gomock.Any()).Return(nil).Times(0)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any()).Times(0)
	err := s.dlqMessageHandler.Purge(lastMessageID)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestPurgeMessages_ThrowErrorOnPurgeMessages() {
	ackLevel := 10
	lastMessageID := 20
	testError := fmt.Errorf("test")

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(ackLevel, lastMessageID).Return(testError).Times(1)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any()).Times(0)
	err := s.dlqMessageHandler.Purge(lastMessageID)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages() {
	ackLevel := 10
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}
	messageID := 11

	domainAttribute := &replicator.DomainTaskAttributes{
		ID: common.StringPtr(uuid.New()),
	}

	tasks := []*replicator.ReplicationTask{
		{
			TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId:         common.Int64Ptr(int64(messageID)),
			DomainTaskAttributes: domainAttribute,
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil).Times(1)
	s.mockReplicationTaskExecutor.EXPECT().Execute(domainAttribute).Return(nil).Times(1)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(messageID).Return(nil).Times(1)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(ackLevel, messageID).Return(nil).Times(1)

	token, err := s.dlqMessageHandler.Merge(lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnGetDLQAckLevel() {
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}
	messageID := 11
	testError := fmt.Errorf("test")
	domainAttribute := &replicator.DomainTaskAttributes{
		ID: common.StringPtr(uuid.New()),
	}

	tasks := []*replicator.ReplicationTask{
		{
			TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId:         common.Int64Ptr(int64(messageID)),
			DomainTaskAttributes: domainAttribute,
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(-1, testError).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(tasks, nil, nil).Times(0)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().DeleteMessageFromDLQ(gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any()).Times(0)

	token, err := s.dlqMessageHandler.Merge(lastMessageID, pageSize, pageToken)
	s.Equal(testError, err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnGetDLQMessages() {
	ackLevel := 10
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}
	testError := fmt.Errorf("test")

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(ackLevel, lastMessageID, pageSize, pageToken).
		Return(nil, nil, testError).Times(1)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().DeleteMessageFromDLQ(gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any()).Times(0)

	token, err := s.dlqMessageHandler.Merge(lastMessageID, pageSize, pageToken)
	s.Equal(testError, err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnHandleReceivingTask() {
	ackLevel := 10
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}
	messageID1 := 11
	messageID2 := 12
	testError := fmt.Errorf("test")
	domainAttribute1 := &replicator.DomainTaskAttributes{
		ID: common.StringPtr(uuid.New()),
	}
	domainAttribute2 := &replicator.DomainTaskAttributes{
		ID: common.StringPtr(uuid.New()),
	}
	tasks := []*replicator.ReplicationTask{
		{
			TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId:         common.Int64Ptr(int64(messageID1)),
			DomainTaskAttributes: domainAttribute1,
		},
		{
			TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId:         common.Int64Ptr(int64(messageID2)),
			DomainTaskAttributes: domainAttribute2,
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil).Times(1)
	s.mockReplicationTaskExecutor.EXPECT().Execute(domainAttribute1).Return(nil).Times(1)
	s.mockReplicationTaskExecutor.EXPECT().Execute(domainAttribute2).Return(testError).Times(1)
	s.mockReplicationQueue.EXPECT().DeleteMessageFromDLQ(messageID1).Return(nil).Times(1)
	s.mockReplicationQueue.EXPECT().DeleteMessageFromDLQ(messageID2).Times(0)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(messageID1).Return(nil).Times(1)

	token, err := s.dlqMessageHandler.Merge(lastMessageID, pageSize, pageToken)
	s.Equal(testError, err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnDeleteMessages() {
	ackLevel := 10
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}
	messageID1 := 11
	messageID2 := 12
	testError := fmt.Errorf("test")
	domainAttribute1 := &replicator.DomainTaskAttributes{
		ID: common.StringPtr(uuid.New()),
	}
	domainAttribute2 := &replicator.DomainTaskAttributes{
		ID: common.StringPtr(uuid.New()),
	}
	tasks := []*replicator.ReplicationTask{
		{
			TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId:         common.Int64Ptr(int64(messageID1)),
			DomainTaskAttributes: domainAttribute1,
		},
		{
			TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId:         common.Int64Ptr(int64(messageID2)),
			DomainTaskAttributes: domainAttribute2,
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil).Times(1)
	s.mockReplicationTaskExecutor.EXPECT().Execute(domainAttribute1).Return(nil).Times(1)
	s.mockReplicationTaskExecutor.EXPECT().Execute(domainAttribute2).Return(nil).Times(1)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(ackLevel, messageID2).Return(testError).Times(1)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(messageID1).Return(nil).Times(1)

	token, err := s.dlqMessageHandler.Merge(lastMessageID, pageSize, pageToken)
	s.Error(err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_IgnoreErrorOnUpdateDLQAckLevel() {
	ackLevel := 10
	lastMessageID := 20
	pageSize := 100
	pageToken := []byte{}
	messageID := 11
	testError := fmt.Errorf("test")
	domainAttribute := &replicator.DomainTaskAttributes{
		ID: common.StringPtr(uuid.New()),
	}

	tasks := []*replicator.ReplicationTask{
		{
			TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
			SourceTaskId:         common.Int64Ptr(int64(messageID)),
			DomainTaskAttributes: domainAttribute,
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel().Return(ackLevel, nil).Times(1)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil).Times(1)
	s.mockReplicationTaskExecutor.EXPECT().Execute(domainAttribute).Return(nil).Times(1)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(ackLevel, messageID).Return(nil).Times(1)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(messageID).Return(testError).Times(1)

	token, err := s.dlqMessageHandler.Merge(lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Nil(token)
}
