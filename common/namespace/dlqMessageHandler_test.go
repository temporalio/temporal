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

package namespace

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type (
	dlqMessageHandlerSuite struct {
		suite.Suite

		*require.Assertions
		controller *gomock.Controller

		mockReplicationTaskExecutor *MockReplicationTaskExecutor
		mockReplicationQueue        *persistence.MockNamespaceReplicationQueue
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

	logger := log.NewTestLogger()
	s.mockReplicationTaskExecutor = NewMockReplicationTaskExecutor(s.controller)
	s.mockReplicationQueue = persistence.NewMockNamespaceReplicationQueue(s.controller)

	s.dlqMessageHandler = NewDLQMessageHandler(
		s.mockReplicationTaskExecutor,
		s.mockReplicationQueue,
		logger,
	).(*dlqMessageHandlerImpl)
}

func (s *dlqMessageHandlerSuite) TearDownTest() {
}

func (s *dlqMessageHandlerSuite) TestReadMessages() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}

	tasks := []*replicationspb.ReplicationTask{
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: 1,
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil)

	resp, token, err := s.dlqMessageHandler.Read(context.Background(), lastMessageID, pageSize, pageToken)

	s.NoError(err)
	s.Equal(tasks, resp)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestReadMessages_ThrowErrorOnGetDLQAckLevel() {
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}

	tasks := []*replicationspb.ReplicationTask{
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: 1,
		},
	}
	testError := fmt.Errorf("test")
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(int64(-1), testError)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(tasks, nil, nil).Times(0)

	_, _, err := s.dlqMessageHandler.Read(context.Background(), lastMessageID, pageSize, pageToken)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestReadMessages_ThrowErrorOnReadMessages() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}

	testError := fmt.Errorf("test")
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID, pageSize, pageToken).
		Return(nil, nil, testError)

	_, _, err := s.dlqMessageHandler.Read(context.Background(), lastMessageID, pageSize, pageToken)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestPurgeMessages() {
	ackLevel := int64(10)
	lastMessageID := int64(20)

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID).Return(nil)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any(), lastMessageID).Return(nil)
	err := s.dlqMessageHandler.Purge(context.Background(), lastMessageID)

	s.NoError(err)
}

func (s *dlqMessageHandlerSuite) TestPurgeMessages_ThrowErrorOnGetDLQAckLevel() {
	lastMessageID := int64(20)
	testError := fmt.Errorf("test")

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(int64(-1), testError)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(0)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any(), gomock.Any()).Times(0)
	err := s.dlqMessageHandler.Purge(context.Background(), lastMessageID)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestPurgeMessages_ThrowErrorOnPurgeMessages() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	testError := fmt.Errorf("test")

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID).Return(testError)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any(), gomock.Any()).Times(0)
	err := s.dlqMessageHandler.Purge(context.Background(), lastMessageID)

	s.Equal(testError, err)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}
	messageID := int64(11)

	namespaceAttribute := &replicationspb.NamespaceTaskAttributes{
		Id: uuid.New(),
	}

	tasks := []*replicationspb.ReplicationTask{
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: messageID,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: namespaceAttribute,
			},
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), namespaceAttribute).Return(nil)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any(), messageID).Return(nil)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(gomock.Any(), ackLevel, messageID).Return(nil)

	token, err := s.dlqMessageHandler.Merge(context.Background(), lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnGetDLQAckLevel() {
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}
	messageID := int64(11)
	testError := fmt.Errorf("test")
	namespaceAttribute := &replicationspb.NamespaceTaskAttributes{
		Id: uuid.New(),
	}

	tasks := []*replicationspb.ReplicationTask{
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: int64(messageID),
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: namespaceAttribute,
			},
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(int64(-1), testError)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(tasks, nil, nil).Times(0)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().DeleteMessageFromDLQ(gomock.Any(), gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any(), gomock.Any()).Times(0)

	token, err := s.dlqMessageHandler.Merge(context.Background(), lastMessageID, pageSize, pageToken)
	s.Equal(testError, err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnGetDLQMessages() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}
	testError := fmt.Errorf("test")

	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID, pageSize, pageToken).
		Return(nil, nil, testError)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().DeleteMessageFromDLQ(gomock.Any(), gomock.Any()).Times(0)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any(), gomock.Any()).Times(0)

	token, err := s.dlqMessageHandler.Merge(context.Background(), lastMessageID, pageSize, pageToken)
	s.Equal(testError, err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnHandleReceivingTask() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}
	messageID1 := int64(11)
	messageID2 := int64(12)
	testError := fmt.Errorf("test")
	namespaceAttribute1 := &replicationspb.NamespaceTaskAttributes{
		Id: uuid.New(),
	}
	namespaceAttribute2 := &replicationspb.NamespaceTaskAttributes{
		Id: uuid.New(),
	}
	tasks := []*replicationspb.ReplicationTask{
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: messageID1,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: namespaceAttribute1,
			},
		},
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: messageID2,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: namespaceAttribute2,
			},
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), namespaceAttribute1).Return(nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), namespaceAttribute2).Return(testError)

	token, err := s.dlqMessageHandler.Merge(context.Background(), lastMessageID, pageSize, pageToken)
	s.Equal(testError, err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_ThrowErrorOnDeleteMessages() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}
	messageID1 := int64(11)
	messageID2 := int64(12)
	testError := fmt.Errorf("test")
	namespaceAttribute1 := &replicationspb.NamespaceTaskAttributes{
		Id: uuid.New(),
	}
	namespaceAttribute2 := &replicationspb.NamespaceTaskAttributes{
		Id: uuid.New(),
	}
	tasks := []*replicationspb.ReplicationTask{
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: messageID1,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: namespaceAttribute1,
			},
		},
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: messageID2,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: namespaceAttribute2,
			},
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), namespaceAttribute1).Return(nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), namespaceAttribute2).Return(nil)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(gomock.Any(), ackLevel, messageID2).Return(testError)

	token, err := s.dlqMessageHandler.Merge(context.Background(), lastMessageID, pageSize, pageToken)
	s.Error(err)
	s.Nil(token)
}

func (s *dlqMessageHandlerSuite) TestMergeMessages_IgnoreErrorOnUpdateDLQAckLevel() {
	ackLevel := int64(10)
	lastMessageID := int64(20)
	pageSize := 100
	pageToken := []byte{}
	messageID := int64(11)
	testError := fmt.Errorf("test")
	namespaceAttribute := &replicationspb.NamespaceTaskAttributes{
		Id: uuid.New(),
	}

	tasks := []*replicationspb.ReplicationTask{
		{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			SourceTaskId: messageID,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: namespaceAttribute,
			},
		},
	}
	s.mockReplicationQueue.EXPECT().GetDLQAckLevel(gomock.Any()).Return(ackLevel, nil)
	s.mockReplicationQueue.EXPECT().GetMessagesFromDLQ(gomock.Any(), ackLevel, lastMessageID, pageSize, pageToken).
		Return(tasks, nil, nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), namespaceAttribute).Return(nil)
	s.mockReplicationQueue.EXPECT().RangeDeleteMessagesFromDLQ(gomock.Any(), ackLevel, messageID).Return(nil)
	s.mockReplicationQueue.EXPECT().UpdateDLQAckLevel(gomock.Any(), messageID).Return(testError)

	token, err := s.dlqMessageHandler.Merge(context.Background(), lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Nil(token)
}
