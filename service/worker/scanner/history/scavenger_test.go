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

package history

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protomock"
)

type (
	ScavengerTestSuite struct {
		suite.Suite
		controller *gomock.Controller

		logger        log.Logger
		metricHandler metrics.Handler

		numShards int32

		mockExecutionManager *persistence.MockExecutionManager
		mockHistoryClient    *historyservicemock.MockHistoryServiceClient
		mockAdminClient      *adminservicemock.MockAdminServiceClient
		mockRegistry         *namespace.MockRegistry
		scavenger            *Scavenger
	}
)

var (
	treeID1 = primitives.MustValidateUUID(uuid.New())
	treeID2 = primitives.MustValidateUUID(uuid.New())
	treeID3 = primitives.MustValidateUUID(uuid.New())
	treeID4 = primitives.MustValidateUUID(uuid.New())
	treeID5 = primitives.MustValidateUUID(uuid.New())

	branchID1 = primitives.MustValidateUUID(uuid.New())
	branchID2 = primitives.MustValidateUUID(uuid.New())
	branchID3 = primitives.MustValidateUUID(uuid.New())
	branchID4 = primitives.MustValidateUUID(uuid.New())
	branchID5 = primitives.MustValidateUUID(uuid.New())
)

func TestScavengerTestSuite(t *testing.T) {
	suite.Run(t, new(ScavengerTestSuite))
}

func (s *ScavengerTestSuite) SetupTest() {
	s.logger = log.NewTestLogger()
	s.metricHandler = metrics.NoopMetricsHandler
	s.numShards = 512
	s.createTestScavenger(100)
}

func (s *ScavengerTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ScavengerTestSuite) createTestScavenger(
	rps int,
) {
	s.controller = gomock.NewController(s.T())
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockRegistry = namespace.NewMockRegistry(s.controller)
	dataAge := dynamicconfig.GetDurationPropertyFn(time.Hour)
	executionDataAge := dynamicconfig.GetDurationPropertyFn(time.Second)
	enableRetentionVerification := dynamicconfig.GetBoolPropertyFn(true)
	s.scavenger = NewScavenger(
		s.numShards,
		s.mockExecutionManager,
		rps,
		s.mockHistoryClient,
		s.mockAdminClient,
		s.mockRegistry,
		ScavengerHeartbeatDetails{},
		dataAge,
		executionDataAge,
		enableRetentionVerification,
		s.metricHandler,
		s.logger,
	)
	s.scavenger.isInTest = true
}

func (s *ScavengerTestSuite) TestAllSkipTasksTwoPages() {
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID2,
					BranchId: branchID2,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID2", "workflowID2", "runID2"),
			},
		},
	}, nil)

	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
		},
	}, nil)

	hbd, err := s.scavenger.Run(context.Background())
	s.Nil(err)
	s.Equal(4, hbd.SkipCount)
	s.Equal(0, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestAllErrorSplittingTasksTwoPages() {
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     "error-info",
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID2,
					BranchId: branchID2,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     "error-info",
			},
		},
	}, nil)

	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     "error-info",
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     "error-info",
			},
		},
	}, nil)

	hbd, err := s.scavenger.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(0, hbd.SuccessCount)
	s.Equal(4, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestNoGarbageTwoPages() {
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID2,
					BranchId: branchID2,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID2", "workflowID2", "runID2"),
			},
		},
	}, nil)

	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
		},
	}, nil)
	mockedNamespace := namespace.NewNamespaceForTest(
		nil,
		&persistencepb.NamespaceConfig{Retention: durationpb.New(time.Hour)},
		false,
		nil,
		0,
	)
	ms := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
				LastUpdateTime: timestamppb.New(time.Now()),
			},
		},
	}
	s.mockRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(mockedNamespace, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID1",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID1",
			RunId:      "runID1",
		},
	})).Return(ms, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	})).Return(ms, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	})).Return(ms, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	})).Return(ms, nil)

	hbd, err := s.scavenger.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(4, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestDeletingBranchesTwoPages() {
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID2,
					BranchId: branchID2,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID2", "workflowID2", "runID2"),
			},
		},
	}, nil)
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
		},
	}, nil)

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID1",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID1",
			RunId:      "runID1",
		},
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	})).Return(nil, serviceerror.NewNotFound(""))

	branchToken1, err := persistence.NewHistoryBranch(treeID1, &branchID1, []*persistencepb.HistoryBranchRange{})
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken1,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID1", "workflowID1", s.numShards),
	})).Return(nil)
	branchToken2, err := persistence.NewHistoryBranch(treeID2, &branchID2, []*persistencepb.HistoryBranchRange{})
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken2,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID2", "workflowID2", s.numShards),
	})).Return(nil)
	branchToken3, err := persistence.NewHistoryBranch(treeID3, &branchID3, []*persistencepb.HistoryBranchRange{})
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID3", "workflowID3", s.numShards),
	})).Return(nil)
	branchToken4, err := persistence.NewHistoryBranch(treeID4, &branchID4, []*persistencepb.HistoryBranchRange{})
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken4,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID4", "workflowID4", s.numShards),
	})).Return(nil)

	hbd, err := s.scavenger.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(4, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestMixesTwoPages() {
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				// skip
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				// split error
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID2,
					BranchId: branchID2,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     "error-info",
			},
		},
	}, nil)
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				// delete succ
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				// delete fail
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
			{
				// not delete
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID5,
					BranchId: branchID5,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID5", "workflowID5", "runID5"),
			},
		},
	}, nil)

	mockedNamespace := namespace.NewNamespaceForTest(
		nil,
		&persistencepb.NamespaceConfig{Retention: durationpb.New(time.Hour)},
		false,
		nil,
		0,
	)
	ms := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
				LastUpdateTime: timestamppb.New(time.Now()),
			},
		},
	}
	s.mockRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(mockedNamespace, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	})).Return(nil, serviceerror.NewNotFound(""))

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID5",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID5",
			RunId:      "runID5",
		},
	})).Return(ms, nil)

	branchToken3, err := persistence.NewHistoryBranch(treeID3, &branchID3, []*persistencepb.HistoryBranchRange{})
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID3", "workflowID3", s.numShards),
	})).Return(nil)

	branchToken4, err := persistence.NewHistoryBranch(treeID4, &branchID4, []*persistencepb.HistoryBranchRange{})
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken4,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID4", "workflowID4", s.numShards),
	})).Return(fmt.Errorf("failed to delete history"))

	hbd, err := s.scavenger.Run(context.Background())
	s.Nil(err)
	s.Equal(1, hbd.SkipCount)
	s.Equal(2, hbd.SuccessCount)
	s.Equal(2, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestDeleteWorkflowAfterRetention() {
	retention := time.Hour
	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID2,
					BranchId: branchID2,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID2", "workflowID2", "runID2"),
			},
		},
	}, nil)

	s.mockExecutionManager.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), protomock.Eq(&persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	})).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
			{
				BranchInfo: &persistencepb.HistoryBranch{
					TreeId:   treeID5,
					BranchId: branchID5,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID5", "workflowID5", "runID5"),
			},
		},
	}, nil)
	mockedNamespace := namespace.NewNamespaceForTest(
		nil,
		&persistencepb.NamespaceConfig{Retention: durationpb.New(retention)},
		false,
		nil,
		0,
	)
	workflowInRetention := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
				LastUpdateTime: timestamppb.New(time.Now()),
			},
		},
	}
	workflowPastRetention2 := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
				WorkflowId:     "workflowID2",
				NamespaceId:    "namespaceID2",
				LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-time.Hour * 24)),
			},
			ExecutionState: &persistencepb.WorkflowExecutionState{
				RunId: "runID2",
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
	}
	workflowPastRetention4 := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
				WorkflowId:     "workflowID4",
				NamespaceId:    "namespaceID4",
				LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-time.Hour * 24)),
			},
			ExecutionState: &persistencepb.WorkflowExecutionState{
				RunId: "runID4",
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
	}
	runningWorkflow5 := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
				WorkflowId:     "workflowID5",
				NamespaceId:    "namespaceID5",
				LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-time.Hour * 24)),
			},
			ExecutionState: &persistencepb.WorkflowExecutionState{
				RunId: "runID5",
				State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			},
		},
	}
	s.mockRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(mockedNamespace, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID1",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID1",
			RunId:      "runID1",
		},
	})).Return(workflowInRetention, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	})).Return(workflowPastRetention2, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	})).Return(workflowInRetention, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	})).Return(workflowPastRetention4, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID5",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID5",
			RunId:      "runID5",
		},
	})).Return(runningWorkflow5, nil)
	s.mockAdminClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), protomock.Eq(&adminservice.DeleteWorkflowExecutionRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	})).Return(nil, nil).Times(1)
	s.mockAdminClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), protomock.Eq(&adminservice.DeleteWorkflowExecutionRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	})).Return(nil, nil).Times(1)

	hbd, err := s.scavenger.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(5, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}
