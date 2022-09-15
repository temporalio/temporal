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
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	ScavengerTestSuite struct {
		suite.Suite

		logger log.Logger
		metric metrics.Client

		numShards int32
	}
)

var (
	treeID1 = primitives.MustValidateUUID("deadbeef-17ee-0000-0000-000000000001")
	treeID2 = primitives.MustValidateUUID("deadbeef-17ee-0000-0000-000000000002")
	treeID3 = primitives.MustValidateUUID("deadbeef-17ee-0000-0000-000000000003")
	treeID4 = primitives.MustValidateUUID("deadbeef-17ee-0000-0000-000000000004")
	treeID5 = primitives.MustValidateUUID("deadbeef-17ee-0000-0000-000000000005")

	branchID1 = primitives.MustValidateUUID("deadbeef-face-0000-0000-000000000001")
	branchID2 = primitives.MustValidateUUID("deadbeef-face-0000-0000-000000000002")
	branchID3 = primitives.MustValidateUUID("deadbeef-face-0000-0000-000000000003")
	branchID4 = primitives.MustValidateUUID("deadbeef-face-0000-0000-000000000004")
	branchID5 = primitives.MustValidateUUID("deadbeef-face-0000-0000-000000000005")
)

func TestScavengerTestSuite(t *testing.T) {
	suite.Run(t, new(ScavengerTestSuite))
}

func (s *ScavengerTestSuite) SetupTest() {
	s.logger = log.NewTestLogger()
	s.metric = metrics.NoopClient
	s.numShards = 512
}

func (s *ScavengerTestSuite) createTestScavenger(
	rps int,
) (*persistence.MockExecutionManager, *historyservicemock.MockHistoryServiceClient, *Scavenger, *gomock.Controller) {
	controller := gomock.NewController(s.T())
	db := persistence.NewMockExecutionManager(controller)
	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	dataAge := dynamicconfig.GetDurationPropertyFn(time.Hour)
	scvgr := NewScavenger(s.numShards, db, rps, historyClient, ScavengerHeartbeatDetails{}, dataAge, s.metric, s.logger)
	scvgr.isInTest = true
	return db, historyClient, scvgr, controller
}

func (s *ScavengerTestSuite) toBranchToken(treeID string, branchID string) []byte {
	data, err := persistence.NewHistoryBranchToken(treeID, branchID)
	s.NoError(err)
	return data
}

func (s *ScavengerTestSuite) TestAllSkipTasksTwoPages() {
	db, _, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken("treeID1", "branchID1"),
				ForkTime:    timestamp.TimeNowPtrUtc(),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchToken: s.toBranchToken("treeID2", "branchID2"),
				ForkTime:    timestamp.TimeNowPtrUtc(),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID2", "workflowID2", "runID2"),
			},
		},
	}, nil)

	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken("treeID3", "branchID3"),
				ForkTime:    timestamp.TimeNowPtrUtc(),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchToken: s.toBranchToken("treeID4", "branchID4"),
				ForkTime:    timestamp.TimeNowPtrUtc(),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
		},
	}, nil)

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(4, hbd.SkipCount)
	s.Equal(0, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestAllErrorSplittingTasksTwoPages() {
	db, _, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken("treeID1", "branchID1"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        "error-info",
			},
			{
				BranchToken: s.toBranchToken("treeID2", "branchID2"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        "error-info",
			},
		},
	}, nil)

	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken("treeID3", "branchID3"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        "error-info",
			},
			{
				BranchToken: s.toBranchToken("treeID4", "branchID4"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        "error-info",
			},
		},
	}, nil)

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(0, hbd.SuccessCount)
	s.Equal(4, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestNoGarbageTwoPages() {
	db, client, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken("treeID1", "branchID1"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchToken: s.toBranchToken("treeID2", "branchID2"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID2", "workflowID2", "runID2"),
			},
		},
	}, nil)

	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken("treeID3", "branchID3"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchToken: s.toBranchToken("treeID4", "branchID4"),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
		},
	}, nil)

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID1",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID1",
			RunId:      "runID1",
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	}).Return(nil, nil)

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(4, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestDeletingBranchesTwoPages() {
	db, client, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken(treeID1, branchID1),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchToken: s.toBranchToken(treeID2, branchID2),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID2", "workflowID2", "runID2"),
			},
		},
	}, nil)
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				BranchToken: s.toBranchToken(treeID3, branchID3),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchToken: s.toBranchToken(treeID4, branchID4),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
		},
	}, nil)

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID1",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID1",
			RunId:      "runID1",
		},
	}).Return(nil, serviceerror.NewNotFound(""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	}).Return(nil, serviceerror.NewNotFound(""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	}).Return(nil, serviceerror.NewNotFound(""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	}).Return(nil, serviceerror.NewNotFound(""))

	branchToken1, err := persistence.NewHistoryBranchToken(treeID1, branchID1)
	s.Nil(err)
	db.EXPECT().DeleteHistoryBranch(gomock.Any(), &persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken1,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID1", "workflowID1", s.numShards),
	}).Return(nil)
	branchToken2, err := persistence.NewHistoryBranchToken(treeID2, branchID2)
	s.Nil(err)
	db.EXPECT().DeleteHistoryBranch(gomock.Any(), &persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken2,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID2", "workflowID2", s.numShards),
	}).Return(nil)
	branchToken3, err := persistence.NewHistoryBranchToken(treeID3, branchID3)
	s.Nil(err)
	db.EXPECT().DeleteHistoryBranch(gomock.Any(), &persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID3", "workflowID3", s.numShards),
	}).Return(nil)
	branchToken4, err := persistence.NewHistoryBranchToken(treeID4, branchID4)
	s.Nil(err)
	db.EXPECT().DeleteHistoryBranch(gomock.Any(), &persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken4,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID4", "workflowID4", s.numShards),
	}).Return(nil)

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(4, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestMixesTwoPages() {
	db, client, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []persistence.HistoryBranchDetail{
			{
				// skip
				BranchToken: s.toBranchToken(treeID1, branchID1),
				ForkTime:    timestamp.TimeNowPtrUtc(),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				// split error
				BranchToken: s.toBranchToken(treeID2, branchID2),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        "error-info",
			},
		},
	}, nil)
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
		Branches: []persistence.HistoryBranchDetail{
			{
				// delete succ
				BranchToken: s.toBranchToken(treeID3, branchID3),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				// delete fail
				BranchToken: s.toBranchToken(treeID4, branchID4),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
			{
				// not delete
				BranchToken: s.toBranchToken(treeID5, branchID5),
				ForkTime:    timestamp.TimeNowPtrUtcAddDuration(-scvgr.historyDataMinAge() * 2),
				Info:        persistence.BuildHistoryGarbageCleanupInfo("namespaceID5", "workflowID5", "runID5"),
			},
		},
	}, nil)

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	}).Return(nil, serviceerror.NewNotFound(""))

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	}).Return(nil, serviceerror.NewNotFound(""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID5",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID5",
			RunId:      "runID5",
		},
	}).Return(nil, nil)

	branchToken3, err := persistence.NewHistoryBranchToken(treeID3, branchID3)
	s.Nil(err)
	db.EXPECT().DeleteHistoryBranch(gomock.Any(), &persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID3", "workflowID3", s.numShards),
	}).Return(nil)

	branchToken4, err := persistence.NewHistoryBranchToken(treeID4, branchID4)
	s.Nil(err)
	db.EXPECT().DeleteHistoryBranch(gomock.Any(), &persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken4,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID4", "workflowID4", s.numShards),
	}).Return(fmt.Errorf("failed to delete history"))

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(1, hbd.SkipCount)
	s.Equal(2, hbd.SuccessCount)
	s.Equal(2, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}
