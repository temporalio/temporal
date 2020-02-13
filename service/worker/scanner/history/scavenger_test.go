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

package history

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	p "github.com/temporalio/temporal/common/persistence"
)

type (
	ScavengerTestSuite struct {
		suite.Suite
		logger log.Logger
		metric metrics.Client
	}
)

func TestScavengerTestSuite(t *testing.T) {
	suite.Run(t, new(ScavengerTestSuite))
}

func (s *ScavengerTestSuite) SetupTest() {
	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		s.Require().NoError(err)
	}
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.metric = metrics.NewClient(tally.NoopScope, metrics.Worker)
}

func (s *ScavengerTestSuite) createTestScavenger(rps int) (*mocks.HistoryV2Manager, *historyservicemock.MockHistoryServiceClient, *Scavenger, *gomock.Controller) {
	db := &mocks.HistoryV2Manager{}
	controller := gomock.NewController(s.T())
	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	scvgr := NewScavenger(db, 100, historyClient, ScavengerHeartbeatDetails{}, s.metric, s.logger)
	scvgr.isInTest = true
	return db, historyClient, scvgr, controller
}

func (s *ScavengerTestSuite) TestAllSkipTasksTwoPages() {
	db, _, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID1",
				BranchID: "branchID1",
				ForkTime: time.Now(),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID1", "workflowID1", "runID1"),
			},
			{
				TreeID:   "treeID2",
				BranchID: "branchID2",
				ForkTime: time.Now(),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID2", "workflowID2", "runID2"),
			},
		},
	}, nil).Once()

	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID3",
				BranchID: "branchID3",
				ForkTime: time.Now(),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID3", "workflowID3", "runID3"),
			},
			{
				TreeID:   "treeID4",
				BranchID: "branchID4",
				ForkTime: time.Now(),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID4", "workflowID4", "runID4"),
			},
		},
	}, nil).Once()

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(4, hbd.SkipCount)
	s.Equal(0, hbd.SuccCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestAllErrorSplittingTasksTwoPages() {
	db, _, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID1",
				BranchID: "branchID1",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     "error-info",
			},
			{
				TreeID:   "treeID2",
				BranchID: "branchID2",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     "error-info",
			},
		},
	}, nil).Once()

	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID3",
				BranchID: "branchID3",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     "error-info",
			},
			{
				TreeID:   "treeID4",
				BranchID: "branchID4",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     "error-info",
			},
		},
	}, nil).Once()

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(0, hbd.SuccCount)
	s.Equal(4, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestNoGarbageTwoPages() {
	db, client, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID1",
				BranchID: "branchID1",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID1", "workflowID1", "runID1"),
			},
			{
				TreeID:   "treeID2",
				BranchID: "branchID2",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID2", "workflowID2", "runID2"),
			},
		},
	}, nil).Once()

	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID3",
				BranchID: "branchID3",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID3", "workflowID3", "runID3"),
			},
			{
				TreeID:   "treeID4",
				BranchID: "branchID4",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID4", "workflowID4", "runID4"),
			},
		},
	}, nil).Once()

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID1",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID1",
			RunId:      "runID1",
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID2",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID3",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID4",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	}).Return(nil, nil)

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(4, hbd.SuccCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestDeletingBranchesTwoPages() {
	db, client, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID1",
				BranchID: "branchID1",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID1", "workflowID1", "runID1"),
			},
			{
				TreeID:   "treeID2",
				BranchID: "branchID2",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID2", "workflowID2", "runID2"),
			},
		},
	}, nil).Once()
	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		Branches: []p.HistoryBranchDetail{
			{
				TreeID:   "treeID3",
				BranchID: "branchID3",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID3", "workflowID3", "runID3"),
			},
			{
				TreeID:   "treeID4",
				BranchID: "branchID4",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID4", "workflowID4", "runID4"),
			},
		},
	}, nil).Once()

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID1",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID1",
			RunId:      "runID1",
		},
	}).Return(nil, status.Error(codes.NotFound, ""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID2",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
	}).Return(nil, status.Error(codes.NotFound, ""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID3",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	}).Return(nil, status.Error(codes.NotFound, ""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID4",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	}).Return(nil, status.Error(codes.NotFound, ""))

	branchToken1, err := p.NewHistoryBranchTokenByBranchID("treeID1", "branchID1")
	s.Nil(err)
	db.On("DeleteHistoryBranch", &p.DeleteHistoryBranchRequest{
		BranchToken: branchToken1,
		ShardID:     common.IntPtr(1),
	}).Return(nil).Once()
	branchToken2, err := p.NewHistoryBranchTokenByBranchID("treeID2", "branchID2")
	s.Nil(err)
	db.On("DeleteHistoryBranch", &p.DeleteHistoryBranchRequest{
		BranchToken: branchToken2,
		ShardID:     common.IntPtr(1),
	}).Return(nil).Once()
	branchToken3, err := p.NewHistoryBranchTokenByBranchID("treeID3", "branchID3")
	s.Nil(err)
	db.On("DeleteHistoryBranch", &p.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.IntPtr(1),
	}).Return(nil).Once()
	branchToken4, err := p.NewHistoryBranchTokenByBranchID("treeID4", "branchID4")
	s.Nil(err)
	db.On("DeleteHistoryBranch", &p.DeleteHistoryBranchRequest{
		BranchToken: branchToken4,
		ShardID:     common.IntPtr(1),
	}).Return(nil).Once()

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(4, hbd.SuccCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}

func (s *ScavengerTestSuite) TestMixesTwoPages() {
	db, client, scvgr, controller := s.createTestScavenger(100)
	defer controller.Finish()
	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize: pageSize,
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		NextPageToken: []byte("page1"),
		Branches: []p.HistoryBranchDetail{
			{
				// skip
				TreeID:   "treeID1",
				BranchID: "branchID1",
				ForkTime: time.Now(),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID1", "workflowID1", "runID1"),
			},
			{
				// split error
				TreeID:   "treeID2",
				BranchID: "branchID2",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     "error-info",
			},
		},
	}, nil).Once()
	db.On("GetAllHistoryTreeBranches", &p.GetAllHistoryTreeBranchesRequest{
		PageSize:      pageSize,
		NextPageToken: []byte("page1"),
	}).Return(&p.GetAllHistoryTreeBranchesResponse{
		Branches: []p.HistoryBranchDetail{
			{
				// delete succ
				TreeID:   "treeID3",
				BranchID: "branchID3",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID3", "workflowID3", "runID3"),
			},
			{
				// delete fail
				TreeID:   "treeID4",
				BranchID: "branchID4",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID4", "workflowID4", "runID4"),
			},
			{
				// not delete
				TreeID:   "treeID5",
				BranchID: "branchID5",
				ForkTime: time.Now().Add(-cleanUpThreshold * 2),
				Info:     p.BuildHistoryGarbageCleanupInfo("domainID5", "workflowID5", "runID5"),
			},
		},
	}, nil).Once()

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID3",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
	}).Return(nil, status.Error(codes.NotFound, ""))

	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID4",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
	}).Return(nil, status.Error(codes.NotFound, ""))
	client.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		DomainUUID: "domainID5",
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: "workflowID5",
			RunId:      "runID5",
		},
	}).Return(nil, nil)

	branchToken3, err := p.NewHistoryBranchTokenByBranchID("treeID3", "branchID3")
	s.Nil(err)
	db.On("DeleteHistoryBranch", &p.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.IntPtr(1),
	}).Return(nil).Once()

	branchToken4, err := p.NewHistoryBranchTokenByBranchID("treeID4", "branchID4")
	s.Nil(err)
	db.On("DeleteHistoryBranch", &p.DeleteHistoryBranchRequest{
		BranchToken: branchToken4,
		ShardID:     common.IntPtr(1),
	}).Return(fmt.Errorf("failed to delete history")).Once()

	hbd, err := scvgr.Run(context.Background())
	s.Nil(err)
	s.Equal(1, hbd.SkipCount)
	s.Equal(2, hbd.SuccCount)
	s.Equal(2, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}
