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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
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

func (s *ScavengerTestSuite) createTestScavenger(rps int) (*mocks.HistoryV2Manager, *historyservicetest.MockClient, *Scavenger, *gomock.Controller) {
	db := &mocks.HistoryV2Manager{}
	controller := gomock.NewController(s.T())
	workflowClient := historyservicetest.NewMockClient(controller)
	scvgr := NewScavenger(db, 100, workflowClient, ScavengerHeartbeatDetails{}, s.metric, s.logger)
	scvgr.isInTest = true
	return db, workflowClient, scvgr, controller
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

	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID1"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID1"),
			RunId:      common.StringPtr("runID1"),
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID2"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID2"),
			RunId:      common.StringPtr("runID2"),
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID3"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID3"),
			RunId:      common.StringPtr("runID3"),
		},
	}).Return(nil, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID4"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID4"),
			RunId:      common.StringPtr("runID4"),
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

	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID1"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID1"),
			RunId:      common.StringPtr("runID1"),
		},
	}).Return(nil, &shared.EntityNotExistsError{})
	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID2"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID2"),
			RunId:      common.StringPtr("runID2"),
		},
	}).Return(nil, &shared.EntityNotExistsError{})
	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID3"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID3"),
			RunId:      common.StringPtr("runID3"),
		},
	}).Return(nil, &shared.EntityNotExistsError{})
	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID4"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID4"),
			RunId:      common.StringPtr("runID4"),
		},
	}).Return(nil, &shared.EntityNotExistsError{})

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

	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID3"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID3"),
			RunId:      common.StringPtr("runID3"),
		},
	}).Return(nil, &shared.EntityNotExistsError{})

	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID4"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID4"),
			RunId:      common.StringPtr("runID4"),
		},
	}).Return(nil, &shared.EntityNotExistsError{})
	client.EXPECT().DescribeMutableState(gomock.Any(), &history.DescribeMutableStateRequest{
		DomainUUID: common.StringPtr("domainID5"),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID5"),
			RunId:      common.StringPtr("runID5"),
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
