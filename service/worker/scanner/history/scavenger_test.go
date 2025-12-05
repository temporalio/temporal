package history

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protomock"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		historyBranchUtil    persistence.HistoryBranchUtilImpl
	}
)

var (
	treeID1 = primitives.MustValidateUUID(uuid.NewString())
	treeID2 = primitives.MustValidateUUID(uuid.NewString())
	treeID3 = primitives.MustValidateUUID(uuid.NewString())
	treeID4 = primitives.MustValidateUUID(uuid.NewString())
	treeID5 = primitives.MustValidateUUID(uuid.NewString())

	branchID1 = primitives.MustValidateUUID(uuid.NewString())
	branchID2 = primitives.MustValidateUUID(uuid.NewString())
	branchID3 = primitives.MustValidateUUID(uuid.NewString())
	branchID4 = primitives.MustValidateUUID(uuid.NewString())
	branchID5 = primitives.MustValidateUUID(uuid.NewString())
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     "error-info",
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     "error-info",
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
		&persistencespb.NamespaceConfig{Retention: durationpb.New(time.Hour)},
		false,
		nil,
		0,
	)
	ms := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
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
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(ms, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(ms, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(ms, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(nil, serviceerror.NewNotFound(""))
	branchToken1, err := s.historyBranchUtil.NewHistoryBranch(uuid.NewString(), uuid.NewString(), uuid.NewString(), treeID1, &branchID1, []*persistencespb.HistoryBranchRange{}, 0, 0, 0)
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken1,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID1", "workflowID1", s.numShards),
	})).Return(nil)
	branchToken2, err := s.historyBranchUtil.NewHistoryBranch(uuid.NewString(), uuid.NewString(), uuid.NewString(), treeID2, &branchID2, []*persistencespb.HistoryBranchRange{}, 0, 0, 0)
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken2,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID2", "workflowID2", s.numShards),
	})).Return(nil)
	branchToken3, err := s.historyBranchUtil.NewHistoryBranch(uuid.NewString(), uuid.NewString(), uuid.NewString(), treeID3, &branchID3, []*persistencespb.HistoryBranchRange{}, 0, 0, 0)
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID3", "workflowID3", s.numShards),
	})).Return(nil)
	branchToken4, err := s.historyBranchUtil.NewHistoryBranch(uuid.NewString(), uuid.NewString(), uuid.NewString(), treeID4, &branchID4, []*persistencespb.HistoryBranchRange{}, 0, 0, 0)
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtc(),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				// split error
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				// delete fail
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-s.scavenger.historyDataMinAge() * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
			{
				// not delete
				BranchInfo: &persistencespb.HistoryBranch{
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
		&persistencespb.NamespaceConfig{Retention: durationpb.New(time.Hour)},
		false,
		nil,
		0,
	)
	ms := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
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
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(nil, serviceerror.NewNotFound(""))

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID5",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID5",
			RunId:      "runID5",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(ms, nil)

	branchToken3, err := s.historyBranchUtil.NewHistoryBranch(uuid.NewString(), uuid.NewString(), uuid.NewString(), treeID3, &branchID3, []*persistencespb.HistoryBranchRange{}, 0, 0, 0)
	s.Nil(err)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), protomock.Eq(&persistence.DeleteHistoryBranchRequest{
		BranchToken: branchToken3,
		ShardID:     common.WorkflowIDToHistoryShard("namespaceID3", "workflowID3", s.numShards),
	})).Return(nil)

	branchToken4, err := s.historyBranchUtil.NewHistoryBranch(uuid.NewString(), uuid.NewString(), uuid.NewString(), treeID4, &branchID4, []*persistencespb.HistoryBranchRange{}, 0, 0, 0)
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID1,
					BranchId: branchID1,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID1", "workflowID1", "runID1"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID3,
					BranchId: branchID3,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID3", "workflowID3", "runID3"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   treeID4,
					BranchId: branchID4,
				},
				ForkTime: timestamp.TimeNowPtrUtcAddDuration(-retention * 2),
				Info:     persistence.BuildHistoryGarbageCleanupInfo("namespaceID4", "workflowID4", "runID4"),
			},
			{
				BranchInfo: &persistencespb.HistoryBranch{
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
		&persistencespb.NamespaceConfig{Retention: durationpb.New(retention)},
		false,
		nil,
		0,
	)
	workflowInRetention := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				LastUpdateTime: timestamppb.New(time.Now()),
			},
		},
	}
	workflowPastRetention2 := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:     "workflowID2",
				NamespaceId:    "namespaceID2",
				LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-time.Hour * 24)),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: "runID2",
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
	}
	workflowPastRetention4 := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:     "workflowID4",
				NamespaceId:    "namespaceID4",
				LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-time.Hour * 24)),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: "runID4",
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
	}
	runningWorkflow5 := &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:     "workflowID5",
				NamespaceId:    "namespaceID5",
				LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-time.Hour * 24)),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
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
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(workflowInRetention, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID2",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(workflowPastRetention2, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID3",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID3",
			RunId:      "runID3",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(workflowInRetention, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID4",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(workflowPastRetention4, nil)
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: "namespaceID5",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID5",
			RunId:      "runID5",
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})).Return(runningWorkflow5, nil)
	s.mockAdminClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), protomock.Eq(&adminservice.DeleteWorkflowExecutionRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID2",
			RunId:      "runID2",
		},
		Archetype: chasm.WorkflowArchetype,
	})).Return(nil, nil).Times(1)
	s.mockAdminClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), protomock.Eq(&adminservice.DeleteWorkflowExecutionRequest{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID4",
			RunId:      "runID4",
		},
		Archetype: chasm.WorkflowArchetype,
	})).Return(nil, nil).Times(1)

	hbd, err := s.scavenger.Run(context.Background())
	s.Nil(err)
	s.Equal(0, hbd.SkipCount)
	s.Equal(5, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(2, hbd.CurrentPage)
	s.Equal(0, len(hbd.NextPageToken))
}
