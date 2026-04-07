package workerdeployment

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	computepb "go.temporal.io/api/compute/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerDeploymentSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller             *gomock.Controller
	env                    *testsuite.TestWorkflowEnvironment
	workerDeploymentClient *ClientImpl

	workflowVersion DeploymentWorkflowVersion
}

func TestWorkerDeploymentSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &WorkerDeploymentSuite{workflowVersion: VersionDataRevisionNumber})
}

func (s *WorkerDeploymentSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflowWithOptions(s.getDeploymentWorkflowFunc(), workflow.RegisterOptions{Name: WorkerDeploymentWorkflowType})

	// Initialize an empty ClientImpl to use its helper methods
	s.workerDeploymentClient = &ClientImpl{}
}

func (s *WorkerDeploymentSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *WorkerDeploymentSuite) skipBeforeVersion(version DeploymentWorkflowVersion) {
	if s.workflowVersion < version {
		s.T().Skipf("test supports version %v and newer", version)
	}
}

func (s *WorkerDeploymentSuite) skipFromVersion(version DeploymentWorkflowVersion) {
	if s.workflowVersion >= version {
		s.T().Skipf("test supports version older than %v", version)
	}
}

// Test_CreateWorkerDeployment_Success tests successful handling of CreateWorkerDeployment update with matching request ID
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeployment_Success() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	requestID := tv.Any().String()
	identity := tv.ClientIdentity()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("CreateWorkerDeployment should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "CreateWorkerDeployment should complete without error")

				var resp deploymentspb.CreateWorkerDeploymentResponse
				err = s.env.GetWorkflowResult(&resp)
				s.Require().NoError(err)
				s.NotNil(resp.ConflictToken)
			},
		}, &deploymentspb.CreateWorkerDeploymentArgs{
			Identity:  identity,
			RequestId: requestID,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: requestID,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeployment_Idempotent tests that CreateWorkerDeployment with the same request ID is idempotent
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeployment_Idempotent() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	requestID := tv.Any().String()
	identity := tv.ClientIdentity()
	existingConflictToken := []byte("existing-token")

	// Send two identical CreateWorkerDeployment updates
	for i := range 2 {
		s.env.RegisterDelayedCallback(func() {
			s.env.UpdateWorkflow(CreateWorkerDeployment, "", &testsuite.TestUpdateCallback{
				OnReject: func(err error) {
					s.Fail("CreateWorkerDeployment should not have been rejected", err)
				},
				OnAccept: func() {},
				OnComplete: func(result any, err error) {
					s.Require().NoError(err, "CreateWorkerDeployment should be idempotent")

					resp, ok := result.(*deploymentspb.CreateWorkerDeploymentResponse)
					s.Require().True(ok, "response should be CreateWorkerDeploymentResponse")
					if i == 0 {
						existingConflictToken = resp.ConflictToken
					} else {
						s.Equal(existingConflictToken, resp.ConflictToken, "conflict token should not change for idempotent request")
					}
				},
			}, &deploymentspb.CreateWorkerDeploymentArgs{
				Identity:  identity,
				RequestId: requestID,
			})
		}, time.Duration(i+1)*time.Millisecond)
	}

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: requestID,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeployment_RejectDifferentRequestID tests that CreateWorkerDeployment with different request ID is rejected
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeployment_RejectDifferentRequestID() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	originalRequestID := tv.Any().String()
	differentRequestID := tv.Any().String()
	identity := tv.ClientIdentity()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Require().ErrorContains(err, errDeploymentAlreadyExists, "should reject with deployment already exists error")
			},
			OnAccept: func() {
				s.Fail("CreateWorkerDeployment should have been rejected")
			},
			OnComplete: func(result any, err error) {
				s.Fail("CreateWorkerDeployment should not have completed")
			},
		}, &deploymentspb.CreateWorkerDeploymentArgs{
			Identity:  identity,
			RequestId: differentRequestID,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: originalRequestID,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeployment_ReviveDeletedDeployment tests that CreateWorkerDeployment can revive a deleted deployment
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeployment_ReviveDeletedDeployment() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	newRequestID := tv.Any().String()
	identity := tv.ClientIdentity()
	syncBatchSize := int32(10)

	// First delete the deployment
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete deployment should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "delete deployment should complete without error")
			},
		}, nil)
	}, 1*time.Millisecond)

	// Then try to create it again with a new request ID (revival)
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("CreateWorkerDeployment should not have been rejected for deleted deployment", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "CreateWorkerDeployment should succeed for deleted deployment")

				resp, ok := result.(*deploymentspb.CreateWorkerDeploymentResponse)
				s.Require().True(ok, "response should be CreateWorkerDeploymentResponse")
				s.NotNil(resp.ConflictToken)

				// Verify the deployment was revived by checking a query
				val, err := s.env.QueryWorkflow(QueryDescribeDeployment)
				s.Require().NoError(err, "query should succeed after revival")

				var queryResp deploymentspb.QueryDescribeWorkerDeploymentResponse
				err = val.Get(&queryResp)
				s.Require().NoError(err)
				s.NotNil(queryResp.State)
				s.Equal(newRequestID, queryResp.State.CreateRequestId, "should have new request ID")
				s.Equal(identity, queryResp.State.LastModifierIdentity, "should have new identity")
				s.Equal(syncBatchSize, queryResp.State.SyncBatchSize, "should keep the original sync batch size")
			},
		}, &deploymentspb.CreateWorkerDeploymentArgs{
			Identity:  identity,
			RequestId: newRequestID,
		})
	}, 5*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "old-request-id",
			SyncBatchSize:   syncBatchSize,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_SetCurrentVersion_RejectStaleConcurrentUpdate tests that a stale concurrent update is rejected.
//
// The scenario that this test is testing is as follows:
// Two *identical* (same version, same identity) SetCurrentVersion updates are sent such that
// both of them are accepted by the validator and are scheduled to be processed.
// Since updates are processed sequentially, update #1 is processed first and then update #2.
// Update #2 should be rejected because by the time it is processed, update #1 would have been
// processed and the state would have changed.
func (s *WorkerDeploymentSuite) Test_SetCurrentVersion_RejectStaleConcurrentUpdate() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	updateArgs := &deploymentspb.SetCurrentVersionArgs{
		Identity:                tv.ClientIdentity(),
		Version:                 tv.DeploymentVersionString(),
		IgnoreMissingTaskQueues: true,
	}

	s.env.RegisterDelayedCallback(func() {
		// Firing update #1
		s.env.UpdateWorkflow(SetCurrentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #1 should not have failed with error %v", err)
			},
			OnAccept: func() {
				// Firing Update #2 which shall gets processed after Update #1 gets completed.
				s.env.UpdateWorkflow(SetCurrentVersion, "", &testsuite.TestUpdateCallback{
					OnReject: func(err error) {
						s.Fail("update #2 should have been accepted by the validator")
					},
					OnAccept: func() {
					},
					OnComplete: func(a any, err error) {
						// Update #2 clears the validator and waits for the first update to complete. Once it starts
						// being processed, it should be rejected since completion of the first update changed the state.
						s.Require().ErrorContains(err, errNoChangeType)
					},
				}, updateArgs)
			},
			OnComplete: func(a any, err error) {
			},
		}, updateArgs)

	}, 0*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		// Add version to deployment's local state since it's a prerequisite for SetCurrentVersion.
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				tv.DeploymentVersionString(): {
					Version: tv.DeploymentVersionString(),
				},
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_SetRampingVersion_RejectStaleConcurrentUpdate tests that a stale concurrent update is rejected.
//
// The scenario that this test is testing is as follows:
// Two *identical* (same version, same identity) SetRampingVersion updates are sent such that
// both of them are accepted by the validator and are scheduled to be processed.
// Since updates are processed sequentially, update #1 is processed first and then update #2.
// Update #2 should be rejected because by the time it is processed, update #1 would have been
// processed and the state would have changed.
func (s *WorkerDeploymentSuite) Test_SetRampingVersion_RejectStaleConcurrentUpdate() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Once().Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	updateArgs := &deploymentspb.SetRampingVersionArgs{
		Identity:                tv.ClientIdentity(),
		Version:                 tv.DeploymentVersionString(),
		Percentage:              50,
		IgnoreMissingTaskQueues: true,
	}

	s.env.RegisterDelayedCallback(func() {
		// Firing Update #1.
		s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update #1 should not have failed with error %v", err)
			},
			OnAccept: func() {
				// Firing Update #2 which shall get processed after Update #1 gets completed.
				s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
					OnReject: func(err error) {
						s.Fail("update #2 should have been accepted by the validator")
					},
					OnAccept: func() {
					},
					OnComplete: func(a any, err error) {
						// Update #2 clears the validator and waits for the first update to complete. Once it starts
						// being processed, it should be rejected since completion of the first update changed the state.
						s.Require().ErrorContains(err, errNoChangeType)
					},
				}, updateArgs)

			},
			OnComplete: func(a any, err error) {
			},
		}, updateArgs)

	}, 0*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				tv.DeploymentVersionString(): {
					Version: tv.DeploymentVersionString(),
				},
			},
		},
	})
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkerDeploymentSuite) Test_SyncUnversionedRamp_SingleTaskQueue() {
	s.skipFromVersion(AsyncSetCurrentAndRamping) // TODO (shahab): write replacement for async
	workers := 1
	s.syncUnversionedRampInBatches(workers)
}

func (s *WorkerDeploymentSuite) Test_SyncUnversionedRamp_MultipleTaskQueues() {
	s.skipFromVersion(AsyncSetCurrentAndRamping) // TODO (shahab): write replacement for async
	workers := 100
	s.syncUnversionedRampInBatches(workers)
}

func (s *WorkerDeploymentSuite) syncUnversionedRampInBatches(totalWorkers int) {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	taskQueueInfos := make([]*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo, totalWorkers)
	for i := range totalWorkers {
		taskQueueInfos[i] = &deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo{
			Name: tv.TaskQueue().Name + fmt.Sprintf("%03d", i),
			Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		}
	}
	// Mock the DescribeVersionFromWorkerDeployment activity to return numWorker taskQueues
	s.env.OnActivity(a.DescribeVersionFromWorkerDeployment, mock.Anything, mock.Anything).Return(
		&deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult{
			TaskQueueInfos: taskQueueInfos,
		}, nil)

	// Mock the SyncDeploymentVersionUserData activity and expect it to be called totalWorkers times
	var totalBatches int
	batchSize := int(s.workerDeploymentClient.getSyncBatchSize())
	if totalWorkers%batchSize == 0 {
		totalBatches = totalWorkers / batchSize
	} else {
		totalBatches = totalWorkers/batchSize + 1
	}

	s.env.OnActivity(a.SyncDeploymentVersionUserDataFromWorkerDeployment, mock.Anything, mock.Anything).Times(totalBatches).Return(nil, nil)

	s.env.RegisterDelayedCallback(func() {

		s.env.UpdateWorkflow(SetRampingVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("update failed with error %v", err)
			},
			OnAccept: func() {
			},
			OnComplete: func(a any, err error) {
			},
		}, &deploymentspb.SetRampingVersionArgs{
			Version: worker_versioning.UnversionedVersionId,
		})

	}, 0*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateTime:    timestamppb.New(time.Now()),
			SyncBatchSize: int32(s.workerDeploymentClient.getSyncBatchSize()), // initialize the sync batch size
			// Initialize the routing config with the current version (tv.DeploymentVersionString()).
			// This simulates a scenario where the worker deployment already has a current version,
			// which is a prerequisite for ramping to an unversioned state.
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: tv.DeploymentVersionString(),
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())

}

// Test_RevisionIncrementsWithAsyncSetCurrentAndRamping tests that revision number
// increments correctly when the workflow has the AsyncSetCurrentAndRamping version.
func (s *WorkerDeploymentSuite) Test_RevisionIncrementsWithAsyncSetCurrentAndRamping() {
	s.skipBeforeVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	version1 := tv.DeploymentVersionString()
	version2 := tv.DeploymentVersionString() + "-v2"

	// First, add version1 as current
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(SetCurrentVersion, version1, &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("SetCurrentVersion update should not have failed", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)

				// Query after SetRampingVersion - revision should be 1
				s.verifyRevisionNumber(1)
			},
		}, &deploymentspb.SetCurrentVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 version1,
			IgnoreMissingTaskQueues: true,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version1: {
					Version: version1,
				},
				version2: {
					Version: version2,
				},
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())

	// Now test SetRampingVersion
	s.env.UpdateWorkflow(SetRampingVersion, version2, &testsuite.TestUpdateCallback{
		OnReject: func(err error) {
			s.Fail("SetRampingVersion update should not have failed", err)
		},
		OnAccept: func() {},
		OnComplete: func(result any, err error) {
			s.Require().NoError(err)
			// After SetRamping completes, verify revision number is 2
			s.verifyRevisionNumber(2)
		},
	}, &deploymentspb.SetRampingVersionArgs{
		Identity:                tv.ClientIdentity(),
		Version:                 version2,
		Percentage:              50,
		IgnoreMissingTaskQueues: true,
	})
	s.True(s.env.IsWorkflowCompleted())
}

// Test_NoRevisionIncrementsWithoutAsyncSetCurrentAndRamping tests that revision number
// doesn't increment for old workflow.
func (s *WorkerDeploymentSuite) Test_NoRevisionIncrementsWithoutAsyncSetCurrentAndRamping() {
	s.skipFromVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{}, nil
		},
	)

	version1 := tv.DeploymentVersionString()
	version2 := tv.DeploymentVersionString() + "-v2"

	// First, add version1 as current
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(SetCurrentVersion, version1, &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("SetCurrentVersion update should not have failed", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)

				// Query after SetRampingVersion - revision should be 0
				s.verifyRevisionNumber(0)
			},
		}, &deploymentspb.SetCurrentVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 version1,
			IgnoreMissingTaskQueues: true,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version1: {
					Version: version1,
				},
				version2: {
					Version: version2,
				},
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())

	// Now test SetRampingVersion
	s.env.UpdateWorkflow(SetRampingVersion, version2, &testsuite.TestUpdateCallback{
		OnReject: func(err error) {
			s.Fail("SetRampingVersion update should not have failed", err)
		},
		OnAccept: func() {},
		OnComplete: func(result any, err error) {
			s.Require().NoError(err)
			// After SetRamping completes, verify revision number is 0
			s.verifyRevisionNumber(0)
		},
	}, &deploymentspb.SetRampingVersionArgs{
		Identity:                tv.ClientIdentity(),
		Version:                 version2,
		Percentage:              50,
		IgnoreMissingTaskQueues: true,
	})
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkerDeploymentSuite) getDeploymentWorkflowFunc() func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
	return func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		workflow.GetInfo(ctx).GetCurrentBuildID()
		// Use AsyncSetCurrentAndRamping version
		workflowVersionGetter := func() DeploymentWorkflowVersion {
			return s.workflowVersion
		}
		maxVersionsGetter := func() int {
			return 1000
		}
		return Workflow(ctx, workflowVersionGetter, maxVersionsGetter, args)
	}
}

func (s *WorkerDeploymentSuite) verifyRevisionNumber(expected int) {
	queryResult, err := s.env.QueryWorkflow(QueryDescribeDeployment)
	s.Require().NoError(err)
	var stateAfterSetRamping deploymentspb.QueryDescribeWorkerDeploymentResponse
	s.Require().NoError(queryResult.Get(&stateAfterSetRamping))
	s.Equal(int64(expected), stateAfterSetRamping.State.RoutingConfig.RevisionNumber)
}

// Test_RevisionNumberPassedToContinueAsNew tests that the revision number is
// preserved when the workflow continues as new.
func (s *WorkerDeploymentSuite) Test_RevisionNumberPassedToContinueAsNew() {
	s.skipBeforeVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{Summary: &deploymentspb.WorkerDeploymentVersionSummary{}}, nil
		},
	)

	version1 := tv.DeploymentVersionString()

	// Set current version to increment revision number
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(SetCurrentVersion, version1, &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("SetCurrentVersion update should not have failed", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)
				// Verify revision number is 46 after update
				s.verifyRevisionNumber(46)
			},
		}, &deploymentspb.SetCurrentVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 version1,
			IgnoreMissingTaskQueues: true,
		})
	}, 0*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version1: {
					Version: version1,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				RevisionNumber: 45,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Require().Error(err, "workflow should have continued as new with state")
	var workflowErr *temporal.WorkflowExecutionError
	s.Require().ErrorAs(err, &workflowErr, "error should be of type WorkflowExecutionError")
	var canErr *workflow.ContinueAsNewError
	s.Require().ErrorAs(workflowErr.Unwrap(), &canErr, "error should be of type ContinueAsNewError")
	s.Contains(string(canErr.Input.Payloads[0].Data), "\"revisionNumber\":\"46\"")
}

// Test_RevisionNumberDoesNotIncrementOnFailedSetCurrent tests that when SetCurrentVersion fails,
// the revision number is not incremented.
func (s *WorkerDeploymentSuite) Test_RevisionNumberDoesNotIncrementOnFailedSetCurrent() {
	s.skipBeforeVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	// Make the activity fail to simulate a failure scenario
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return nil, temporal.NewApplicationError("sync failed", "SyncFailure")
		},
	)

	version1 := tv.DeploymentVersionString()
	initialRevision := int64(5)

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(SetCurrentVersion, version1, &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("SetCurrentVersion update should have been accepted by validator")
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				// The update should fail due to activity failure
				s.Require().Error(err)
				s.Require().ErrorContains(err, "sync failed")

				// Verify revision number did NOT increment (still at initial value)
				s.verifyRevisionNumber(int(initialRevision))
			},
		}, &deploymentspb.SetCurrentVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 version1,
			IgnoreMissingTaskQueues: true,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version1: {
					Version: version1,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				RevisionNumber: initialRevision,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_RevisionNumberDoesNotIncrementOnFailedSetRamping tests that when SetRampingVersion fails,
// the revision number is not incremented.
func (s *WorkerDeploymentSuite) Test_RevisionNumberDoesNotIncrementOnFailedSetRamping() {
	s.skipBeforeVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	// Make the activity fail to simulate a failure scenario
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return nil, temporal.NewApplicationError("sync failed", "SyncFailure")
		},
	)

	version1 := tv.DeploymentVersionString()
	version2 := tv.DeploymentVersionString() + "-v2"
	initialRevision := int64(10)

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(SetRampingVersion, version2, &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("SetRampingVersion update should have been accepted by validator")
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				// The update should fail due to activity failure
				s.Require().Error(err)
				s.Require().ErrorContains(err, "sync failed")

				// Verify revision number did NOT increment (still at initial value)
				s.verifyRevisionNumber(int(initialRevision))
			},
		}, &deploymentspb.SetRampingVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 version2,
			Percentage:              50,
			IgnoreMissingTaskQueues: true,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version1: {
					Version: version1,
				},
				version2: {
					Version: version2,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: version1,
				RevisionNumber: initialRevision,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_HandlePropagationComplete tests that the deployment workflow properly handles
// propagation complete signals from version workflows
func (s *WorkerDeploymentSuite) Test_HandlePropagationComplete() {
	s.skipBeforeVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	buildID := tv.BuildID()
	revisionNumber := int64(5)

	// Setup initial state with a propagating revision
	s.env.RegisterDelayedCallback(func() {
		// Send propagation complete signal
		s.env.SignalWorkflow(PropagationCompleteSignal, &deploymentspb.PropagationCompletionInfo{
			BuildId:        buildID,
			RevisionNumber: revisionNumber,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			PropagatingRevisions: map[string]*deploymentspb.PropagatingRevisions{
				buildID: {
					RevisionNumbers: []int64{3, revisionNumber, 7},
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				RevisionNumber: 10,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())

	// Query to verify the revision was removed
	queryResult, err := s.env.QueryWorkflow(QueryDescribeDeployment)
	s.Require().NoError(err)
	var state deploymentspb.QueryDescribeWorkerDeploymentResponse
	s.Require().NoError(queryResult.Get(&state))

	// Verify the revision was removed from the propagating revisions
	s.Require().Contains(state.State.PropagatingRevisions, buildID)
	s.NotContains(state.State.PropagatingRevisions[buildID].RevisionNumbers, revisionNumber)
	s.Contains(state.State.PropagatingRevisions[buildID].RevisionNumbers, int64(3))
	s.Contains(state.State.PropagatingRevisions[buildID].RevisionNumbers, int64(7))
}

// Test_HandlePropagationComplete_RemovesEmptyBuildId tests that the deployment workflow
// removes empty build ID entries when the last revision completes propagation
func (s *WorkerDeploymentSuite) Test_HandlePropagationComplete_RemovesEmptyBuildId() {
	s.skipBeforeVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	buildID := tv.BuildID()
	revisionNumber := int64(5)

	// Setup initial state with only one revision for this build
	s.env.RegisterDelayedCallback(func() {
		// Send propagation complete signal
		s.env.SignalWorkflow(PropagationCompleteSignal, &deploymentspb.PropagationCompletionInfo{
			BuildId:        buildID,
			RevisionNumber: revisionNumber,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			PropagatingRevisions: map[string]*deploymentspb.PropagatingRevisions{
				buildID: {
					RevisionNumbers: []int64{revisionNumber},
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				RevisionNumber: 10,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())

	// Query to verify the build ID was removed entirely
	queryResult, err := s.env.QueryWorkflow(QueryDescribeDeployment)
	s.Require().NoError(err)
	var state deploymentspb.QueryDescribeWorkerDeploymentResponse
	s.Require().NoError(queryResult.Get(&state))

	// Verify the build ID entry was removed entirely
	s.NotContains(state.State.PropagatingRevisions, buildID)
}

// Test_DeleteDeployment_Success tests successful deletion of a deployment with no versions
func (s *WorkerDeploymentSuite) Test_DeleteDeployment_Success() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete deployment should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "delete deployment should complete without error")
			},
		}, nil) // DeleteDeployment takes no arguments
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteDeployment_FailsWithVersions tests that deletion fails when deployment has versions
func (s *WorkerDeploymentSuite) Test_DeleteDeployment_FailsWithVersions() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	version := tv.DeploymentVersionString()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				// The validator should reject this update
				s.Require().ErrorContains(err, "deployment has versions, can't be deleted")
			},
			OnAccept: func() {
				s.Fail("delete deployment should have been rejected by validator")
			},
			OnComplete: func(result any, err error) {
				s.Fail("delete deployment should not have reached completion")
			},
		}, nil)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version: {
					Version:    version,
					CreateTime: timestamppb.New(time.Now()),
					Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
				},
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteDeployment_QueryAfterDeletion tests that querying a deleted deployment returns an error
func (s *WorkerDeploymentSuite) Test_DeleteDeployment_QueryAfterDeletion() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	// Send delete update
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete deployment should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "delete deployment should complete without error")
			},
		}, nil)
	}, 1*time.Millisecond)

	// Query after deletion - should fail
	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow(QueryDescribeDeployment)
		s.Require().Error(err, "query should fail after deletion")
		s.Nil(val)
		s.Contains(err.Error(), errDeploymentDeleted)
	}, 5*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
}

// Test_DeleteDeployment_QueryBeforeDeletion tests that querying before deletion works normally
func (s *WorkerDeploymentSuite) Test_DeleteDeployment_QueryBeforeDeletion() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	// Query before deletion - should succeed
	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow(QueryDescribeDeployment)
		s.Require().NoError(err, "query should succeed before deletion")
		s.NotNil(val)

		var resp deploymentspb.QueryDescribeWorkerDeploymentResponse
		err = val.Get(&resp)
		s.Require().NoError(err)
		s.NotNil(resp.State)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_Success tests successful deletion of a version from deployment workflow
func (s *WorkerDeploymentSuite) Test_DeleteVersion_Success() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	version := tv.DeploymentVersionString()

	var a *Activities
	s.env.RegisterActivity(a.DeleteWorkerDeploymentVersion)
	s.env.OnActivity(a.DeleteWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(nil).Once()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete version should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "delete version should complete without error")
			},
		}, &deploymentspb.DeleteVersionArgs{
			Identity:     tv.ClientIdentity(),
			Version:      version,
			SkipDrainage: false,
		})
	}, 1*time.Millisecond)

	// Query after deletion to verify version was removed
	s.env.RegisterDelayedCallback(func() {
		queryResult, err := s.env.QueryWorkflow(QueryDescribeDeployment)
		s.Require().NoError(err)
		var state deploymentspb.QueryDescribeWorkerDeploymentResponse
		s.Require().NoError(queryResult.Get(&state))
		s.NotContains(state.State.Versions, version, "version should be removed from state after deletion")
	}, 50*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version: {
					Version:    version,
					CreateTime: timestamppb.New(time.Now()),
					Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: worker_versioning.UnversionedVersionId,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_FailsWhenCurrentOrRamping tests that deletion fails when version is current or ramping
func (s *WorkerDeploymentSuite) Test_DeleteVersion_FailsWhenCurrentOrRamping() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	version := tv.DeploymentVersionString()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				// The validator should reject this update
				s.Require().ErrorContains(err, fmt.Sprintf(ErrVersionIsCurrentOrRamping, tv.DeploymentVersionStringV32()))
			},
			OnAccept: func() {
				s.Fail("delete version should have been rejected by validator")
			},
			OnComplete: func(result any, err error) {
				s.Fail("delete version should not have reached completion")
			},
		}, &deploymentspb.DeleteVersionArgs{
			Identity:     tv.ClientIdentity(),
			Version:      version,
			SkipDrainage: false,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version: {
					Version:    version,
					CreateTime: timestamppb.New(time.Now()),
					Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: version, // Version is current
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_FailsWhenVersionNotFound tests that deletion fails when version doesn't exist
func (s *WorkerDeploymentSuite) Test_DeleteVersion_FailsWhenVersionNotFound() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	nonExistentVersion := tv.DeploymentVersionString() + "-not-exists"

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				// The validator should reject this update
				s.Require().ErrorContains(err, errVersionNotFound)
			},
			OnAccept: func() {
				s.Fail("delete version should have been rejected by validator")
			},
			OnComplete: func(result any, err error) {
				s.Fail("delete version should not have reached completion")
			},
		}, &deploymentspb.DeleteVersionArgs{
			Identity:     tv.ClientIdentity(),
			Version:      nonExistentVersion,
			SkipDrainage: false,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: worker_versioning.UnversionedVersionId,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_DeleteVersion_ConcurrentDeletes tests that concurrent deletion attempts are handled correctly
func (s *WorkerDeploymentSuite) Test_DeleteVersion_ConcurrentDeletes() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	version := tv.DeploymentVersionString()

	var a *Activities
	s.env.RegisterActivity(a.DeleteWorkerDeploymentVersion)
	s.env.OnActivity(a.DeleteWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(nil).Once() // Only one should succeed

	deleteArgs := &deploymentspb.DeleteVersionArgs{
		Identity:     tv.ClientIdentity(),
		Version:      version,
		SkipDrainage: false,
	}

	s.env.RegisterDelayedCallback(func() {
		// Fire first delete update
		s.env.UpdateWorkflow(DeleteVersion, "delete1", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("first delete should not have been rejected by validator", err)
			},
			OnAccept: func() {
				// Fire second delete update while first is processing
				s.env.UpdateWorkflow(DeleteVersion, "delete2", &testsuite.TestUpdateCallback{
					OnReject: func(err error) {
						s.Fail("second delete should have been accepted by validator")
					},
					OnAccept: func() {},
					OnComplete: func(result any, err error) {
						// Second delete should fail because version is already deleted
						s.Require().Error(err, "second delete should fail")
						s.Require().ErrorContains(err, errVersionNotFound)
					},
				}, deleteArgs)
			},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "first delete should complete without error")
			},
		}, deleteArgs)
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version: {
					Version:    version,
					CreateTime: timestamppb.New(time.Now()),
					Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: worker_versioning.UnversionedVersionId,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_SetCurrent_AddsPropagatingRevision tests that when SetCurrent is called in async mode,
// the propagating revision number is added to the state for tracking.
func (s *WorkerDeploymentSuite) Test_SetCurrent_AddsPropagatingRevision() {
	s.skipBeforeVersion(AsyncSetCurrentAndRamping)

	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	var a *Activities
	s.env.RegisterActivity(a.SyncWorkerDeploymentVersion)
	s.env.OnActivity(a.SyncWorkerDeploymentVersion, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
			return &deploymentspb.SyncVersionStateActivityResult{
				Summary: &deploymentspb.WorkerDeploymentVersionSummary{
					Version: args.Version,
				},
			}, nil
		},
	)

	version1 := tv.DeploymentVersionString()
	buildID := tv.BuildID()
	initialRevision := int64(5)

	// Set current version to trigger revision tracking
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(SetCurrentVersion, version1, &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("SetCurrentVersion update should not have failed", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)

				// Query the state to verify propagating revision was added
				queryResult, err := s.env.QueryWorkflow(QueryDescribeDeployment)
				s.Require().NoError(err)
				var state deploymentspb.QueryDescribeWorkerDeploymentResponse
				s.Require().NoError(queryResult.Get(&state))

				// Verify revision number incremented
				expectedRevision := initialRevision + 1
				s.Equal(expectedRevision, state.State.RoutingConfig.RevisionNumber,
					"revision number should be incremented")

				// Verify propagating revisions map contains the build ID
				s.Require().Contains(state.State.PropagatingRevisions, buildID,
					"propagating revisions should contain the build ID")

				// Verify the revision number is tracked for this build
				revisions := state.State.PropagatingRevisions[buildID].RevisionNumbers
				s.Require().Contains(revisions, expectedRevision,
					"propagating revisions should track the new revision number")
			},
		}, &deploymentspb.SetCurrentVersionArgs{
			Identity:                tv.ClientIdentity(),
			Version:                 version1,
			IgnoreMissingTaskQueues: true,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version1: {
					Version: version1,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				RevisionNumber: initialRevision,
			},
			PropagatingRevisions: make(map[string]*deploymentspb.PropagatingRevisions),
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_Success tests successful creation of a new version
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_Success() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	requestID := tv.Any().String()
	identity := tv.ClientIdentity()
	version := tv.DeploymentVersionString()

	var a *Activities
	s.env.RegisterActivity(a.StartWorkerDeploymentVersionWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentVersionWorkflow, mock.Anything, mock.MatchedBy(func(args *deploymentspb.StartWorkerDeploymentVersionRequest) bool {
		return args.DeploymentName == tv.DeploymentSeries() && args.BuildId == tv.BuildID() && args.RequestId == requestID
	})).Return(nil).Once()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("CreateWorkerDeploymentVersion should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err, "CreateWorkerDeploymentVersion should complete without error")

				// Verify version was added to state
				queryResult, err := s.env.QueryWorkflow(QueryDescribeDeployment)
				s.Require().NoError(err)
				var state deploymentspb.QueryDescribeWorkerDeploymentResponse
				s.Require().NoError(queryResult.Get(&state))
				s.Contains(state.State.Versions, version)
				s.Equal(version, state.State.Versions[version].Version)
				s.Equal(requestID, state.State.Versions[version].CreateRequestId)
				s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED, state.State.Versions[version].Status)
				s.NotNil(state.State.Versions[version].CreateTime)
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: requestID,
			Version:   version,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions:        map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_WithComputeConfig tests that creating a version with a
// compute config calls both ValidateWorkerControllerInstanceSpec and UpdateWorkerControllerInstance.
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_WithComputeConfig() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	requestID := tv.Any().String()
	identity := tv.ClientIdentity()
	version := tv.DeploymentVersionString()

	var a *Activities
	s.env.RegisterActivity(a.StartWorkerDeploymentVersionWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentVersionWorkflow, mock.Anything, mock.Anything).Return(nil).Once()

	s.env.RegisterActivity(a.UpdateWorkerControllerInstanceFromDeployment)
	updateCalled := false
	s.env.OnActivity(a.UpdateWorkerControllerInstanceFromDeployment, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		updateCalled = true
	}).Once()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("CreateWorkerDeploymentVersion should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)
				s.True(updateCalled, "UpdateWorkerControllerInstance should have been called")

				// Verify version was added to state.
				queryResult, qErr := s.env.QueryWorkflow(QueryDescribeDeployment)
				s.Require().NoError(qErr)
				var state deploymentspb.QueryDescribeWorkerDeploymentResponse
				s.Require().NoError(queryResult.Get(&state))
				s.Contains(state.State.Versions, version)
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: requestID,
			Version:   version,
			ComputeConfig: &computepb.ComputeConfig{
				ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
					"group1": {Provider: &computepb.ComputeProvider{Type: "aws-lambda"}},
				},
			},
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions:        map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_Idempotent tests that the same request ID is idempotent
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_Idempotent() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	requestID := tv.Any().String()
	identity := tv.ClientIdentity()
	version := tv.DeploymentVersionString()

	// Only register the activity once - the second update should not call it
	var a *Activities
	s.env.RegisterActivity(a.StartWorkerDeploymentVersionWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentVersionWorkflow, mock.Anything, mock.Anything).Return(nil).Once()

	// Send two identical CreateWorkerDeploymentVersion updates
	for i := range 2 {
		s.env.RegisterDelayedCallback(func() {
			s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
				OnReject: func(err error) {
					s.Fail("CreateWorkerDeploymentVersion should not have been rejected", err)
				},
				OnAccept: func() {},
				OnComplete: func(result any, err error) {
					s.Require().NoError(err, "CreateWorkerDeploymentVersion should be idempotent")
				},
			}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
				Identity:  identity,
				RequestId: requestID,
				Version:   version,
			})
		}, time.Duration(i+1)*time.Millisecond)
	}

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions:        map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_UpdateWorkerControllerInstanceFailure tests that a failure in
// UpdateWorkerControllerInstance prevents the version from being created.
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_UpdateWorkerControllerInstanceFailure() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	requestID := tv.Any().String()
	identity := tv.ClientIdentity()
	version := tv.DeploymentVersionString()

	var a *Activities
	s.env.RegisterActivity(a.UpdateWorkerControllerInstanceFromDeployment)
	s.env.OnActivity(a.UpdateWorkerControllerInstanceFromDeployment, mock.Anything, mock.Anything).Return(
		temporal.NewNonRetryableApplicationError("controller update failed", errInvalidComputeConfig, nil),
	).Once()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("should not be rejected at validator", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().Error(err)
				s.Require().ErrorContains(err, errInvalidComputeConfig)

				// Verify version was NOT added to state.
				queryResult, qErr := s.env.QueryWorkflow(QueryDescribeDeployment)
				s.Require().NoError(qErr)
				var state deploymentspb.QueryDescribeWorkerDeploymentResponse
				s.Require().NoError(queryResult.Get(&state))
				s.NotContains(state.State.Versions, version, "version should not be created when UpdateWorkerControllerInstance fails")
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: requestID,
			Version:   version,
			ComputeConfig: &computepb.ComputeConfig{
				ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
					"group1": {Provider: &computepb.ComputeProvider{Type: "bad-provider"}},
				},
			},
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions:        map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_RejectDifferentRequestID tests that a different request ID is rejected
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_RejectDifferentRequestID() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	version := tv.DeploymentVersionString()
	identity := tv.ClientIdentity()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Require().ErrorContains(err, errVersionAlreadyExists, "should reject with version already exists error")
			},
			OnAccept: func() {
				s.Fail("CreateWorkerDeploymentVersion should have been rejected")
			},
			OnComplete: func(result any, err error) {
				s.Fail("CreateWorkerDeploymentVersion should not have completed")
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: "different-request-id",
			Version:   version,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version: {
					Version:         version,
					CreateRequestId: "original-request-id",
					Status:          enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
				},
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_RejectAutoCreatedVersion tests that auto-created versions
// are rejected when an explicit create is attempted with a different request ID
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_RejectAutoCreatedVersion() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	version := tv.DeploymentVersionString()
	identity := tv.ClientIdentity()

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Require().ErrorContains(err, errVersionAlreadyExists, "should reject auto-created version with different request ID")
			},
			OnAccept: func() {
				s.Fail("CreateWorkerDeploymentVersion should have been rejected")
			},
			OnComplete: func(result any, err error) {
				s.Fail("CreateWorkerDeploymentVersion should not have completed")
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: "explicit-request-id",
			Version:   version,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				version: {
					Version:         version,
					CreateRequestId: AutoCreateRequestIDPrefix + "some-auto-id",
					Status:          enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
				},
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_RejectDeletedDeployment tests that creating a version
// on a deleted deployment is rejected
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_RejectDeletedDeployment() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	version := tv.DeploymentVersionString()
	identity := tv.ClientIdentity()

	// First delete the deployment
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(DeleteDeployment, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("delete deployment should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)
			},
		}, nil)
	}, 1*time.Millisecond)

	// Then try to create a version on the deleted deployment
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Require().ErrorContains(err, errDeploymentDeleted)
			},
			OnAccept: func() {
				s.Fail("CreateWorkerDeploymentVersion should have been rejected on deleted deployment")
			},
			OnComplete: func(result any, err error) {
				s.Fail("CreateWorkerDeploymentVersion should not have completed on deleted deployment")
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: "some-request-id",
			Version:   version,
		})
	}, 5*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions:        map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_MaxVersionsLimit tests that creating a version fails
// when the max versions limit is reached and no version can be auto-deleted
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_MaxVersionsLimit() {
	tv := testvars.New(s.T())

	newVersion := tv.DeploymentVersionString()
	identity := tv.ClientIdentity()

	// Pre-populate with a version that is current (can't be auto-deleted)
	existingVersion := tv.WithBuildIDNumber(2).DeploymentVersionString()

	// Use a custom workflow function with maxVersions=1
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		workflowVersionGetter := func() DeploymentWorkflowVersion {
			return s.workflowVersion
		}
		maxVersionsGetter := func() int {
			return 1 // Only allow 1 version
		}
		return Workflow(ctx, workflowVersionGetter, maxVersionsGetter, args)
	}, workflow.RegisterOptions{Name: WorkerDeploymentWorkflowType})
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("should have been accepted by validator", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().Error(err)
				s.Require().ErrorContains(err, errTooManyVersions)
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: "new-version-request-id",
			Version:   newVersion,
		})
	}, 1*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				existingVersion: {
					Version:    existingVersion,
					CreateTime: timestamppb.Now(),
					Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion: existingVersion,
			},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}

// Test_CreateWorkerDeploymentVersion_SyncSummaryPreservesCreateRequestID tests that
// syncing a version summary from the version workflow preserves the create_request_id
func (s *WorkerDeploymentSuite) Test_CreateWorkerDeploymentVersion_SyncSummaryPreservesCreateRequestID() {
	tv := testvars.New(s.T())
	s.env.OnUpsertMemo(mock.Anything).Return(nil)

	requestID := tv.Any().String()
	identity := tv.ClientIdentity()
	version := tv.DeploymentVersionString()

	var a *Activities
	s.env.RegisterActivity(a.StartWorkerDeploymentVersionWorkflow)
	s.env.OnActivity(a.StartWorkerDeploymentVersionWorkflow, mock.Anything, mock.Anything).Return(nil).Once()

	// First create the version
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(CreateWorkerDeploymentVersion, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				s.Fail("CreateWorkerDeploymentVersion should not have been rejected", err)
			},
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)
			},
		}, &deploymentspb.CreateWorkerDeploymentVersionArgs{
			Identity:  identity,
			RequestId: requestID,
			Version:   version,
		})
	}, 1*time.Millisecond)

	// Then send a SyncVersionSummary signal (simulating version workflow syncing back)
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SyncVersionSummarySignal, &deploymentspb.WorkerDeploymentVersionSummary{
			Version:    version,
			CreateTime: timestamppb.Now(),
			Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			// Note: no CreateRequestId in the signal - version workflow doesn't set it
		})
	}, 5*time.Millisecond)

	// Verify create_request_id is preserved after sync
	s.env.RegisterDelayedCallback(func() {
		queryResult, err := s.env.QueryWorkflow(QueryDescribeDeployment)
		s.Require().NoError(err)
		var state deploymentspb.QueryDescribeWorkerDeploymentResponse
		s.Require().NoError(queryResult.Get(&state))
		s.Require().Contains(state.State.Versions, version)
		s.Equal(requestID, state.State.Versions[version].CreateRequestId,
			"create_request_id should be preserved after summary sync")
	}, 10*time.Millisecond)

	s.env.ExecuteWorkflow(WorkerDeploymentWorkflowType, &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  tv.NamespaceName().String(),
		NamespaceId:    tv.NamespaceID().String(),
		DeploymentName: tv.DeploymentSeries(),
		State: &deploymentspb.WorkerDeploymentLocalState{
			CreateRequestId: "deployment-request-id",
			Versions:        map[string]*deploymentspb.WorkerDeploymentVersionSummary{},
		},
	})

	s.True(s.env.IsWorkflowCompleted())
}
