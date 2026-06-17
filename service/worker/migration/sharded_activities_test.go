package migration

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protomock"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Sharded-activity tests reuse activitiesSuite's SetupTest so they get the
// same mock graph (HistoryClient, AdminClient, ChasmRegistry, etc.) as the
// legacy force-replication activity tests. ReplicateBatch resolves the
// remote admin client via clientBean.GetRemoteAdminClient, which the suite
// already arms with mockRemoteAdminClient — no per-test setup needed.

// payloadFor wraps a single ExecutionInfo into a BatchPayload on the named
// shard. Tests that need multiple execs across shards build the BatchPayload
// inline.
func payloadFor(shard int32, ex *ExecutionInfo) BatchPayload {
	return BatchPayload{
		shard: {ex.BusinessID: {{RunID: ex.RunID, ArchetypeID: ex.ArchetypeID}}},
	}
}

// newShardedReq builds a shardedBatchReq with sensible defaults for unit
// tests. ShardNoProgress is large so the stuck-shard backstop doesn't trip
// from real wall-clock latency; IdleShardCost is large so maybeSignalRelease
// (which needs the sdkClientFactory, nil here) doesn't fire.
func newShardedReq(execs BatchPayload) *shardedBatchReq {
	return &shardedBatchReq{
		BatchID:             1,
		Namespace:           mockedNamespace,
		NamespaceID:         mockedNamespaceID,
		Executions:          execs,
		TargetClusterName:   remoteCluster,
		PerBatchGenerateRPS: defaultPerBatchGenerateRPS,
		ShardNoProgress:     time.Hour,
		IdleShardCost:       time.Hour,
	}
}

// expectRemoteNotFound primes the remote admin client to return NotFound
// for the given exec — the trigger for the verify-skip code path that
// consults source DMS to decide between zombie/retention skip vs. real
// pending state.
func (s *activitiesSuite) expectRemoteNotFound(ex *ExecutionInfo) {
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: ex.BusinessID,
			RunId:      ex.RunID,
		},
		Archetype:       chasm.WorkflowArchetype,
		ArchetypeId:     ex.ArchetypeID,
		SkipForceReload: true,
	})).Return(nil, serviceerror.NewNotFound("")).Times(1)
}

func (s *activitiesSuite) expectSourceDMS(ex *ExecutionInfo, resp *historyservice.DescribeMutableStateResponse, err error) {
	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: ex.BusinessID,
			RunId:      ex.RunID,
		},
		ArchetypeId:     ex.ArchetypeID,
		SkipForceReload: true,
	})).Return(resp, err).Times(1)
}

// TestReplicateBatch_Success exercises the full inject+verify happy path
// for one exec: the inject phase calls GenerateLastHistoryReplicationTasks
// against HistoryClient, then the verify phase's DescribeMutableState on
// the remote admin client returns OK so workflowVerifier marks it
// verified. Mirrors the legacy TestVerifyReplicationTasks_Success and
// TestGenerateReplicationTasks_Success.
func (s *activitiesSuite) TestReplicateBatch_Success() {
	env, _ := s.initEnv()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(mockedNamespaceID)).
		Return(&testNamespace, nil).Times(1)

	// Inject phase calls HistoryClient (generateMigrationTaskViaFrontend=false).
	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), protomock.Eq(&historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution1.BusinessID,
			RunId:      execution1.RunID,
		},
		ArchetypeId:    execution1.ArchetypeID,
		TargetClusters: []string{remoteCluster},
	})).Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)

	// Verify phase: remote DMS returns OK; workflowVerifierProvider
	// returns verified=true unconditionally so the exec verifies on the
	// first pass.
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution1.BusinessID,
			RunId:      execution1.RunID,
		},
		Archetype:       chasm.WorkflowArchetype,
		ArchetypeId:     execution1.ArchetypeID,
		SkipForceReload: true,
	})).Return(&adminservice.DescribeMutableStateResponse{}, nil).Times(1)

	req := newShardedReq(payloadFor(0, execution1))
	f, err := env.ExecuteActivity(s.a.ReplicateBatch, req)
	s.NoError(err)
	var out replicateBatchResult
	s.NoError(f.Get(&out))
	s.Equal(int64(1), out.VerifiedCount)
	s.Equal([]int32{0}, out.CompletedShards)
}

// TestReplicateBatch_SkipZombie exercises the retention/zombie skip path:
// remote DMS returns NotFound, source DMS returns a zombie state, and
// checkSkipWorkflowExecution marks the exec as verified-via-skip so the
// shard's verify accounting completes. Mirrors the existing
// TestVerifyReplicationTasks_SkipWorkflowExecution.
func (s *activitiesSuite) TestReplicateBatch_SkipZombie() {
	env, _ := s.initEnv()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(mockedNamespaceID)).
		Return(&testNamespace, nil).Times(1)

	// Seed InjectDone=true in heartbeat to skip inject and only exercise the verify-skip path.
	env.SetHeartbeatDetails(replicateBatchHeartbeat{InjectDone: true})

	req := newShardedReq(payloadFor(0, execution1))

	s.expectRemoteNotFound(execution1)
	s.expectSourceDMS(execution1, zombieState, nil)

	f, err := env.ExecuteActivity(s.a.ReplicateBatch, req)
	s.NoError(err)
	var out replicateBatchResult
	s.NoError(f.Get(&out))
	s.Equal(int64(1), out.VerifiedCount)
}

// TestReplicateBatch_SkipRetention exercises the close-time/retention skip
// path: remote DMS returns NotFound, source DMS returns a completed
// workflow whose CloseTime+Retention is in the past, so
// checkSkipWorkflowExecution marks it skipped (counted as verified).
// Mirrors the existing Test_verifyReplicationTasksSkipRetention.
func (s *activitiesSuite) TestReplicateBatch_SkipRetention() {
	env, _ := s.initEnv()

	retention := time.Hour
	closeTime := time.Now().Add(-2 * retention) // deleteTime is in the past

	// Build a real namespace.Namespace with a retention setting so
	// checkSkipWorkflowExecution's `ns.Retention()` returns non-zero.
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{},
		Config: &persistencespb.NamespaceConfig{
			Retention: durationpb.New(retention),
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
	}
	ns, nsErr := namespace.FromPersistentState(detail, factory(detail))
	s.NoError(nsErr)
	// Override the suite-default GetNamespaceByID for this test so the
	// activity sees a namespace with retention configured.
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(mockedNamespaceID)).
		Return(ns, nil).Times(1)

	// Seed InjectDone=true in heartbeat to skip inject.
	env.SetHeartbeatDetails(replicateBatchHeartbeat{InjectDone: true})

	s.expectRemoteNotFound(execution1)
	s.expectSourceDMS(execution1, &historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionState: &persistencespb.WorkflowExecutionState{
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				CloseTime: timestamppb.New(closeTime),
			},
		},
	}, nil)

	req := newShardedReq(payloadFor(0, execution1))
	f, err := env.ExecuteActivity(s.a.ReplicateBatch, req)
	s.NoError(err)
	var out replicateBatchResult
	s.NoError(f.Get(&out))
	s.Equal(int64(1), out.VerifiedCount)
}

// TestReplicateBatch_ShardNoProgress: the per-shard cumulative no-progress
// backstop fires non-retryably when a shard has gone longer than
// req.ShardNoProgress without a verified outcome. Heartbeat InjectDone=true
// skips inject so we go straight to verify; the remote returns BUSY so
// nothing verifies. With ShardNoProgress=time.Nanosecond the check fires
// on the first pass. Mirrors TestVerifyReplicationTasks_FailedNotFound.
func (s *activitiesSuite) TestReplicateBatch_ShardNoProgress() {
	env, _ := s.initEnv()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(mockedNamespaceID)).
		Return(&testNamespace, nil).Times(1)

	// Remote returns BUSY_WORKFLOW so verify counts the exec as
	// pending without consulting source — that keeps the shard's
	// lastProgress at its seeded (already-stale) value.
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.ResourceExhausted{
			Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		}).AnyTimes()

	// Seed InjectDone=true to skip inject, go straight to verify.
	env.SetHeartbeatDetails(replicateBatchHeartbeat{InjectDone: true})

	req := newShardedReq(payloadFor(0, execution1))
	req.ShardNoProgress = time.Nanosecond // trip almost immediately

	_, err := env.ExecuteActivity(s.a.ReplicateBatch, req)
	s.Error(err)
	var appErr *temporal.ApplicationError
	s.ErrorAs(err, &appErr)
	s.Equal("ShardNoProgress", appErr.Type())
	s.True(appErr.NonRetryable(), "ShardNoProgress should be non-retryable")
}

// TestReplicateBatch_DisableVerification: with verification disabled the
// activity runs inject, then returns immediately with VerifiedCount=0 and
// every batch shard listed as completed — no DMS calls. Mirrors the
// workflow-level TestSharded_DisableVerification_NoVerifiedCount but
// from the activity side.
func (s *activitiesSuite) TestReplicateBatch_DisableVerification() {
	env, _ := s.initEnv()
	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), gomock.Any()).
		Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)

	req := newShardedReq(payloadFor(0, execution1))
	req.DisableVerification = true
	f, err := env.ExecuteActivity(s.a.ReplicateBatch, req)
	s.NoError(err)
	var out replicateBatchResult
	s.NoError(f.Get(&out))
	s.Equal(int64(0), out.VerifiedCount)
	s.Equal([]int32{0}, out.CompletedShards)
}

// TestReplicateBatch_HeartbeatResumesInject: a recorded NextInjectIdx
// heartbeat from a prior attempt causes the inject phase to skip
// already-injected execs. Pre-seeds heartbeat NextInjectIdx=1 across a
// two-exec batch, then asserts only the second exec's
// GenerateLastHistoryReplicationTasks is invoked. Mirrors the legacy
// TestGenerateReplicationTasks_Success_ViaFrontend's heartbeat-resume
// assertion.
func (s *activitiesSuite) TestReplicateBatch_HeartbeatResumesInject() {
	env, _ := s.initEnv()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(mockedNamespaceID)).
		Return(&testNamespace, nil).Times(1)

	// Pre-seed the heartbeat so inject resumes at index 1.
	env.SetHeartbeatDetails(replicateBatchHeartbeat{NextInjectIdx: 1, InjectDone: false})

	// Two execs on shard 0. flatten() orders by BID alphabetically, so
	// index 0 = execution1 ("workflow1"), index 1 = execution2 ("workflow2").
	payload := BatchPayload{
		0: {
			execution1.BusinessID: {{RunID: execution1.RunID, ArchetypeID: execution1.ArchetypeID}},
			execution2.BusinessID: {{RunID: execution2.RunID, ArchetypeID: execution2.ArchetypeID}},
		},
	}

	// Only execution2 (index 1) should be injected. Strict Times(1)
	// ensures execution1 is NOT injected — gomock would fail an
	// unexpected execution1 call.
	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), protomock.Eq(&historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution2.BusinessID,
			RunId:      execution2.RunID,
		},
		ArchetypeId:    execution2.ArchetypeID,
		TargetClusters: []string{remoteCluster},
	})).Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)

	// Both execs verify in one pass.
	for _, ex := range []*ExecutionInfo{execution1, execution2} {
		s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
			Namespace: mockedNamespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: ex.BusinessID,
				RunId:      ex.RunID,
			},
			Archetype:       chasm.WorkflowArchetype,
			ArchetypeId:     ex.ArchetypeID,
			SkipForceReload: true,
		})).Return(&adminservice.DescribeMutableStateResponse{}, nil).Times(1)
	}

	req := newShardedReq(payload)
	f, err := env.ExecuteActivity(s.a.ReplicateBatch, req)
	s.NoError(err)
	var out replicateBatchResult
	s.NoError(f.Get(&out))
	s.Equal(int64(2), out.VerifiedCount)
}

// TestReplicateBatch_EmptyBatch: an empty BatchPayload returns early with
// zero result and no errors or mock calls.
func (s *activitiesSuite) TestReplicateBatch_EmptyBatch() {
	env, _ := s.initEnv()

	req := newShardedReq(BatchPayload{})
	f, err := env.ExecuteActivity(s.a.ReplicateBatch, req)
	s.NoError(err)
	var out replicateBatchResult
	s.NoError(f.Get(&out))
	s.Equal(int64(0), out.VerifiedCount)
	s.Empty(out.CompletedShards)
}
