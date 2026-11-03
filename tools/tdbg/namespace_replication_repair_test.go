package tdbg

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNamespaceReplicationRepairCommandDryRun(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	target := proto.CloneOf(source)
	target.Info.Description = "divergent"
	factory := &testNamespaceReplicationCLIClientFactory{
		sourceAddress: "configured-source-address",
		clients: map[string]*testNamespaceReplicationAdminClient{
			"configured-source-address": testNamespaceReplicationSourceClient(source),
			"override-address":          testNamespaceReplicationTargetClient("cluster-b", target),
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg",
		"namespace",
		"replication",
		"repair",
		"--namespace", "namespace",
		"--cluster-address", "cluster-b=override-address",
	})
	require.NoError(t, err)
	require.Contains(t, output.String(), namespaceReplicationRepairStatusPlanned)
	require.Contains(t, output.String(), namespaceReplicationRepairActionPrepareSourceAndApply)
}

func TestNamespaceReplicationRepairCommandExecute(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	target := proto.CloneOf(source)
	target.Info.Description = "divergent"
	sourceClient := testNamespaceReplicationSourceClient(source)
	targetClient := testNamespaceReplicationTargetClient("cluster-b", target)
	sourceClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(source, request.GetNamespaceTask())
		return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
	}
	targetClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(target, request.GetNamespaceTask())
		return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
	}
	sourceClient.reconcileNamespaceMutationComponentsFn = func(request *adminservice.ReconcileNamespaceMutationComponentsRequest) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
		require.Equal(t, "operator test", request.GetReason())
		return &adminservice.ReconcileNamespaceMutationComponentsResponse{}, nil
	}
	factory := &testNamespaceReplicationCLIClientFactory{
		sourceAddress: "configured-source-address",
		clients: map[string]*testNamespaceReplicationAdminClient{
			"configured-source-address": sourceClient,
			"override-address":          targetClient,
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg",
		"--yes",
		"namespace",
		"replication",
		"repair",
		"--namespace", "namespace",
		"--cluster-address", "cluster-b=override-address",
		"--reason", "operator test",
		"--execute",
	})
	require.NoError(t, err)
	require.Contains(t, output.String(), namespaceReplicationRepairExecutionSucceeded)
	require.Contains(t, output.String(), "Final verification: "+namespaceReplicationStatusHealthy)
	require.Contains(t, output.String(), "Component reconciliation:")
	require.Equal(t, source.GetConfigVersion(), target.GetConfigVersion())
	require.Equal(t, source.GetInfo().GetDescription(), target.GetInfo().GetDescription())
}

func TestPrintNamespaceReplicationRepairPlanBeforePrompt(t *testing.T) {
	plan := &namespaceReplicationRepairPlan{
		Status:                      namespaceReplicationRepairStatusPlanned,
		RepairID:                    "repair-id",
		NamespaceName:               "namespace",
		NamespaceID:                 "namespace-id",
		SourceCluster:               "cluster-a",
		SourceAction:                namespaceReplicationRepairActionPrepareSource,
		SourceConfigVersion:         10,
		ProposedSourceConfigVersion: 11,
	}

	for _, testCase := range []struct {
		name          string
		printJSON     bool
		wantStdout    bool
		wantErrOutput bool
	}{
		{name: "human output precedes prompt", wantStdout: true},
		{name: "JSON mode preserves stdout", printJSON: true, wantErrOutput: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}
			app := cli.NewApp()
			app.Writer = stdout
			app.ErrWriter = stderr
			flags := flag.NewFlagSet(testCase.name, flag.ContinueOnError)
			flags.Bool(FlagYes, false, "")
			flags.Bool(FlagPrintJSON, testCase.printJSON, "")
			ctx := cli.NewContext(app, flags, nil)

			require.NoError(t, printNamespaceReplicationRepairPlanBeforePrompt(ctx, plan))
			require.Equal(t, testCase.wantStdout, stdout.Len() > 0)
			require.Equal(t, testCase.wantErrOutput, stderr.Len() > 0)
			output := stdout.String() + stderr.String()
			require.Contains(t, output, "Source action: PREPARE_SOURCE (10 -> 11)")
		})
	}
}

func TestPlanNamespaceReplicationRepair(t *testing.T) {
	testCases := []struct {
		name               string
		sourceVersion      int64
		targetVersion      int64
		missing            bool
		divergent          bool
		wantStatus         string
		wantSourceVersion  int64
		wantSourceAction   string
		wantTargetAction   string
		wantDetailContains string
	}{
		{
			name:              "healthy",
			sourceVersion:     10,
			targetVersion:     10,
			wantStatus:        namespaceReplicationRepairStatusNoChanges,
			wantSourceVersion: 10,
			wantSourceAction:  namespaceReplicationRepairActionNone,
			wantTargetAction:  namespaceReplicationRepairActionNone,
		},
		{
			name:              "matching target ahead prepares source to max",
			sourceVersion:     10,
			targetVersion:     12,
			wantStatus:        namespaceReplicationRepairStatusPlanned,
			wantSourceVersion: 12,
			wantSourceAction:  namespaceReplicationRepairActionPrepareSource,
			wantTargetAction:  namespaceReplicationRepairActionNone,
		},
		{
			name:              "divergent target below source applies source directly",
			sourceVersion:     10,
			targetVersion:     9,
			divergent:         true,
			wantStatus:        namespaceReplicationRepairStatusPlanned,
			wantSourceVersion: 10,
			wantSourceAction:  namespaceReplicationRepairActionNone,
			wantTargetAction:  namespaceReplicationRepairActionApplySource,
		},
		{
			name:              "divergent equal version prepares max plus one",
			sourceVersion:     10,
			targetVersion:     10,
			divergent:         true,
			wantStatus:        namespaceReplicationRepairStatusPlanned,
			wantSourceVersion: 11,
			wantSourceAction:  namespaceReplicationRepairActionPrepareSource,
			wantTargetAction:  namespaceReplicationRepairActionPrepareSourceAndApply,
		},
		{
			name:              "divergent target ahead prepares max plus one",
			sourceVersion:     10,
			targetVersion:     12,
			divergent:         true,
			wantStatus:        namespaceReplicationRepairStatusPlanned,
			wantSourceVersion: 13,
			wantSourceAction:  namespaceReplicationRepairActionPrepareSource,
			wantTargetAction:  namespaceReplicationRepairActionPrepareSourceAndApply,
		},
		{
			name:               "overflow blocks",
			sourceVersion:      math.MaxInt64,
			targetVersion:      math.MaxInt64,
			divergent:          true,
			wantStatus:         namespaceReplicationRepairStatusBlocked,
			wantSourceVersion:  math.MaxInt64,
			wantSourceAction:   namespaceReplicationRepairActionNone,
			wantDetailContains: "overflowing int64",
		},
		{
			name:              "missing target creates without version bump",
			sourceVersion:     10,
			missing:           true,
			wantStatus:        namespaceReplicationRepairStatusPlanned,
			wantSourceVersion: 10,
			wantSourceAction:  namespaceReplicationRepairActionNone,
			wantTargetAction:  namespaceReplicationRepairActionCreate,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			source := testNamespaceResponse(
				"namespace",
				"namespace-id",
				"cluster-a",
				[]string{"cluster-a", "cluster-b"},
				testCase.sourceVersion,
				20,
			)
			var target *adminservice.GetNamespaceResponse
			if !testCase.missing {
				target = proto.CloneOf(source)
				target.ConfigVersion = testCase.targetVersion
				if testCase.divergent {
					target.Info.Description = "divergent"
				}
			}
			verifier := testNamespaceReplicationVerifier(source, map[string]*adminservice.GetNamespaceResponse{
				"cluster-b": target,
			})
			observation, err := verifier.observe(context.Background(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			plan, err := planNamespaceReplicationRepair(
				observation,
				"repair-id",
				time.Unix(100, 0),
			)
			require.NoError(t, err)
			require.Equal(t, testCase.wantStatus, plan.Status)
			require.Equal(t, testCase.wantSourceVersion, plan.ProposedSourceConfigVersion)
			require.Equal(t, testCase.wantSourceAction, plan.SourceAction)
			if testCase.wantTargetAction != "" {
				require.Len(t, plan.Clusters, 1)
				require.Equal(t, testCase.wantTargetAction, plan.Clusters[0].Action)
			}
			if testCase.wantDetailContains != "" {
				require.Contains(t, plan.StatusDetail, testCase.wantDetailContains)
			}
		})
	}
}

func TestBuildNamespaceReplicationRepairTaskPreservesTargetLocalDataAndFailover(t *testing.T) {
	source := testNamespaceResponse(
		"namespace",
		"namespace-id",
		"cluster-a",
		[]string{"cluster-a", "cluster-b"},
		11,
		20,
	)
	source.Info.Data = map[string]string{"shared": "source", "mcn.local": "source-local"}
	target := proto.CloneOf(source)
	target.Info.Data = map[string]string{"shared": "old", "mcn.local": "target-local"}
	target.ReplicationConfig.ActiveClusterName = "cluster-b"
	target.FailoverVersion = 19
	target.FailoverHistory[0].FailoverVersion = 19
	ignored := map[string]struct{}{"mcn.local": {}}

	task, err := buildNamespaceReplicationRepairTask(
		source,
		target,
		11,
		19,
		ignored,
		namespaceRepairDataPreserveTargetIgnored,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"shared":    "source",
		"mcn.local": "target-local",
	}, task.GetInfo().GetData())
	require.Equal(t, "cluster-b", task.GetReplicationConfig().GetActiveClusterName())
	require.Equal(t, int64(19), task.GetFailoverVersion())
	require.Equal(t, int64(19), task.GetFailoverHistory()[0].GetFailoverVersion())
	require.True(t, proto.Equal(source.GetConfig().GetBadBinaries(), task.GetConfig().GetBadBinaries()))

	missingTask, err := buildNamespaceReplicationRepairTask(
		source,
		source,
		11,
		20,
		ignored,
		namespaceRepairDataExcludeIgnored,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"shared": "source"}, missingTask.GetInfo().GetData())

	sourceTask, err := buildNamespaceReplicationRepairTask(
		source,
		source,
		12,
		20,
		ignored,
		namespaceRepairDataSourceExact,
	)
	require.NoError(t, err)
	require.Equal(t, source.GetInfo().GetData(), sourceTask.GetInfo().GetData())
}

func TestBuildNamespaceReplicationRepairTaskClearsTargetBadBinaries(t *testing.T) {
	source := testNamespaceResponse(
		"namespace",
		"namespace-id",
		"cluster-a",
		[]string{"cluster-a", "cluster-b"},
		11,
		20,
	)
	source.Config.BadBinaries = nil
	target := proto.CloneOf(source)
	target.Config.BadBinaries = &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{
		"stale-checksum": {Reason: "stale"},
	}}

	task, err := buildNamespaceReplicationRepairTask(
		source,
		target,
		11,
		20,
		nil,
		namespaceRepairDataPreserveTargetIgnored,
	)
	require.NoError(t, err)
	require.NotNil(t, task.GetConfig().BadBinaries)
	require.Empty(t, task.GetConfig().GetBadBinaries().GetBinaries())
}

func TestNamespaceReplicationRepairerExecutesAndReconciles(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	target := proto.CloneOf(source)
	target.Info.Description = "divergent"
	target.FailoverHistory = nil

	sourceClient := testNamespaceReplicationSourceClient(source)
	targetClient := testNamespaceReplicationTargetClient("cluster-b", target)
	sourceClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		require.Equal(t, adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR, request.GetAttribution().GetOrigin())
		applyRepairTaskToSnapshot(source, request.GetNamespaceTask())
		return &adminservice.ApplyNamespaceMutationResponse{
			Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
		}, nil
	}
	targetClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(target, request.GetNamespaceTask())
		return &adminservice.ApplyNamespaceMutationResponse{
			Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
		}, nil
	}
	var reconciliationRequest *adminservice.ReconcileNamespaceMutationComponentsRequest
	sourceClient.reconcileNamespaceMutationComponentsFn = func(request *adminservice.ReconcileNamespaceMutationComponentsRequest) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
		reconciliationRequest = proto.CloneOf(request)
		require.Equal(t, int32(namespaceReplicationRepairReconciliationPageSize), request.GetPageSize())
		require.Empty(t, request.GetNextPageToken())
		return &adminservice.ReconcileNamespaceMutationComponentsResponse{}, nil
	}

	provider := &testNamespaceReplicationClientProvider{clients: map[string]*testNamespaceReplicationAdminClient{
		"source-address":    sourceClient,
		"cluster-b-address": targetClient,
	}}
	verifier := newNamespaceReplicationVerifier(provider, namespaceReplicationOptions{})
	verifier.now = func() time.Time { return time.Unix(100, 0).UTC() }
	verifyRequest := testNamespaceReplicationVerifyRequest()
	observation, err := verifier.observe(context.Background(), verifyRequest)
	require.NoError(t, err)
	plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(101, 0))
	require.NoError(t, err)
	require.Equal(t, int64(11), plan.ProposedSourceConfigVersion)

	repairer := newNamespaceReplicationRepairer(verifier)
	result, err := repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest: verifyRequest,
		Observation:   observation,
		Plan:          plan,
		Reason:        "repair test",
	})
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationRepairExecutionSucceeded, result.Status)
	require.Equal(t, int64(11), source.GetConfigVersion())
	require.Equal(t, int64(11), target.GetConfigVersion())
	require.Equal(t, source.GetInfo().GetDescription(), target.GetInfo().GetDescription())
	require.NotNil(t, reconciliationRequest)
	require.Equal(t, "repair-id", reconciliationRequest.GetRepairId())
	require.Equal(t, []string{"cluster-b"}, reconciliationRequest.GetVerifiedPeerClusters())
	require.Equal(t, time.Unix(102, 0).UTC(), reconciliationRequest.GetCutoffTime().AsTime())
}

func TestNamespaceReplicationRepairerReturnsCompletedReconciliationPagesOnLaterFailure(t *testing.T) {
	client := &testNamespaceReplicationAdminClient{}
	client.reconcileNamespaceMutationComponentsFn = func(request *adminservice.ReconcileNamespaceMutationComponentsRequest) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
		switch string(request.GetNextPageToken()) {
		case "":
			return &adminservice.ReconcileNamespaceMutationComponentsResponse{
				Components:    []*adminservice.NamespaceMutationComponentReconciliation{{BusinessId: "business-1"}},
				NextPageToken: []byte("page-2"),
			}, nil
		case "page-2":
			return nil, errors.New("visibility unavailable")
		default:
			t.Fatalf("unexpected page token %q", request.GetNextPageToken())
			return nil, nil
		}
	}
	verifier := newNamespaceReplicationVerifier(&testNamespaceReplicationClientProvider{}, namespaceReplicationOptions{})
	repairer := newNamespaceReplicationRepairer(verifier)
	request := namespaceReplicationRepairExecutionRequest{
		Observation: &namespaceReplicationObservation{result: &namespaceReplicationVerificationResult{
			SourceCluster:    "cluster-a",
			ExpectedClusters: []string{"cluster-a", "cluster-b"},
			Clusters:         []namespaceReplicationClusterVerification{{Cluster: "cluster-b", Role: "TARGET"}},
		}},
		Plan:   &namespaceReplicationRepairPlan{NamespaceID: "namespace-id", RepairID: "repair-id"},
		Reason: "repair test",
	}

	result, err := repairer.reconcileRepairComponents(
		context.Background(),
		client,
		request,
		time.Unix(100, 0),
		[]string{"cluster-b"},
	)
	require.ErrorContains(t, err, "visibility unavailable")
	require.Len(t, result.GetComponents(), 1)
	require.Equal(t, "business-1", result.GetComponents()[0].GetBusinessId())
}

func TestNamespaceReplicationRepairerReconcilesOnlyIndividuallyVerifiedPeers(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b", "cluster-c"}, 11, 20)
	targetB := proto.CloneOf(source)
	targetB.ConfigVersion = 10
	targetB.Info.Description = "divergent-b"
	targetC := proto.CloneOf(source)
	targetC.ConfigVersion = 10
	targetC.Info.Description = "divergent-c"

	sourceClient := testNamespaceReplicationSourceClient(source)
	targetBClient := testNamespaceReplicationTargetClient("cluster-b", targetB)
	targetBClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(targetB, request.GetNamespaceTask())
		return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
	}
	targetCClient := testNamespaceReplicationTargetClient("cluster-c", targetC)
	targetCClient.applyNamespaceMutationFn = func(*adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		return nil, errors.New("cluster-c unavailable")
	}
	var reconciliationRequest *adminservice.ReconcileNamespaceMutationComponentsRequest
	sourceClient.reconcileNamespaceMutationComponentsFn = func(request *adminservice.ReconcileNamespaceMutationComponentsRequest) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
		reconciliationRequest = proto.CloneOf(request)
		return &adminservice.ReconcileNamespaceMutationComponentsResponse{}, nil
	}

	provider := &testNamespaceReplicationClientProvider{clients: map[string]*testNamespaceReplicationAdminClient{
		"source-address":    sourceClient,
		"cluster-b-address": targetBClient,
		"cluster-c-address": targetCClient,
	}}
	verifier := newNamespaceReplicationVerifier(provider, namespaceReplicationOptions{})
	verifyRequest := testNamespaceReplicationVerifyRequest()
	observation, err := verifier.observe(context.Background(), verifyRequest)
	require.NoError(t, err)
	plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(101, 0))
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationRepairActionNone, plan.SourceAction)

	repairer := newNamespaceReplicationRepairer(verifier)
	result, err := repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest: verifyRequest,
		Observation:   observation,
		Plan:          plan,
		Reason:        "partial repair test",
	})
	require.Error(t, err)
	require.Equal(t, namespaceReplicationRepairExecutionFailed, result.Status)
	require.NotNil(t, reconciliationRequest)
	require.Equal(t, []string{"cluster-b"}, reconciliationRequest.GetVerifiedPeerClusters())
	require.Equal(t, []string{"cluster-b", "cluster-c"}, reconciliationRequest.GetAuthoritativePeerClusters())
}

func TestNamespaceReplicationRepairerCreatesMissingTarget(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	source.Config.BadBinaries = &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{
		"checksum": {Reason: "bad binary"},
	}}
	sourceClient := testNamespaceReplicationSourceClient(source)
	var target *adminservice.GetNamespaceResponse
	targetClient := &testNamespaceReplicationAdminClient{
		describeClusterFn: func() (*adminservice.DescribeClusterResponse, error) {
			return &adminservice.DescribeClusterResponse{ClusterName: "cluster-b"}, nil
		},
		getNamespaceFn: func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
			if target == nil {
				return nil, serviceerror.NewNamespaceNotFound("namespace")
			}
			return proto.CloneOf(target), nil
		},
	}
	targetClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		require.Equal(t, enumsspb.NAMESPACE_OPERATION_UPDATE, request.GetNamespaceTask().GetNamespaceOperation())
		require.True(t, proto.Equal(source.GetConfig().GetBadBinaries(), request.GetNamespaceTask().GetConfig().GetBadBinaries()))
		target = testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 0, 0)
		applyRepairTaskToSnapshot(target, request.GetNamespaceTask())
		return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED}, nil
	}
	var reconciliationRequest *adminservice.ReconcileNamespaceMutationComponentsRequest
	sourceClient.reconcileNamespaceMutationComponentsFn = func(request *adminservice.ReconcileNamespaceMutationComponentsRequest) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
		reconciliationRequest = proto.CloneOf(request)
		return &adminservice.ReconcileNamespaceMutationComponentsResponse{}, nil
	}
	provider := &testNamespaceReplicationClientProvider{clients: map[string]*testNamespaceReplicationAdminClient{
		"source-address":    sourceClient,
		"cluster-b-address": targetClient,
	}}
	verifier := newNamespaceReplicationVerifier(provider, namespaceReplicationOptions{})
	verifyRequest := testNamespaceReplicationVerifyRequest()
	observation, err := verifier.observe(context.Background(), verifyRequest)
	require.NoError(t, err)
	plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(101, 0))
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationRepairActionCreate, plan.Clusters[0].Action)

	repairer := newNamespaceReplicationRepairer(verifier)
	result, err := repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest: verifyRequest,
		Observation:   observation,
		Plan:          plan,
		Reason:        "create missing target",
	})
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationRepairExecutionSucceeded, result.Status)
	require.NotNil(t, target)
	require.True(t, proto.Equal(source.GetConfig().GetBadBinaries(), target.GetConfig().GetBadBinaries()))
	require.Equal(t, []string{"cluster-b"}, reconciliationRequest.GetVerifiedPeerClusters())
}

func TestNamespaceReplicationRepairerAbortsWhenSourceChangesAfterPlanning(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
	verifier := testNamespaceReplicationVerifier(source, nil)
	verifyRequest := testNamespaceReplicationVerifyRequest()
	observation, err := verifier.observe(context.Background(), verifyRequest)
	require.NoError(t, err)
	plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(101, 0))
	require.NoError(t, err)
	source.Info.Description = "changed after planning"

	repairer := newNamespaceReplicationRepairer(verifier)
	result, err := repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest: verifyRequest,
		Observation:   observation,
		Plan:          plan,
		Reason:        "source race test",
	})
	require.ErrorContains(t, err, "source namespace changed after repair planning")
	require.Equal(t, namespaceReplicationRepairExecutionFailed, result.Status)
	require.Nil(t, result.ComponentReconciliation)
}

func TestVerifiedRepairPeersRequiresStableSource(t *testing.T) {
	version := int64(10)
	observation := &namespaceReplicationObservation{
		result: &namespaceReplicationVerificationResult{
			SourceCluster: "cluster-a",
			Clusters: []namespaceReplicationClusterVerification{
				{Cluster: "cluster-a", Role: "SOURCE"},
				{
					Cluster:       "cluster-b",
					Role:          "TARGET",
					Presence:      namespaceReplicationPresencePresent,
					ConfigVersion: &version,
					ConfigMatch:   namespaceReplicationMatchMatch,
					FailoverMatch: namespaceReplicationMatchMatch,
				},
			},
		},
		sourceSnapshot: testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, version, 20),
	}
	require.Empty(t, verifiedRepairPeers(observation))

	observation.sourceStable = true
	require.Equal(t, []string{"cluster-b"}, verifiedRepairPeers(observation))
}

func TestNamespaceReplicationRepairerRejectsLaterCutoff(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
	source.ObservedAt = timestamppb.New(time.Unix(101, 0))
	verifier := testNamespaceReplicationVerifier(source, nil)
	observation, err := verifier.observe(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(100, 0))
	require.NoError(t, err)

	repairer := newNamespaceReplicationRepairer(verifier)
	later := time.Unix(102, 0).UTC()
	_, err = repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest:   testNamespaceReplicationVerifyRequest(),
		Observation:     observation,
		Plan:            plan,
		Reason:          "cutoff test",
		ReconcileBefore: &later,
	})
	require.ErrorContains(t, err, "cannot be later")
}

func TestNamespaceRepairCutoffRequiresServerObservation(t *testing.T) {
	t.Parallel()
	repairer := newNamespaceReplicationRepairer(nil)
	_, err := repairer.effectiveRepairCutoff(nil, nil)
	require.ErrorContains(t, err, "server-timed namespace observation")
	serverTime := time.Unix(100, 0).UTC()
	cutoff, err := repairer.effectiveRepairCutoff(nil, timestamppb.New(serverTime))
	require.NoError(t, err)
	require.Equal(t, serverTime, cutoff)
	earlier := serverTime.Add(-time.Hour)
	cutoff, err = repairer.effectiveRepairCutoff(&earlier, timestamppb.New(serverTime))
	require.NoError(t, err)
	require.Equal(t, earlier, cutoff)
}

func applyRepairTaskToSnapshot(
	snapshot *adminservice.GetNamespaceResponse,
	task *replicationspb.NamespaceTaskAttributes,
) {
	snapshot.Info = proto.CloneOf(task.GetInfo())
	snapshot.Info.Id = task.GetId()
	snapshot.Config = proto.CloneOf(task.GetConfig())
	snapshot.ReplicationConfig = proto.CloneOf(task.GetReplicationConfig())
	snapshot.ConfigVersion = task.GetConfigVersion()
	snapshot.FailoverVersion = task.GetFailoverVersion()
	snapshot.FailoverHistory = make([]*replicationpb.FailoverStatus, 0, len(task.GetFailoverHistory()))
	for _, status := range task.GetFailoverHistory() {
		snapshot.FailoverHistory = append(snapshot.FailoverHistory, proto.CloneOf(status))
	}
}
