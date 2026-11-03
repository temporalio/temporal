package tdbg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNamespaceRepairInvalidSourceReturnsBlockedJSON(t *testing.T) {
	for _, state := range []string{"inactive", "handover"} {
		t.Run(state, func(t *testing.T) {
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			if state == "inactive" {
				source.ReplicationConfig.ActiveClusterName = "cluster-b"
			} else {
				source.ReplicationConfig.State = enumspb.REPLICATION_STATE_HANDOVER
			}
			factory := &testNamespaceReplicationCLIClientFactory{
				sourceAddress: "source-address", clients: map[string]*testNamespaceReplicationAdminClient{"source-address": testNamespaceReplicationSourceClient(source)},
			}
			app, output := testNamespaceReplicationCLIApp(factory)
			err := app.Run([]string{"tdbg", "namespace", "replication", "repair", "--namespace", "namespace", "--print-json"})
			var exit cli.ExitCoder
			require.ErrorAs(t, err, &exit)
			require.Equal(t, 2, exit.ExitCode())
			var result namespaceReplicationRepairPlan
			require.NoError(t, json.Unmarshal(output.Bytes(), &result))
			require.Equal(t, namespaceReplicationRepairStatusBlocked, result.Status)
			require.NotEmpty(t, result.StatusDetail)
			require.Equal(t, namespaceReplicationRepairActionNone, result.SourceAction)
		})
	}
}

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
	require.Contains(t, output.String(), namespaceReplicationRepairActionApplySource)
}

func TestNamespaceReplicationRepairCommandExecute(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	target := proto.CloneOf(source)
	target.Info.Description = "divergent"
	sourceClient := testNamespaceReplicationSourceClient(source)
	targetClient := testNamespaceReplicationTargetClient("cluster-b", target)
	sourceClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(source, decodeNamespaceRepairTask(t, request))
		return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
	}
	targetClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(target, decodeNamespaceRepairTask(t, request))
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

func TestRepairPlanOnlyBumpsPastDivergentPeers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                     string
		healthy, divergent, want int64
	}{
		{"healthy equal version", 10, 9, 10},
		{"healthy higher version", 20, 9, 20},
		{"divergent below healthy", 20, 15, 20},
		{"divergent equal healthy", 20, 20, 21},
		{"healthy at max version", math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b", "cluster-c"}, 10, 20)
			healthy, divergent := proto.CloneOf(source), proto.CloneOf(source)
			healthy.ConfigVersion = tc.healthy
			divergent.ConfigVersion = tc.divergent
			divergent.Info.Description = "divergent"
			verifier := testNamespaceReplicationVerifier(source, map[string]*adminservice.GetNamespaceResponse{"cluster-b": healthy, "cluster-c": divergent})
			observation, err := verifier.observe(t.Context(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(100, 0))
			require.NoError(t, err)
			require.Equal(t, namespaceReplicationRepairStatusPlanned, plan.Status)
			require.Equal(t, tc.want, plan.ProposedSourceConfigVersion)
			if tc.want == 10 {
				require.Equal(t, namespaceReplicationRepairActionNone, plan.SourceAction)
			}
		})
	}
}

func TestRepairWarnsWhenRemovedPeerDeliveriesAreAbandoned(t *testing.T) {
	t.Parallel()
	reconciliation := &adminservice.ReconcileNamespaceMutationComponentsResponse{
		Components: []*adminservice.NamespaceMutationComponentReconciliation{
			{Peers: []*adminservice.NamespaceMutationPeerReconciliation{
				{TargetCluster: "removed-z", Outcome: adminservice.NamespaceMutationPeerReconciliation_OUTCOME_RECONCILED, Basis: adminservice.NamespaceMutationPeerReconciliation_BASIS_REMOVED_MEMBERSHIP},
				{TargetCluster: "current-b", Outcome: adminservice.NamespaceMutationPeerReconciliation_OUTCOME_RECONCILED, Basis: adminservice.NamespaceMutationPeerReconciliation_BASIS_VERIFIED_TARGET},
			}},
			{Peers: []*adminservice.NamespaceMutationPeerReconciliation{
				{TargetCluster: "removed-a", Outcome: adminservice.NamespaceMutationPeerReconciliation_OUTCOME_ALREADY_TERMINAL, Basis: adminservice.NamespaceMutationPeerReconciliation_BASIS_REMOVED_MEMBERSHIP},
				{TargetCluster: "removed-z", Outcome: adminservice.NamespaceMutationPeerReconciliation_OUTCOME_RECONCILED, Basis: adminservice.NamespaceMutationPeerReconciliation_BASIS_REMOVED_MEMBERSHIP},
			}},
		},
	}
	warnings := namespaceReplicationRepairWarnings(reconciliation)
	require.Equal(t, []string{"Deliveries to removed peers were abandoned: removed-a, removed-z. These peers were not inspected or updated and may retain stale namespace membership."}, warnings)
	require.Empty(t, namespaceReplicationRepairWarnings(nil))
	require.Empty(t, namespaceReplicationRepairWarnings(&adminservice.ReconcileNamespaceMutationComponentsResponse{
		Components: reconciliation.Components[:0],
	}))
	for _, printJSON := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%v", printJSON), func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			app := cli.NewApp()
			app.Writer = &output
			flags := flag.NewFlagSet("repair", flag.ContinueOnError)
			flags.Bool(FlagPrintJSON, printJSON, "")
			ctx := cli.NewContext(app, flags, nil)
			result := &namespaceReplicationRepairExecutionResult{
				Plan:     &namespaceReplicationRepairPlan{RepairID: "repair-id"},
				Warnings: warnings, ComponentReconciliation: reconciliation,
			}
			require.NoError(t, printNamespaceReplicationRepairExecution(ctx, result))
			if printJSON {
				var decoded namespaceReplicationRepairExecutionResult
				require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
				require.Equal(t, warnings, decoded.Warnings)
			} else {
				require.Contains(t, output.String(), "Warning: "+warnings[0])
			}
		})
	}
}

func TestRepairOutputDoesNotHideFailedComponentDiscovery(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	app := cli.NewApp()
	app.Writer = &output
	ctx := cli.NewContext(app, flag.NewFlagSet("repair", flag.ContinueOnError), nil)
	require.NoError(t, printNamespaceReplicationRepairExecution(ctx, &namespaceReplicationRepairExecutionResult{
		Plan:                         &namespaceReplicationRepairPlan{RepairID: "repair-id"},
		ComponentReconciliation:      &adminservice.ReconcileNamespaceMutationComponentsResponse{},
		ComponentReconciliationError: "visibility unavailable",
	}))
	require.Contains(t, output.String(), "Component reconciliation incomplete: visibility unavailable")
	require.NotContains(t, output.String(), "no running components found")
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
			wantTargetAction:  namespaceReplicationRepairActionApplySource,
		},
		{
			name:              "divergent target ahead prepares max plus one",
			sourceVersion:     10,
			targetVersion:     12,
			divergent:         true,
			wantStatus:        namespaceReplicationRepairStatusPlanned,
			wantSourceVersion: 13,
			wantSourceAction:  namespaceReplicationRepairActionPrepareSource,
			wantTargetAction:  namespaceReplicationRepairActionApplySource,
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
	sourceBefore, targetBefore := proto.CloneOf(source), proto.CloneOf(target)
	ignored := map[string]struct{}{"mcn.local": {}}

	task, err := buildNamespaceReplicationRepairTask(
		source,
		target,
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
		ignored,
		namespaceRepairDataExcludeIgnored,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"shared": "source"}, missingTask.GetInfo().GetData())

	sourceTask, err := buildNamespaceReplicationRepairTask(
		source,
		source,
		ignored,
		namespaceRepairDataSourceExact,
	)
	require.NoError(t, err)
	require.Equal(t, source.GetInfo().GetData(), sourceTask.GetInfo().GetData())
	task.Info.Data["shared"] = "changed task"
	task.Config.WorkflowExecutionRetentionTtl.Seconds++
	task.FailoverHistory[0].FailoverVersion++
	protorequire.ProtoEqual(t, sourceBefore, source)
	protorequire.ProtoEqual(t, targetBefore, target)
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
		nil,
		namespaceRepairDataPreserveTargetIgnored,
	)
	require.NoError(t, err)
	require.Nil(t, task.GetConfig().BadBinaries)
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
		applyRepairTaskToSnapshot(source, decodeNamespaceRepairTask(t, request))
		return &adminservice.ApplyNamespaceMutationResponse{
			Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE,
		}, nil
	}
	targetClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(target, decodeNamespaceRepairTask(t, request))
		return &adminservice.ApplyNamespaceMutationResponse{
			Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
		}, nil
	}
	var reconciliationRequest *adminservice.ReconcileNamespaceMutationComponentsRequest
	sourceClient.reconcileNamespaceMutationComponentsFn = func(request *adminservice.ReconcileNamespaceMutationComponentsRequest) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
		reconciliationRequest = proto.CloneOf(request)
		require.Equal(t, int32(namespaceReplicationRepairReconciliationPageSize), request.GetPageSize())
		require.Empty(t, request.GetNextPageToken())
		return &adminservice.ReconcileNamespaceMutationComponentsResponse{
			Components: []*adminservice.NamespaceMutationComponentReconciliation{{Peers: []*adminservice.NamespaceMutationPeerReconciliation{
				{TargetCluster: "removed-c", Outcome: adminservice.NamespaceMutationPeerReconciliation_OUTCOME_RECONCILED, Basis: adminservice.NamespaceMutationPeerReconciliation_BASIS_REMOVED_MEMBERSHIP},
			}}},
		}, nil
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
	require.Equal(t, []string{namespaceReplicationWarningInitialHistory}, plan.Clusters[0].Warnings)

	repairer := &namespaceReplicationRepairer{verifier: verifier}
	result, err := repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest: verifyRequest,
		Observation:   observation,
		Plan:          plan,
		Reason:        "repair test",
	})
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationRepairExecutionSucceeded, result.Status)
	require.Contains(t, result.StatusDetail, "success does not imply former replicas were synchronized")
	require.Len(t, result.Warnings, 1)
	require.Contains(t, result.Warnings[0], "removed-c")
	require.Equal(t, int64(11), source.GetConfigVersion())
	require.Equal(t, int64(11), target.GetConfigVersion())
	require.Equal(t, source.GetInfo().GetDescription(), target.GetInfo().GetDescription())
	require.Empty(t, target.GetFailoverHistory())
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
	repairer := &namespaceReplicationRepairer{verifier: verifier}
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
		&namespaceReplicationObservation{sourceStable: true, sourceSnapshot: testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20), result: request.Observation.result},
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
		applyRepairTaskToSnapshot(targetB, decodeNamespaceRepairTask(t, request))
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

	repairer := &namespaceReplicationRepairer{verifier: verifier}
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
	t.Parallel()
	for _, outcome := range []adminservice.ApplyNamespaceMutationResponse_Outcome{
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE,
	} {
		t.Run(outcome.String(), func(t *testing.T) {
			t.Parallel()

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
				task := decodeNamespaceRepairTask(t, request)
				require.Equal(t, enumsspb.NAMESPACE_OPERATION_UPDATE, task.GetNamespaceOperation())
				require.True(t, proto.Equal(source.GetConfig().GetBadBinaries(), task.GetConfig().GetBadBinaries()))
				target = testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 0, 0)
				applyRepairTaskToSnapshot(target, task)
				return &adminservice.ApplyNamespaceMutationResponse{Outcome: outcome}, nil
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

			repairer := &namespaceReplicationRepairer{verifier: verifier}
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

		})
	}
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

	repairer := &namespaceReplicationRepairer{verifier: verifier}
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

	repairer := &namespaceReplicationRepairer{verifier: verifier}
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
	repairer := &namespaceReplicationRepairer{verifier: nil}
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

func TestRepairRequiresMatchingReadbackAfterStaleSuccess(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		mutate    func(*adminservice.GetNamespaceResponse)
		outcome   adminservice.ApplyNamespaceMutationResponse_Outcome
		wantError string
	}{
		{name: "wrong config", mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.Description = "other" }, wantError: "config readback"},
		{name: "wrong config version", mutate: func(r *adminservice.GetNamespaceResponse) { r.ConfigVersion++ }, wantError: "config version readback"},
		{name: "wrong failover version", mutate: func(r *adminservice.GetNamespaceResponse) { r.FailoverVersion++ }, wantError: "failover version readback"},
		{name: "wrong identity", mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.Id = "other-id" }, wantError: "config readback"},
		{name: "not global", mutate: func(r *adminservice.GetNamespaceResponse) { r.IsGlobalNamespace = false }, wantError: "not global"},
		{name: "not admitted", outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED, wantError: "unexpected apply outcome"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			expected := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
			task, err := buildNamespaceReplicationRepairTask(expected, expected, nil, namespaceRepairDataSourceExact)
			require.NoError(t, err)
			actual := proto.CloneOf(expected)
			if test.mutate != nil {
				test.mutate(actual)
			}
			client := testNamespaceReplicationTargetClient("cluster-b", actual)
			client.applyNamespaceMutationFn = func(*adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
				outcome := test.outcome
				if outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED {
					outcome = adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE
				}
				return &adminservice.ApplyNamespaceMutationResponse{Outcome: outcome}, nil
			}
			r := &namespaceReplicationRepairer{verifier: &namespaceReplicationVerifier{rpcTimeout: time.Second}}
			result := namespaceReplicationRepairApplyResult{Action: namespaceReplicationRepairActionCreate}
			_, err = r.applyRepairAndReadback(t.Context(), client, namespaceReplicationRepairExecutionRequest{
				Plan: &namespaceReplicationRepairPlan{SourceCluster: "cluster-a", RepairID: "repair-id"}, Reason: "repair",
			}, task, &result)
			require.ErrorContains(t, err, test.wantError)
			require.False(t, result.Readback)
		})
	}
}

func TestRepairCreateRejectsNameCollisionAfterPlanning(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
	client := testNamespaceReplicationTargetClient("cluster-b", nil)
	client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		if request.GetId() != "" {
			return nil, serviceerror.NewNamespaceNotFound("namespace")
		}
		collision := proto.CloneOf(source)
		collision.Info.Id = "other-id"
		return collision, nil
	}
	client.applyNamespaceMutationFn = func(*adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		t.Fatal("repair must not write after a name collision")
		return nil, nil
	}
	r := &namespaceReplicationRepairer{verifier: testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"cluster-b-address": client})}
	result := r.applyTargetRepair(t.Context(), namespaceReplicationRepairExecutionRequest{
		Observation: &namespaceReplicationObservation{clusterAddresses: map[string]string{"cluster-b": "cluster-b-address"}},
		Plan:        &namespaceReplicationRepairPlan{NamespaceID: "namespace-id", SourceCluster: "cluster-a", RepairID: "repair-id"}, Reason: "repair",
	}, source, namespaceReplicationClusterRepairPlan{Cluster: "cluster-b", Action: namespaceReplicationRepairActionCreate})
	require.Contains(t, result.Error, "namespace name appeared")
	require.False(t, result.Readback)
}

func TestRepairRefreshesTargetBeforeWriting(t *testing.T) {
	t.Parallel()
	for _, versionChanged := range []bool{false, true} {
		t.Run(fmt.Sprintf("versionChanged=%v", versionChanged), func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
			target := proto.CloneOf(source)
			target.ConfigVersion = 10
			target.Info.Description = "stale"
			target.Info.Data["local-key"] = "before confirmation"
			frozen := proto.CloneOf(target)
			target.Info.Data["local-key"] = "after confirmation"
			if versionChanged {
				target.ConfigVersion++
			}
			client := testNamespaceReplicationTargetClient("cluster-b", target)
			applied := false
			client.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
				applied = true
				task := decodeNamespaceRepairTask(t, request)
				require.Equal(t, "after confirmation", task.GetInfo().GetData()["local-key"])
				applyRepairTaskToSnapshot(target, task)
				return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
			}
			verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"cluster-b-address": client})
			verifier.dataKeysToIgnore = map[string]struct{}{"local-key": {}}
			r := &namespaceReplicationRepairer{verifier: verifier}
			result := r.applyTargetRepair(t.Context(), namespaceReplicationRepairExecutionRequest{
				Observation: &namespaceReplicationObservation{clusterAddresses: map[string]string{"cluster-b": "cluster-b-address"}, targetSnapshots: map[string]*adminservice.GetNamespaceResponse{"cluster-b": frozen}},
				Plan:        &namespaceReplicationRepairPlan{NamespaceID: "namespace-id", SourceCluster: "cluster-a", RepairID: "repair-id"}, Reason: "repair",
			}, source, namespaceReplicationClusterRepairPlan{Cluster: "cluster-b", Action: namespaceReplicationRepairActionApplySource})
			require.Equal(t, !versionChanged, applied)
			if versionChanged {
				require.Contains(t, result.Error, "changed after planning")
			} else {
				require.Empty(t, result.Error)
				require.True(t, result.Readback)
			}
		})
	}
}

func decodeNamespaceRepairTask(t *testing.T, request *adminservice.ApplyNamespaceMutationRequest) *replicationspb.NamespaceTaskAttributes {
	t.Helper()
	require.Nil(t, request.GetNamespaceTask(), "repair sends task content only in namespace_task_payload")
	require.NotEmpty(t, request.GetNamespaceTaskPayload())
	task := &replicationspb.NamespaceTaskAttributes{}
	require.NoError(t, proto.Unmarshal(request.GetNamespaceTaskPayload(), task))
	return task
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
