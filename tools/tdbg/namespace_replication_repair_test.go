package tdbg

import (
	"bytes"
	"context"
	"encoding/json"
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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNamespaceRepairInvalidSourceReturnsBlockedJSON(t *testing.T) {
	for _, test := range []struct {
		name       string
		mutate     func(*adminservice.GetNamespaceResponse)
		wantDetail string
	}{
		{name: "inactive", mutate: func(r *adminservice.GetNamespaceResponse) { r.ReplicationConfig.ActiveClusterName = "cluster-b" }},
		{name: "handover", mutate: func(r *adminservice.GetNamespaceResponse) {
			r.ReplicationConfig.State = enumspb.REPLICATION_STATE_HANDOVER
		}},
		{name: "refused", mutate: func(r *adminservice.GetNamespaceResponse) {
			r.RepairScope = &adminservice.NamespaceRepairScope{RefusalReason: "membership is kept elsewhere"}
		}, wantDetail: "membership is kept elsewhere"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			test.mutate(source)
			factory := &testNamespaceReplicationCLIClientFactory{
				clients: map[string]*testNamespaceReplicationAdminClient{"source": testNamespaceReplicationSourceClient(source)},
			}
			app, output := testNamespaceReplicationCLIApp(factory)
			err := app.Run([]string{"tdbg", "namespace", "replication", "repair", "--namespace-id", "namespace-id", "--print-json"})
			var exit cli.ExitCoder
			require.ErrorAs(t, err, &exit)
			require.Equal(t, 2, exit.ExitCode())
			var result namespaceReplicationRepairPlan
			require.NoError(t, json.Unmarshal(output.Bytes(), &result))
			require.Equal(t, namespaceReplicationRepairStatusBlocked, result.Status)
			require.NotEmpty(t, result.StatusDetail)
			require.Contains(t, result.StatusDetail, test.wantDetail)
			require.Equal(t, namespaceReplicationRepairActionNone, result.SourceAction)
		})
	}
}

func TestNamespaceReplicationRepairCommandDryRun(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	target := proto.CloneOf(source)
	target.Info.Description = "divergent"
	factory := &testNamespaceReplicationCLIClientFactory{
		clients: map[string]*testNamespaceReplicationAdminClient{
			"source":    testNamespaceReplicationSourceClient(source),
			"cluster-b": testNamespaceReplicationTargetClient("cluster-b", target),
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg",
		"namespace",
		"replication",
		"repair",
		"--namespace-id", "namespace-id",
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
	factory := &testNamespaceReplicationCLIClientFactory{
		clients: map[string]*testNamespaceReplicationAdminClient{
			"source":    sourceClient,
			"cluster-b": targetClient,
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg",
		"--yes",
		"namespace",
		"replication",
		"repair",
		"--namespace-id", "namespace-id",
		"--reason", "operator test",
		"--execute",
	})
	require.NoError(t, err)
	require.Contains(t, output.String(), namespaceReplicationRepairExecutionSucceeded)
	require.Contains(t, output.String(), "Final verification: "+namespaceReplicationStatusHealthy)
	require.Equal(t, source.GetConfigVersion(), target.GetConfigVersion())
	require.Equal(t, source.GetInfo().GetDescription(), target.GetInfo().GetDescription())
}

// Repair selects the namespace exactly as verify does. These CLI tests run
// sequentially: running a urfave/cli app mutates tdbg's shared global flags.
func TestNamespaceReplicationRepairCommandNamespaceSelection(t *testing.T) {
	for _, test := range []struct {
		name       string
		global     []string
		local      []string
		byName     bool
		wantError  string
		wantOutput string
		wantCalls  int
	}{
		{name: "name", local: []string{"--namespace", "namespace"}, byName: true},
		{name: "alias", local: []string{"-n", "namespace"}, byName: true},
		{name: "inherited name", global: []string{"-n", "namespace"}, byName: true},
		{name: "ID", local: []string{"--namespace-id", "namespace-id"}},
		{name: "ID with matching name", local: []string{"-n", "namespace", "--namespace-id", "namespace-id"}},
		{name: "ID with mismatched name", local: []string{"-n", "other", "--namespace-id", "namespace-id"}, wantError: "repair is blocked", wantCalls: 1, wantOutput: `not "other"` + "\n"},
		{name: "ID with mismatched inherited name", global: []string{"-n", "other"}, local: []string{"--namespace-id", "namespace-id"}, wantError: "repair is blocked", wantCalls: 1, wantOutput: `not "other" from tdbg --namespace or TEMPORAL_CLI_NAMESPACE` + "\n"},
		{name: "conflicting names", global: []string{"-n", "other"}, local: []string{"-n", "namespace"}, wantError: `--namespace "namespace" conflicts with "other"`},
		{name: "blank inherited name is not a selector", global: []string{"-n", " "}, wantError: "--namespace or --namespace-id is required"},
		{name: "no selector", wantError: "--namespace or --namespace-id is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			target := proto.CloneOf(source)
			target.Info.Description = "divergent"
			sourceClient := testNamespaceReplicationSourceClient(source)
			reads := 0
			sourceClient.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
				if test.byName && reads == 0 {
					require.Equal(t, "namespace", request.GetNamespace())
				} else {
					require.Equal(t, "namespace-id", request.GetId())
				}
				reads++
				return proto.CloneOf(source), nil
			}
			factory := &testNamespaceReplicationCLIClientFactory{
				clients: map[string]*testNamespaceReplicationAdminClient{
					"source":    sourceClient,
					"cluster-b": testNamespaceReplicationTargetClient("cluster-b", target),
				},
			}
			app, output := testNamespaceReplicationCLIApp(factory)
			args := append([]string{"tdbg"}, test.global...)
			args = append(args, "namespace", "replication", "repair")
			err := app.Run(append(args, test.local...))
			require.Contains(t, output.String(), test.wantOutput)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.Equal(t, 2, tdbgExitCode(err))
				require.Equal(t, test.wantCalls, factory.adminClientCalls)
				return
			}
			require.NoError(t, err)
			require.Contains(t, output.String(), namespaceReplicationRepairStatusPlanned)
			require.Contains(t, output.String(), "Namespace: namespace (namespace-id)")
		})
	}
}

// Execution verifies again after writing. That read, like every read after the
// first, uses the resolved ID: the name may belong to another namespace by then.
func TestNamespaceReplicationRepairCommandExecuteResolvesNameOnce(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	target := proto.CloneOf(source)
	target.Info.Description = "divergent"
	sourceClient := testNamespaceReplicationSourceClient(source)
	targetClient := testNamespaceReplicationTargetClient("cluster-b", target)
	var reads []string
	sourceClient.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		if request.GetNamespace() != "" {
			reads = append(reads, "name:"+request.GetNamespace())
		} else {
			reads = append(reads, "id:"+request.GetId())
		}
		return proto.CloneOf(source), nil
	}
	sourceClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(source, decodeNamespaceRepairTask(t, request))
		return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
	}
	targetClient.applyNamespaceMutationFn = func(request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
		applyRepairTaskToSnapshot(target, decodeNamespaceRepairTask(t, request))
		return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
	}
	factory := &testNamespaceReplicationCLIClientFactory{
		clients: map[string]*testNamespaceReplicationAdminClient{
			"source":    sourceClient,
			"cluster-b": targetClient,
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg", "--yes", "namespace", "replication", "repair",
		"--namespace", "namespace", "--reason", "operator test", "--execute",
	})
	require.NoError(t, err)
	require.Contains(t, output.String(), namespaceReplicationRepairExecutionSucceeded)
	require.Contains(t, output.String(), "Final verification: "+namespaceReplicationStatusHealthy)
	require.Equal(t, "name:namespace", reads[0])
	// Planning, freezing the source and the final verification each re-read it.
	require.Greater(t, len(reads), 3)
	for _, read := range reads[1:] {
		require.Equal(t, "id:namespace-id", read)
	}
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
	source.Info.Data = map[string]string{"shared": "source", "cell.local": "source-local"}
	target := proto.CloneOf(source)
	target.Info.Data = map[string]string{"shared": "old", "cell.local": "target-local"}
	target.ReplicationConfig.ActiveClusterName = "cluster-b"
	target.FailoverVersion = 19
	target.FailoverHistory[0].FailoverVersion = 19
	sourceBefore, targetBefore := proto.CloneOf(source), proto.CloneOf(target)
	ignored := map[string]struct{}{"cell.local": {}}

	task, err := buildNamespaceReplicationRepairTask(
		source,
		target,
		ignored,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"shared":     "source",
		"cell.local": "target-local",
	}, task.GetInfo().GetData())
	require.Equal(t, "cluster-b", task.GetReplicationConfig().GetActiveClusterName())
	require.Equal(t, int64(19), task.GetFailoverVersion())
	require.Equal(t, int64(19), task.GetFailoverHistory()[0].GetFailoverVersion())
	require.True(t, proto.Equal(source.GetConfig().GetBadBinaries(), task.GetConfig().GetBadBinaries()))

	missingTask, err := buildNamespaceReplicationRepairTask(
		source,
		nil,
		ignored,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"shared": "source"}, missingTask.GetInfo().GetData())

	sourceTask, err := buildNamespaceReplicationRepairTask(
		source,
		source,
		ignored,
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
	)
	require.NoError(t, err)
	require.Nil(t, task.GetConfig().BadBinaries)
	require.Empty(t, task.GetConfig().GetBadBinaries().GetBinaries())
}

func TestNamespaceReplicationRepairerExecutes(t *testing.T) {
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
	verifier := testNamespaceReplicationVerifierWithClients(nil, map[string]*testNamespaceReplicationAdminClient{
		"source":    sourceClient,
		"cluster-b": targetClient,
	})
	verifier.now = func() time.Time { return time.Unix(100, 0).UTC() }
	verifyRequest := testNamespaceReplicationVerifyRequest()
	observation, err := verifier.observe(context.Background(), verifyRequest)
	require.NoError(t, err)
	plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(101, 0))
	require.NoError(t, err)
	require.Equal(t, int64(11), plan.ProposedSourceConfigVersion)
	require.Equal(t, []string{namespaceReplicationWarningInitialHistory}, plan.Clusters[0].Warnings)

	repairer := testRepairerFor(verifier)
	result, err := repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest: verifyRequest,
		Observation:   observation,
		Plan:          plan,
		Reason:        "repair test",
	})
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationRepairExecutionSucceeded, result.Status)
	require.Contains(t, result.StatusDetail, "success does not imply former replicas were synchronized")
	require.Equal(t, int64(11), source.GetConfigVersion())
	require.Equal(t, int64(11), target.GetConfigVersion())
	require.Equal(t, source.GetInfo().GetDescription(), target.GetInfo().GetDescription())
	require.Empty(t, target.GetFailoverHistory())
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
			verifier := testNamespaceReplicationVerifierWithClients(nil, map[string]*testNamespaceReplicationAdminClient{
				"source":    sourceClient,
				"cluster-b": targetClient,
			})
			verifyRequest := testNamespaceReplicationVerifyRequest()
			observation, err := verifier.observe(context.Background(), verifyRequest)
			require.NoError(t, err)
			plan, err := planNamespaceReplicationRepair(observation, "repair-id", time.Unix(101, 0))
			require.NoError(t, err)
			require.Equal(t, namespaceReplicationRepairActionCreate, plan.Clusters[0].Action)

			repairer := testRepairerFor(verifier)
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

	repairer := testRepairerFor(verifier)
	result, err := repairer.Execute(context.Background(), namespaceReplicationRepairExecutionRequest{
		VerifyRequest: verifyRequest,
		Observation:   observation,
		Plan:          plan,
		Reason:        "source race test",
	})
	require.ErrorContains(t, err, "source namespace changed after repair planning")
	require.Equal(t, namespaceReplicationRepairExecutionFailed, result.Status)
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
		// The read-back by ID rejects another namespace before comparing config.
		{name: "wrong identity", mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.Id = "other-id" }, wantError: "response has a different namespace ID"},
		{name: "not global", mutate: func(r *adminservice.GetNamespaceResponse) { r.IsGlobalNamespace = false }, wantError: "not global"},
		{name: "not admitted", outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED, wantError: "unexpected apply outcome"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			expected := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
			task, err := buildNamespaceReplicationRepairTask(expected, expected, nil)
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
			r := testRepairerFor(testNamespaceReplicationVerifierWithClients(expected, map[string]*testNamespaceReplicationAdminClient{"cluster-b": client}))
			result := namespaceReplicationRepairApplyResult{Action: namespaceReplicationRepairActionCreate}
			_, err = r.applyRepairAndReadback(t.Context(), peerRepairEndpoint{repairer: r, cluster: "cluster-b"}, namespaceReplicationRepairExecutionRequest{
				Plan: &namespaceReplicationRepairPlan{SourceCluster: "cluster-a", RepairID: "repair-id"}, Reason: "repair",
			}, task, &result)
			require.ErrorContains(t, err, test.wantError)
			require.False(t, result.Readback)
		})
	}
}

func TestRepairCarriesSourceRestoreInfo(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
	source.RestoreInfo = &persistencespb.NamespaceRestoreInfo{
		RestoreId:   "restore-id",
		HostCluster: "cluster-a",
		State:       enumspb.NAMESPACE_RESTORE_STATE_COMPLETED,
		StartTime:   timestamppb.New(time.Unix(30, 0)),
		CloseTime:   timestamppb.New(time.Unix(40, 0)),
	}
	target := proto.CloneOf(source)
	target.RestoreInfo = nil
	task, err := buildNamespaceReplicationRepairTask(source, target, nil)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, source.GetRestoreInfo(), task.GetRestoreInfo())

	for _, test := range []struct {
		name      string
		mutate    func(*adminservice.GetNamespaceResponse)
		wantError string
	}{
		{name: "matching"},
		{name: "different restore state", mutate: func(r *adminservice.GetNamespaceResponse) {
			r.RestoreInfo.State = enumspb.NAMESPACE_RESTORE_STATE_FAILED
		}, wantError: "config readback"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			actual := proto.CloneOf(source)
			if test.mutate != nil {
				test.mutate(actual)
			}
			client := testNamespaceReplicationTargetClient("cluster-b", actual)
			client.applyNamespaceMutationFn = func(*adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error) {
				return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
			}
			r := testRepairerFor(testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"cluster-b": client}))
			result := namespaceReplicationRepairApplyResult{Action: namespaceReplicationRepairActionApplySource}
			_, err := r.applyRepairAndReadback(t.Context(), peerRepairEndpoint{repairer: r, cluster: "cluster-b"}, namespaceReplicationRepairExecutionRequest{
				Plan: &namespaceReplicationRepairPlan{SourceCluster: "cluster-a", RepairID: "repair-id"}, Reason: "repair",
			}, task, &result)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.False(t, result.Readback)
				return
			}
			require.NoError(t, err)
			require.True(t, result.Readback)
		})
	}
}

// Repair writes only what verify's projections carry, and readback rebuilds the
// expected namespace from the repair task one top-level field at a time. A
// replicated field the projections skip is cleared on every target repair
// writes; a projected field readback skips fails every repair of a namespace
// that sets it. Nested fields are copied whole, so only top-level fields need
// this guard.
func TestRepairReadbackCoversEveryProjectedField(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
	source.RestoreInfo = &persistencespb.NamespaceRestoreInfo{
		RestoreId:   "restore-id",
		HostCluster: "cluster-a",
		State:       enumspb.NAMESPACE_RESTORE_STATE_COMPLETED,
	}

	// Without a value on the source, a projected field compares nil to nil and
	// the readback below would pass without covering it. Repair sets the
	// operation itself and compares versions outside the projections.
	notProjected := map[protoreflect.Name]bool{"namespace_operation": true, "config_version": true, "failover_version": true}
	configProjection, failoverProjection, err := namespaceReplicationProjections(source, nil)
	require.NoError(t, err)
	fields := configProjection.ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if notProjected[field.Name()] {
			continue
		}
		require.Truef(t, configProjection.ProtoReflect().Has(field) || failoverProjection.ProtoReflect().Has(field),
			"the projections leave %s unset: compare it in verify and set it on the source above, "+
				"or list it in notProjected only if repair may clear it on the targets it writes", field.Name())
	}

	task, err := buildNamespaceReplicationRepairTask(source, source, nil)
	require.NoError(t, err)
	require.NoError(t, validateNamespaceRepairReadback(task, proto.CloneOf(source), nil))
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
	r := testRepairerFor(testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"cluster-b": client}))
	result := r.applyTargetRepair(t.Context(), namespaceReplicationRepairExecutionRequest{
		Observation: &namespaceReplicationObservation{},
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
			verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"cluster-b": client})
			verifier.dataKeysToIgnore = map[string]struct{}{"local-key": {}}
			r := testRepairerFor(verifier)
			result := r.applyTargetRepair(t.Context(), namespaceReplicationRepairExecutionRequest{
				Observation: &namespaceReplicationObservation{targetSnapshots: map[string]*adminservice.GetNamespaceResponse{"cluster-b": frozen}},
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
	for _, failoverStatus := range task.GetFailoverHistory() {
		snapshot.FailoverHistory = append(snapshot.FailoverHistory, proto.CloneOf(failoverStatus))
	}
}

// testRepairerFor builds a repairer over the verifier's source client, which
// relays peer reads and writes as the server does.
func testRepairerFor(verifier *namespaceReplicationVerifier) *namespaceReplicationRepairer {
	reader := verifier.reader.(adminNamespaceReplicationReader)
	return &namespaceReplicationRepairer{verifier: verifier, client: reader.client, rpcTimeout: reader.rpcTimeout}
}

func TestPeerRepairEndpointWritesThroughRelay(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
	mutation := &adminservice.ApplyNamespaceMutationRequest{
		SourceCluster: "cluster-a",
		Attribution:   &adminservice.NamespaceMutationAttribution{Origin: adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR},
	}
	for _, test := range []struct {
		name      string
		relay     func(*adminservice.ApplyRemoteClusterNamespaceRepairRequest) (*adminservice.ApplyRemoteClusterNamespaceRepairResponse, error)
		wantError string
	}{
		{
			name: "relays to the named peer",
			relay: func(request *adminservice.ApplyRemoteClusterNamespaceRepairRequest) (*adminservice.ApplyRemoteClusterNamespaceRepairResponse, error) {
				require.Equal(t, "cluster-b", request.GetClusterName())
				require.True(t, proto.Equal(mutation, request.GetMutation()))
				return &adminservice.ApplyRemoteClusterNamespaceRepairResponse{
					ClusterName: "cluster-b",
					Response:    &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED},
				}, nil
			},
		},
		{
			name: "source frontend without the relay",
			relay: func(*adminservice.ApplyRemoteClusterNamespaceRepairRequest) (*adminservice.ApplyRemoteClusterNamespaceRepairResponse, error) {
				return nil, status.Error(codes.Unimplemented, "unknown method ApplyRemoteClusterNamespaceRepair")
			},
			wantError: errNamespaceReplicationRepairRelayUnsupported.Error(),
		},
		{
			name: "relay reports another cluster",
			relay: func(*adminservice.ApplyRemoteClusterNamespaceRepairRequest) (*adminservice.ApplyRemoteClusterNamespaceRepairResponse, error) {
				return &adminservice.ApplyRemoteClusterNamespaceRepairResponse{ClusterName: "cluster-c"}, nil
			},
			wantError: `cluster identity mismatch: expected "cluster-b", got "cluster-c"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sourceClient := testNamespaceReplicationSourceClient(source)
			sourceClient.remoteClusterRepairFn = test.relay
			r := testRepairerFor(testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"source": sourceClient}))
			response, err := peerRepairEndpoint{repairer: r, cluster: "cluster-b"}.Apply(t.Context(), mutation)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED, response.GetOutcome())
		})
	}
}
