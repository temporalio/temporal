package tdbg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNamespaceReplicationVerifier_Status(t *testing.T) {
	tests := []struct {
		name              string
		sourceVersion     int64
		targetVersion     int64
		mutate            func(*adminservice.GetNamespaceResponse, *adminservice.GetNamespaceResponse)
		missing           bool
		wantStatus        string
		wantPresence      string
		wantConfigMatch   string
		wantFailoverMatch string
		wantDifference    string
		wantError         string
	}{
		{
			name:          "healthy with harmless normalization and version skew",
			sourceVersion: 10,
			targetVersion: 9,
			mutate: func(source, target *adminservice.GetNamespaceResponse) {
				source.Info.Data = nil
				source.Config.CustomSearchAttributeAliases = nil
				source.Config.BadBinaries = nil
				target.ReplicationConfig.Clusters = []*replicationpb.ClusterReplicationConfig{
					{ClusterName: "cluster-b"},
					{ClusterName: "cluster-a"},
				}
				target.Info.Data = map[string]string{}
				target.Config.CustomSearchAttributeAliases = map[string]string{}
				target.Config.BadBinaries = &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}
			},
			wantStatus:        namespaceReplicationStatusHealthy,
			wantPresence:      namespaceReplicationPresencePresent,
			wantConfigMatch:   namespaceReplicationMatchMatch,
			wantFailoverMatch: namespaceReplicationMatchMatch,
		},
		{
			name:          "config mismatch",
			sourceVersion: 11,
			targetVersion: 10,
			mutate: func(_, target *adminservice.GetNamespaceResponse) {
				target.Info.Description = "different and must not be printed"
			},
			wantStatus:        namespaceReplicationStatusRepairRequired,
			wantPresence:      namespaceReplicationPresencePresent,
			wantConfigMatch:   namespaceReplicationMatchMismatch,
			wantFailoverMatch: namespaceReplicationMatchMatch,
			wantDifference:    "info.description",
		},
		{
			name:              "matching target version ahead",
			sourceVersion:     10,
			targetVersion:     12,
			wantStatus:        namespaceReplicationStatusRepairRequired,
			wantPresence:      namespaceReplicationPresencePresent,
			wantConfigMatch:   namespaceReplicationMatchMatch,
			wantFailoverMatch: namespaceReplicationMatchMatch,
		},
		{
			name:          "missing target",
			sourceVersion: 10,
			missing:       true,
			wantStatus:    namespaceReplicationStatusRepairRequired,
			wantPresence:  namespaceReplicationPresenceMissing,
		},
		{
			name:          "failover projection mismatch",
			sourceVersion: 10,
			targetVersion: 10,
			mutate: func(_, target *adminservice.GetNamespaceResponse) {
				target.ReplicationConfig.ActiveClusterName = "cluster-b"
			},
			wantStatus:        namespaceReplicationStatusBlocked,
			wantPresence:      namespaceReplicationPresencePresent,
			wantConfigMatch:   namespaceReplicationMatchMatch,
			wantFailoverMatch: namespaceReplicationMatchMismatch,
			wantDifference:    "replication.active_cluster",
		},
		{
			name:          "target failover version ahead",
			sourceVersion: 10,
			targetVersion: 10,
			mutate: func(_, target *adminservice.GetNamespaceResponse) {
				target.FailoverVersion++
			},
			wantStatus:        namespaceReplicationStatusBlocked,
			wantPresence:      namespaceReplicationPresencePresent,
			wantConfigMatch:   namespaceReplicationMatchMatch,
			wantFailoverMatch: namespaceReplicationMatchMatch,
		},
		{
			name:          "local target",
			sourceVersion: 10,
			targetVersion: 10,
			mutate: func(_, target *adminservice.GetNamespaceResponse) {
				target.IsGlobalNamespace = false
			},
			wantStatus:     namespaceReplicationStatusBlocked,
			wantPresence:   namespaceReplicationPresencePresent,
			wantDifference: "namespace.is_global",
			wantError:      "not global",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, test.sourceVersion, 20)
			var target *adminservice.GetNamespaceResponse
			if !test.missing {
				target = proto.Clone(source).(*adminservice.GetNamespaceResponse)
				target.ConfigVersion = test.targetVersion
			}
			if test.mutate != nil {
				test.mutate(source, target)
			}

			verifier := testNamespaceReplicationVerifier(source, map[string]*adminservice.GetNamespaceResponse{"cluster-b": target})
			result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			require.Equal(t, test.wantStatus, result.Status)
			require.Len(t, result.Clusters, 2)
			cluster := result.Clusters[1]
			require.Equal(t, test.wantPresence, cluster.Presence)
			if test.wantConfigMatch != "" {
				require.Equal(t, test.wantConfigMatch, cluster.ConfigMatch)
			}
			if test.wantFailoverMatch != "" {
				require.Equal(t, test.wantFailoverMatch, cluster.FailoverMatch)
			}
			if test.wantDifference != "" {
				require.Contains(t, cluster.Differences, test.wantDifference)
			}
			if test.wantError != "" {
				require.Contains(t, cluster.Error, test.wantError)
			}
			if test.wantStatus == namespaceReplicationStatusHealthy {
				require.Equal(t, result.SourceConfigFingerprint, cluster.ConfigFingerprint)
			}
		})
	}
}

func TestNamespaceReplicationVerifier_SourceOnlyDoesNotListClusterMetadata(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
	sourceClient := testNamespaceReplicationSourceClient(source)
	sourceClient.listClustersFn = func(*adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error) {
		return nil, errors.New("ListClusters must not be called without targets")
	}
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
		"source-address": sourceClient,
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusHealthy, result.Status)
	require.Len(t, result.Clusters, 1)
}

func TestNamespaceReplicationVerifier_NameCollisionBlocksRepair(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	nameCollision := testNamespaceResponse("namespace", "other-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	client := testNamespaceReplicationTargetClient("cluster-b", nil)
	client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		if request.GetNamespace() != "" {
			return proto.Clone(nameCollision).(*adminservice.GetNamespaceResponse), nil
		}
		return nil, serviceerror.NewNamespaceNotFound("namespace-id")
	}
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
		"cluster-b-address": client,
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Equal(t, namespaceReplicationPresenceNameCollision, result.Clusters[1].Presence)
	require.NotContains(t, result.Clusters[1].Error, "other-id")
}

func TestNamespaceReplicationVerifier_IDCollisionBlocksRepair(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	idCollision := testNamespaceResponse("other-name", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	client := testNamespaceReplicationTargetClient("cluster-b", nil)
	client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		if request.GetId() != "" {
			return proto.Clone(idCollision).(*adminservice.GetNamespaceResponse), nil
		}
		return nil, serviceerror.NewNamespaceNotFound("namespace")
	}
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
		"cluster-b-address": client,
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Equal(t, namespaceReplicationPresenceIDCollision, result.Clusters[1].Presence)
	require.NotContains(t, result.Clusters[1].Error, "other-name")
}

func TestNamespaceReplicationVerifier_UnavailableTargetBlocksRepair(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Equal(t, namespaceReplicationPresenceUnavailable, result.Clusters[1].Presence)
}

func TestNamespaceReplicationVerifier_ClusterMetadataFailureReturnsBlockedResult(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	sourceClient := testNamespaceReplicationSourceClient(source)
	sourceClient.listClustersFn = func(*adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error) {
		return nil, errors.New("metadata unavailable")
	}
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
		"source-address": sourceClient,
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Contains(t, result.StatusDetail, "metadata unavailable")
}

func TestNamespaceReplicationVerifier_SourceMovementIsInconclusive(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	moved := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	moved.ConfigVersion++
	target := proto.Clone(source).(*adminservice.GetNamespaceResponse)

	sourceClient := testNamespaceReplicationSourceClient(source)
	getCount := 0
	sourceClient.getNamespaceFn = func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		getCount++
		if getCount == 1 {
			return proto.Clone(source).(*adminservice.GetNamespaceResponse), nil
		}
		return proto.Clone(moved).(*adminservice.GetNamespaceResponse), nil
	}
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
		"source-address":    sourceClient,
		"cluster-b-address": testNamespaceReplicationTargetClient("cluster-b", target),
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusInconclusiveSourceChanged, result.Status)
}

func TestValidateSourceNamespace(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*adminservice.GetNamespaceResponse)
		contains string
	}{
		{
			name: "local namespace",
			mutate: func(response *adminservice.GetNamespaceResponse) {
				response.IsGlobalNamespace = false
			},
			contains: "not global",
		},
		{
			name: "source not active",
			mutate: func(response *adminservice.GetNamespaceResponse) {
				response.ReplicationConfig.ActiveClusterName = "cluster-b"
			},
			contains: "is not active",
		},
		{
			name: "handover",
			mutate: func(response *adminservice.GetNamespaceResponse) {
				response.ReplicationConfig.State = enumspb.REPLICATION_STATE_HANDOVER
			},
			contains: "not NORMAL",
		},
		{
			name: "deleted namespace",
			mutate: func(response *adminservice.GetNamespaceResponse) {
				response.Info.State = enumspb.NAMESPACE_STATE_DELETED
			},
			contains: "only REGISTERED or DEPRECATED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 1, 1)
			test.mutate(response)
			require.ErrorContains(t, validateSourceNamespace(response, "cluster-a"), test.contains)
		})
	}
}

func TestValidateExpectedClusters(t *testing.T) {
	response := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-b", "cluster-a"}, 1, 1)
	clusters, err := validateExpectedClusters(response, "cluster-a")
	require.NoError(t, err)
	require.Equal(t, []string{"cluster-a", "cluster-b"}, clusters)

	response.ReplicationConfig.Clusters = append(
		response.ReplicationConfig.Clusters,
		&replicationpb.ClusterReplicationConfig{ClusterName: "cluster-b"},
	)
	_, err = validateExpectedClusters(response, "cluster-a")
	require.ErrorContains(t, err, "appears 2 times")

	response.ReplicationConfig.Clusters = []*replicationpb.ClusterReplicationConfig{{ClusterName: "cluster-b"}}
	_, err = validateExpectedClusters(response, "cluster-a")
	require.ErrorContains(t, err, "exactly once")
}

func TestListClusterMetadataPagesAndRejectsRepeatedToken(t *testing.T) {
	client := &testNamespaceReplicationAdminClient{}
	client.listClustersFn = func(request *adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error) {
		if len(request.GetNextPageToken()) == 0 {
			return &adminservice.ListClustersResponse{
				Clusters: []*persistencespb.ClusterMetadata{
					{ClusterName: "cluster-b"},
					{ClusterName: "unrelated"},
					{ClusterName: "unrelated"},
					{},
				},
				NextPageToken: []byte("next"),
			}, nil
		}
		return &adminservice.ListClustersResponse{
			Clusters: []*persistencespb.ClusterMetadata{{ClusterName: "cluster-c"}},
		}, nil
	}
	metadata, err := listClusterMetadata(
		context.Background(),
		time.Second,
		client,
		[]string{"cluster-b", "cluster-c"},
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"cluster-b", "cluster-c"}, []string{
		metadata["cluster-b"].GetClusterName(),
		metadata["cluster-c"].GetClusterName(),
	})

	client.listClustersFn = func(*adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error) {
		return &adminservice.ListClustersResponse{NextPageToken: []byte("same")}, nil
	}
	_, err = listClusterMetadata(
		context.Background(),
		time.Second,
		client,
		[]string{"cluster-b"},
	)
	require.ErrorContains(t, err, "repeated next-page token")
}

func TestNamespaceReplicationProjectionsCoverReplicatedFields(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	tests := []struct {
		name           string
		difference     string
		configChange   bool
		failoverChange bool
		mutate         func(*adminservice.GetNamespaceResponse)
	}{
		{name: "namespace ID", difference: "namespace.id", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.Id = "other-id" }},
		{name: "namespace name", difference: "namespace.name", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.Name = "other-name" }},
		{name: "namespace state", difference: "info.state", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.State = enumspb.NAMESPACE_STATE_DEPRECATED }},
		{name: "description", difference: "info.description", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.Description = "other" }},
		{name: "owner email", difference: "info.owner_email", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.OwnerEmail = "other@example.com" }},
		{name: "data", difference: "info.data", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Info.Data["key"] = "other" }},
		{name: "retention", difference: "config.workflow_execution_retention_ttl", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) {
			r.Config.WorkflowExecutionRetentionTtl = durationpb.New(48 * time.Hour)
		}},
		{name: "bad binaries", difference: "config.bad_binaries", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Config.BadBinaries.Binaries["checksum"].Reason = "other" }},
		{name: "history archival state", difference: "config.history_archival_state", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) {
			r.Config.HistoryArchivalState = enumspb.ARCHIVAL_STATE_DISABLED
		}},
		{name: "history archival URI", difference: "config.history_archival_uri", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Config.HistoryArchivalUri = "s3://other-history" }},
		{name: "visibility archival state", difference: "config.visibility_archival_state", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) {
			r.Config.VisibilityArchivalState = enumspb.ARCHIVAL_STATE_DISABLED
		}},
		{name: "visibility archival URI", difference: "config.visibility_archival_uri", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.Config.VisibilityArchivalUri = "s3://other-visibility" }},
		{name: "search attribute aliases", difference: "config.custom_search_attribute_aliases", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) {
			r.Config.CustomSearchAttributeAliases["Alias"] = "Keyword02"
		}},
		{name: "cluster membership", difference: "replication.clusters", configChange: true, mutate: func(r *adminservice.GetNamespaceResponse) {
			r.ReplicationConfig.Clusters = r.ReplicationConfig.Clusters[:1]
		}},
		{name: "active cluster", difference: "replication.active_cluster", failoverChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.ReplicationConfig.ActiveClusterName = "cluster-b" }},
		{name: "replication state", difference: "replication.state", failoverChange: true, mutate: func(r *adminservice.GetNamespaceResponse) {
			r.ReplicationConfig.State = enumspb.REPLICATION_STATE_HANDOVER
		}},
		{name: "failover history", difference: "replication.failover_history", failoverChange: true, mutate: func(r *adminservice.GetNamespaceResponse) { r.FailoverHistory[0].FailoverVersion++ }},
	}

	sourceConfig, sourceFailover, err := namespaceReplicationProjections(source)
	require.NoError(t, err)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := proto.Clone(source).(*adminservice.GetNamespaceResponse)
			test.mutate(target)
			targetConfig, targetFailover, err := namespaceReplicationProjections(target)
			require.NoError(t, err)
			require.Equal(t, !test.configChange, proto.Equal(sourceConfig, targetConfig))
			require.Equal(t, !test.failoverChange, proto.Equal(sourceFailover, targetFailover))
			require.Contains(t, namespaceReplicationDifferences(source, target), test.difference)
		})
	}

	versionOnly := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	versionOnly.ConfigVersion++
	versionOnly.FailoverVersion++
	versionConfig, versionFailover, err := namespaceReplicationProjections(versionOnly)
	require.NoError(t, err)
	require.True(t, proto.Equal(sourceConfig, versionConfig))
	require.True(t, proto.Equal(sourceFailover, versionFailover))
	require.Empty(t, namespaceReplicationDifferences(source, versionOnly))
}

func TestParseNamespaceReplicationAddressOverrides(t *testing.T) {
	overrides, err := parseNamespaceReplicationAddressOverrides([]string{
		"cluster-b=host-b:7233",
		" cluster-c = host-c:7233 ",
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"cluster-b": "host-b:7233",
		"cluster-c": "host-c:7233",
	}, overrides)

	_, err = parseNamespaceReplicationAddressOverrides([]string{"cluster-b=one", "cluster-b=two"})
	require.ErrorContains(t, err, "duplicate")
	_, err = parseNamespaceReplicationAddressOverrides([]string{"invalid"})
	require.ErrorContains(t, err, "expected")
}

func TestResolveClusterAddresses(t *testing.T) {
	addresses, err := resolveClusterAddresses(
		[]string{"cluster-a", "cluster-b", "cluster-c"},
		"cluster-a",
		"source:7233",
		map[string]*persistencespb.ClusterMetadata{
			"cluster-b": {ClusterName: "cluster-b", ClusterAddress: "persisted-b:7233"},
		},
		map[string]string{"cluster-c": "override-c:7233"},
	)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"cluster-a": "source:7233",
		"cluster-b": "persisted-b:7233",
		"cluster-c": "override-c:7233",
	}, addresses)

	_, err = resolveClusterAddresses(
		[]string{"cluster-a"},
		"cluster-a",
		"source:7233",
		nil,
		map[string]string{"cluster-a": "other:7233"},
	)
	require.ErrorContains(t, err, "source cluster address")
}

func TestTDBGExitCode(t *testing.T) {
	require.Equal(t, 1, tdbgExitCode(errors.New("ordinary")))
	require.Equal(t, 2, tdbgExitCode(cli.Exit("blocked", 2)))
	require.Equal(t, namespaceReplicationRepairRequiredExitCode, tdbgExitCode(cli.Exit(
		"repair required",
		namespaceReplicationRepairRequiredExitCode,
	)))
}

func TestNamespaceReplicationVerificationJSONRedactsNamespaceValues(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
	target := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	target.ConfigVersion = 10
	target.Info.Description = "sensitive-description"
	verifier := testNamespaceReplicationVerifier(source, map[string]*adminservice.GetNamespaceResponse{
		"cluster-b": target,
	})
	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)

	payload, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "sensitive-description")
	require.Contains(t, string(payload), "info.description")
}

func TestNamespaceReplicationVerifyRequiresExplicitSelector(t *testing.T) {
	app := NewCliApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}
	app.ExitErrHandler = func(*cli.Context, error) {}

	err := app.Run([]string{"tdbg", "namespace", "replication", "verify"})
	require.ErrorContains(t, err, "exactly one of --namespace or --namespace-id")
}

func TestNamespaceReplicationVerifyCommandUsesConfiguredAddressAndOverrides(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	sourceClient := testNamespaceReplicationSourceClient(source)
	sourceClient.listClustersFn = func(*adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error) {
		return nil, errors.New("ListClusters must not be called when every target has an override")
	}
	factory := &testNamespaceReplicationCLIClientFactory{
		sourceAddress: "configured-source-address",
		clients: map[string]*testNamespaceReplicationAdminClient{
			"configured-source-address": sourceClient,
			"override-address":          testNamespaceReplicationTargetClient("cluster-b", source),
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg",
		"--tls-server-name", "source.example.com",
		"namespace",
		"replication",
		"verify",
		"--namespace", "namespace",
		"--cluster-address", "cluster-b=override-address",
		"--print-json",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"configured-source-address", "override-address"}, factory.openedAddresses)
	require.Equal(t, []string{"source.example.com", ""}, factory.openedTLSServerNames)
	var result namespaceReplicationVerificationResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, namespaceReplicationStatusHealthy, result.Status)
}

func TestNamespaceReplicationVerifyCommandPrintsBlockedResult(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
	source.ReplicationConfig.State = enumspb.REPLICATION_STATE_HANDOVER
	factory := &testNamespaceReplicationCLIClientFactory{
		sourceAddress: "configured-source-address",
		clients: map[string]*testNamespaceReplicationAdminClient{
			"configured-source-address": testNamespaceReplicationSourceClient(source),
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg",
		"namespace",
		"replication",
		"verify",
		"--namespace", "namespace",
		"--print-json",
	})
	require.Error(t, err)
	require.Equal(t, 2, tdbgExitCode(err))
	var result namespaceReplicationVerificationResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Contains(t, result.StatusDetail, "not NORMAL")
}

func TestNamespaceReplicationVerifyCommandPrintsRepairTable(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 11, 20)
	target := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	target.ConfigVersion = 10
	target.Info.Description = "different"
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
		"verify",
		"--namespace", "namespace",
		"--cluster-address", "cluster-b=override-address",
	})
	require.Error(t, err)
	require.Equal(t, namespaceReplicationRepairRequiredExitCode, tdbgExitCode(err))
	require.Contains(t, output.String(), "REPAIR_REQUIRED")
	require.Contains(t, output.String(), "info.description")
}

func TestNamespaceReplicationVerifyCommandPropagatesJSONWriteFailure(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
	factory := &testNamespaceReplicationCLIClientFactory{
		sourceAddress: "configured-source-address",
		clients: map[string]*testNamespaceReplicationAdminClient{
			"configured-source-address": testNamespaceReplicationSourceClient(source),
		},
	}
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = factory
		params.Writer = namespaceReplicationErrorWriter{}
		params.ErrWriter = &bytes.Buffer{}
	})
	app.ExitErrHandler = func(*cli.Context, error) {}

	err := app.Run([]string{
		"tdbg",
		"namespace",
		"replication",
		"verify",
		"--namespace", "namespace",
		"--print-json",
	})
	require.ErrorContains(t, err, "write failed")
	require.Equal(t, 2, tdbgExitCode(err))
}

func testNamespaceReplicationCLIApp(
	factory ClientFactory,
) (*cli.App, *bytes.Buffer) {
	output := &bytes.Buffer{}
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = factory
		params.Writer = output
		params.ErrWriter = &bytes.Buffer{}
	})
	app.ExitErrHandler = func(*cli.Context, error) {}
	return app, output
}

func testNamespaceReplicationVerifier(
	source *adminservice.GetNamespaceResponse,
	targets map[string]*adminservice.GetNamespaceResponse,
) *namespaceReplicationVerifier {
	clients := map[string]*testNamespaceReplicationAdminClient{
		"source-address": testNamespaceReplicationSourceClient(source),
	}
	for cluster, target := range targets {
		clients[cluster+"-address"] = testNamespaceReplicationTargetClient(cluster, target)
	}
	return testNamespaceReplicationVerifierWithClients(source, clients)
}

func testNamespaceReplicationVerifierWithClients(
	source *adminservice.GetNamespaceResponse,
	clients map[string]*testNamespaceReplicationAdminClient,
) *namespaceReplicationVerifier {
	if clients["source-address"] == nil {
		clients["source-address"] = testNamespaceReplicationSourceClient(source)
	}
	verifier := newNamespaceReplicationVerifier(&testNamespaceReplicationClientProvider{clients: clients})
	verifier.now = func() time.Time { return time.Unix(123, 0).UTC() }
	return verifier
}

func testNamespaceReplicationVerifyRequest() namespaceReplicationVerifyRequest {
	return namespaceReplicationVerifyRequest{
		SourceAddress: "source-address",
		Selector:      namespaceReplicationSelector{Name: "namespace"},
	}
}

func testNamespaceReplicationSourceClient(
	source *adminservice.GetNamespaceResponse,
) *testNamespaceReplicationAdminClient {
	return &testNamespaceReplicationAdminClient{
		describeClusterFn: func() (*adminservice.DescribeClusterResponse, error) {
			return &adminservice.DescribeClusterResponse{ClusterName: "cluster-a"}, nil
		},
		getNamespaceFn: func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
			return proto.Clone(source).(*adminservice.GetNamespaceResponse), nil
		},
		listClustersFn: func(*adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error) {
			clusters := make([]*persistencespb.ClusterMetadata, 0)
			for _, cluster := range source.GetReplicationConfig().GetClusters() {
				clusters = append(clusters, &persistencespb.ClusterMetadata{
					ClusterName:    cluster.GetClusterName(),
					ClusterAddress: cluster.GetClusterName() + "-address",
				})
			}
			return &adminservice.ListClustersResponse{Clusters: clusters}, nil
		},
	}
}

func testNamespaceReplicationTargetClient(
	cluster string,
	target *adminservice.GetNamespaceResponse,
) *testNamespaceReplicationAdminClient {
	return &testNamespaceReplicationAdminClient{
		describeClusterFn: func() (*adminservice.DescribeClusterResponse, error) {
			return &adminservice.DescribeClusterResponse{ClusterName: cluster}, nil
		},
		getNamespaceFn: func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
			if target == nil {
				return nil, serviceerror.NewNamespaceNotFound("namespace")
			}
			return proto.Clone(target).(*adminservice.GetNamespaceResponse), nil
		},
	}
}

func testNamespaceResponse(
	name string,
	id string,
	activeCluster string,
	clusters []string,
	configVersion int64,
	failoverVersion int64,
) *adminservice.GetNamespaceResponse {
	clusterConfigs := make([]*replicationpb.ClusterReplicationConfig, 0, len(clusters))
	for _, cluster := range clusters {
		clusterConfigs = append(clusterConfigs, &replicationpb.ClusterReplicationConfig{ClusterName: cluster})
	}
	return &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Id:          id,
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: "description",
			OwnerEmail:  "owner@example.com",
			Data:        map[string]string{"key": "value"},
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(24 * time.Hour),
			BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{
				"checksum": {Reason: "bad", Operator: "operator", CreateTime: timestamppb.New(time.Unix(10, 0))},
			}},
			HistoryArchivalState:         enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:           "s3://history",
			VisibilityArchivalState:      enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:        "s3://visibility",
			CustomSearchAttributeAliases: map[string]string{"Alias": "Keyword01"},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters:          clusterConfigs,
			State:             enumspb.REPLICATION_STATE_NORMAL,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
		FailoverHistory: []*replicationpb.FailoverStatus{
			{FailoverTime: timestamppb.New(time.Unix(20, 0)), FailoverVersion: failoverVersion},
		},
		IsGlobalNamespace: true,
	}
}

type testNamespaceReplicationClientProvider struct {
	clients map[string]*testNamespaceReplicationAdminClient
}

type testNamespaceReplicationCLIClientFactory struct {
	ClientFactory
	sourceAddress        string
	clients              map[string]*testNamespaceReplicationAdminClient
	openedAddresses      []string
	openedTLSServerNames []string
}

func (f *testNamespaceReplicationCLIClientFactory) FrontendAddress(*cli.Context) string {
	return f.sourceAddress
}

func (f *testNamespaceReplicationCLIClientFactory) AdminClientForAddress(
	_ *cli.Context,
	address string,
	tlsServerName string,
) (adminservice.AdminServiceClient, io.Closer, error) {
	f.openedAddresses = append(f.openedAddresses, address)
	f.openedTLSServerNames = append(f.openedTLSServerNames, tlsServerName)
	client := f.clients[address]
	if client == nil {
		return nil, nil, fmt.Errorf("unknown address %q", address)
	}
	return client, testNamespaceReplicationCloser{}, nil
}

func (p *testNamespaceReplicationClientProvider) OpenSource(
	address string,
) (*namespaceReplicationAdminClientConnection, error) {
	return p.open(address)
}

func (p *testNamespaceReplicationClientProvider) OpenTarget(
	address string,
) (*namespaceReplicationAdminClientConnection, error) {
	return p.open(address)
}

func (p *testNamespaceReplicationClientProvider) open(
	address string,
) (*namespaceReplicationAdminClientConnection, error) {
	client := p.clients[address]
	if client == nil {
		return nil, errors.New("unknown address")
	}
	return &namespaceReplicationAdminClientConnection{
		client: client,
		closer: testNamespaceReplicationCloser{},
	}, nil
}

type testNamespaceReplicationCloser struct{}

func (testNamespaceReplicationCloser) Close() error { return nil }

type namespaceReplicationErrorWriter struct{}

func (namespaceReplicationErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

type testNamespaceReplicationAdminClient struct {
	adminservice.AdminServiceClient
	describeClusterFn func() (*adminservice.DescribeClusterResponse, error)
	getNamespaceFn    func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error)
	listClustersFn    func(*adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error)
}

func (c *testNamespaceReplicationAdminClient) DescribeCluster(
	context.Context,
	*adminservice.DescribeClusterRequest,
	...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	return c.describeClusterFn()
}

func (c *testNamespaceReplicationAdminClient) GetNamespace(
	_ context.Context,
	request *adminservice.GetNamespaceRequest,
	_ ...grpc.CallOption,
) (*adminservice.GetNamespaceResponse, error) {
	return c.getNamespaceFn(request)
}

func (c *testNamespaceReplicationAdminClient) ListClusters(
	_ context.Context,
	request *adminservice.ListClustersRequest,
	_ ...grpc.CallOption,
) (*adminservice.ListClustersResponse, error) {
	return c.listClustersFn(request)
}

var _ io.Closer = testNamespaceReplicationCloser{}
