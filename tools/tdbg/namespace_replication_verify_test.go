package tdbg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNamespaceReplicationVerifierStatus(t *testing.T) {
	t.Parallel()
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
		wantDetail        string
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
			wantDetail:        "replicas block safe repair: cluster-b (failover state differs from the source)",
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
			wantDetail:        "replicas block safe repair: cluster-b (failover version 21 is ahead of the source's 20)",
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
			wantDetail:     "replicas block safe repair: cluster-b (target namespace is not global)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
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
			require.Equal(t, test.wantDetail, result.StatusDetail)
			if test.wantStatus == namespaceReplicationStatusHealthy {
				require.Equal(t, result.SourceConfigFingerprint, cluster.ConfigFingerprint)
			}
		})
	}
}

func TestNamespaceReplicationVerifierObserveCapturesReusableState(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse(
		"namespace",
		"namespace-id",
		"cluster-a",
		[]string{"cluster-a", "cluster-b", "cluster-c"},
		10,
		20,
	)
	finalSource := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	slices.Reverse(finalSource.ReplicationConfig.Clusters)
	target := proto.Clone(source).(*adminservice.GetNamespaceResponse)

	sourceClient := testNamespaceReplicationSourceClient(source)
	sourceReads := 0
	sourceClient.getNamespaceFn = func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		sourceReads++
		if sourceReads == 1 {
			return proto.Clone(source).(*adminservice.GetNamespaceResponse), nil
		}
		return proto.Clone(finalSource).(*adminservice.GetNamespaceResponse), nil
	}
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
		"source":    sourceClient,
		"cluster-b": testNamespaceReplicationTargetClient("cluster-b", target),
		"cluster-c": testNamespaceReplicationTargetClient("cluster-c", nil),
	})

	observation, err := verifier.observe(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, 2, sourceReads)
	require.Equal(t, namespaceReplicationStatusRepairRequired, observation.result.Status)
	require.True(t, proto.Equal(finalSource, observation.sourceSnapshot))
	require.True(t, proto.Equal(target, observation.targetSnapshots["cluster-b"]))
	missing, found := observation.targetSnapshots["cluster-c"]
	require.True(t, found)
	require.Nil(t, missing)
}

func TestNamespaceReplicationVerifierInitialHistoryWarning(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		mutate  func(*adminservice.GetNamespaceResponse, *adminservice.GetNamespaceResponse)
		status  string
		warning bool
	}{
		{name: "initial history only", status: namespaceReplicationStatusHealthy, warning: true},
		{name: "config repair remains allowed", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.Info.Description = "stale"
		}, status: namespaceReplicationStatusRepairRequired, warning: true},
		{name: "stale failover", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.FailoverVersion--
		}, status: namespaceReplicationStatusBlocked},
		{name: "target failover ahead", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.FailoverVersion++
		}, status: namespaceReplicationStatusBlocked},
		{name: "different nonempty history", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.FailoverHistory = []*replicationpb.FailoverStatus{{FailoverVersion: 19}}
		}, status: namespaceReplicationStatusBlocked},
		{name: "different active cluster", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.ReplicationConfig.ActiveClusterName = "cluster-b"
		}, status: namespaceReplicationStatusBlocked},
		{name: "handover", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.ReplicationConfig.State = enumspb.REPLICATION_STATE_HANDOVER
		}, status: namespaceReplicationStatusBlocked},
		{name: "multiple history entries", mutate: func(source, _ *adminservice.GetNamespaceResponse) {
			source.FailoverHistory = append(source.FailoverHistory, &replicationpb.FailoverStatus{FailoverVersion: 19})
		}, status: namespaceReplicationStatusBlocked},
		{name: "history not at current version", mutate: func(source, _ *adminservice.GetNamespaceResponse) {
			source.FailoverHistory[0].FailoverVersion--
		}, status: namespaceReplicationStatusBlocked},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			target := proto.CloneOf(source)
			target.FailoverHistory = nil
			if test.mutate != nil {
				test.mutate(source, target)
			}
			v := testNamespaceReplicationVerifier(source, map[string]*adminservice.GetNamespaceResponse{"cluster-b": target})
			result, err := v.Verify(t.Context(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			require.Equal(t, test.status, result.Status)
			require.Equal(t, test.warning, len(result.Clusters[1].Warnings) != 0)
			require.Equal(t, namespaceReplicationMatchMismatch, result.Clusters[1].FailoverMatch)
			require.Contains(t, result.Clusters[1].Differences, "replication.failover_history")
			if test.warning {
				app := NewCliApp()
				var output bytes.Buffer
				app.Writer = &output
				require.NoError(t, printNamespaceReplicationVerification(cli.NewContext(app, nil, nil), result))
				require.Contains(t, output.String(), "WARNINGS")
				require.Contains(t, output.String(), "failover history entry;")
				payload, err := json.Marshal(result)
				require.NoError(t, err)
				require.Contains(t, string(payload), namespaceReplicationWarningInitialHistory)
				result.Clusters[1].Warnings = []string{"reworded warning"}
				deriveNamespaceReplicationStatus(result)
				require.Equal(t, test.status, result.Status)
			}
		})
	}
}

func TestNamespaceReplicationVerifierReplicationStateAndClusterValidation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*adminservice.GetNamespaceResponse, *adminservice.GetNamespaceResponse)
		status string
	}{
		{name: "unspecified source is normal", mutate: func(source, _ *adminservice.GetNamespaceResponse) {
			source.ReplicationConfig.State = enumspb.REPLICATION_STATE_UNSPECIFIED
		}, status: namespaceReplicationStatusHealthy},
		{name: "unspecified target is normal", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.ReplicationConfig.State = enumspb.REPLICATION_STATE_UNSPECIFIED
		}, status: namespaceReplicationStatusHealthy},
		{name: "unknown source state", mutate: func(source, _ *adminservice.GetNamespaceResponse) {
			source.ReplicationConfig.State = enumspb.ReplicationState(99)
		}, status: namespaceReplicationStatusBlocked},
		{name: "duplicate target cluster", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.ReplicationConfig.Clusters = append(target.ReplicationConfig.Clusters, target.ReplicationConfig.Clusters[0])
		}, status: namespaceReplicationStatusBlocked},
		{name: "whitespace source cluster", mutate: func(source, _ *adminservice.GetNamespaceResponse) {
			source.ReplicationConfig.Clusters[1].ClusterName = " cluster-b "
		}, status: namespaceReplicationStatusBlocked},
		{name: "whitespace target cluster", mutate: func(_, target *adminservice.GetNamespaceResponse) {
			target.ReplicationConfig.Clusters[1].ClusterName = " cluster-b "
		}, status: namespaceReplicationStatusBlocked},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			target := proto.CloneOf(source)
			test.mutate(source, target)
			v := testNamespaceReplicationVerifier(source, map[string]*adminservice.GetNamespaceResponse{"cluster-b": target})
			result, err := v.Verify(t.Context(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			require.Equal(t, test.status, result.Status)
		})
	}
}

func TestNamespaceReplicationVerifierUnrelatedNotFoundBlocks(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	client := testNamespaceReplicationTargetClient("cluster-b", nil)
	client.getNamespaceFn = func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		return nil, status.Error(codes.NotFound, "unrelated resource not found")
	}
	v := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"cluster-b": client})
	result, err := v.Verify(t.Context(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Equal(t, namespaceReplicationPresenceUnavailable, result.Clusters[1].Presence)
}

func TestNamespaceReplicationVerifierIgnoresConfiguredNamespaceDataKeys(t *testing.T) {
	t.Parallel()
	const ignoredKey = "__MCNConfig"

	tests := []struct {
		name           string
		mutate         func(map[string]string)
		wantStatus     string
		wantDifference bool
	}{
		{
			name: "ignored key differs",
			mutate: func(data map[string]string) {
				data[ignoredKey] = "target-local-value"
			},
			wantStatus: namespaceReplicationStatusHealthy,
		},
		{
			name: "ordinary key differs",
			mutate: func(data map[string]string) {
				data["replicated-key"] = "different"
			},
			wantStatus:     namespaceReplicationStatusRepairRequired,
			wantDifference: true,
		},
		{
			name: "similarly named key differs",
			mutate: func(data map[string]string) {
				data[ignoredKey+"Extra"] = "different"
			},
			wantStatus:     namespaceReplicationStatusRepairRequired,
			wantDifference: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			source.Info.Data = map[string]string{
				ignoredKey:           "source-value",
				"replicated-key":     "value",
				ignoredKey + "Extra": "value",
			}
			target := proto.Clone(source).(*adminservice.GetNamespaceResponse)
			test.mutate(target.Info.Data)

			verifier := testNamespaceReplicationVerifier(source, map[string]*adminservice.GetNamespaceResponse{
				"cluster-b": target,
			})
			verifier.dataKeysToIgnore = map[string]struct{}{ignoredKey: {}}
			result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			require.Equal(t, test.wantStatus, result.Status)
			if test.wantDifference {
				require.Contains(t, result.Clusters[1].Differences, "info.data")
			} else {
				require.NotContains(t, result.Clusters[1].Differences, "info.data")
			}
			if test.wantStatus == namespaceReplicationStatusHealthy {
				require.Equal(t, result.SourceConfigFingerprint, result.Clusters[1].ConfigFingerprint)
			}
		})
	}
}

func TestNamespaceReplicationVerifierNameCollisionBlocksRepair(t *testing.T) {
	t.Parallel()
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
		"cluster-b": client,
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Equal(t, namespaceReplicationPresenceNameCollision, result.Clusters[1].Presence)
	require.NotContains(t, result.Clusters[1].Error, "other-id")
}

func TestNamespaceReplicationVerifierCrossChecksNamespaceName(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		expected   string
		inherited  bool
		wantStatus string
		wantDetail string
	}{
		{name: "not given", wantStatus: namespaceReplicationStatusHealthy},
		{name: "matches", expected: "namespace", wantStatus: namespaceReplicationStatusHealthy},
		{name: "differs", expected: "other", wantStatus: namespaceReplicationStatusBlocked, wantDetail: `namespace ID "namespace-id" belongs to namespace "namespace", not "other"`},
		{name: "inherited differs", expected: "other", inherited: true, wantStatus: namespaceReplicationStatusBlocked, wantDetail: `namespace ID "namespace-id" belongs to namespace "namespace", not "other" from tdbg --namespace or TEMPORAL_CLI_NAMESPACE`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			peer := testNamespaceReplicationTargetClient("cluster-b", proto.CloneOf(source))
			peerReads := 0
			getNamespace := peer.getNamespaceFn
			peer.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
				peerReads++
				return getNamespace(request)
			}
			verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
				"cluster-b": peer,
			})
			request := testNamespaceReplicationVerifyRequest()
			request.ExpectedNamespaceName = test.expected
			request.ExpectedNamespaceNameInherited = test.inherited

			result, err := verifier.Verify(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, test.wantStatus, result.Status)
			require.Equal(t, test.wantDetail, result.StatusDetail)
			require.Equal(t, "namespace", result.NamespaceName)
			if test.wantDetail != "" {
				// A mismatch stops before any peer is read.
				require.Zero(t, peerReads)
				require.Empty(t, result.Clusters)
			} else {
				require.NotZero(t, peerReads)
			}
		})
	}
}

func TestNamespaceReplicationVerifierResolvesNameOnce(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		inherited     bool
		initialErr    error
		mutateInitial func(*adminservice.GetNamespaceResponse)
		finalErr      error
		mutateFinal   func(*adminservice.GetNamespaceResponse)
		wantStatus    string
		wantError     string
	}{
		{name: "healthy", wantStatus: namespaceReplicationStatusHealthy},
		{name: "name lookup fails", initialErr: serviceerror.NewNamespaceNotFound("namespace"), wantStatus: namespaceReplicationStatusBlocked, wantError: "read source namespace: Namespace namespace is not found"},
		{name: "inherited name lookup fails", inherited: true, initialErr: serviceerror.NewNamespaceNotFound("namespace"), wantStatus: namespaceReplicationStatusBlocked, wantError: `read source namespace: namespace "namespace" from tdbg --namespace or TEMPORAL_CLI_NAMESPACE: Namespace namespace is not found`},
		{name: "name lookup returns another name", mutateInitial: func(r *adminservice.GetNamespaceResponse) { r.Info.Name = "other" }, wantStatus: namespaceReplicationStatusBlocked, wantError: "response has a different namespace name"},
		{name: "name lookup returns incomplete identity", mutateInitial: func(r *adminservice.GetNamespaceResponse) { r.Info.Name = "" }, wantStatus: namespaceReplicationStatusBlocked, wantError: "invalid source namespace: namespace response has incomplete identity"},
		{name: "source deleted during scan", finalErr: serviceerror.NewNamespaceNotFound("namespace"), wantStatus: namespaceReplicationStatusInconclusiveSourceChanged},
		{name: "stale ID mapping returns replacement", mutateFinal: func(r *adminservice.GetNamespaceResponse) { r.Info.Id = "replacement-id" }, wantStatus: namespaceReplicationStatusInconclusiveSourceChanged},
		{name: "source renamed during scan", mutateFinal: func(r *adminservice.GetNamespaceResponse) { r.Info.Name = "renamed" }, wantStatus: namespaceReplicationStatusInconclusiveSourceChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
			client := testNamespaceReplicationSourceClient(source)
			reads := 0
			client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
				reads++
				response := proto.CloneOf(source)
				if reads == 1 {
					require.Equal(t, "namespace", request.GetNamespace())
					require.Empty(t, request.GetId())
					if test.mutateInitial != nil {
						test.mutateInitial(response)
					}
					return response, test.initialErr
				}
				// A name can be reused during the scan; never resolve it again.
				require.Equal(t, 2, reads)
				require.Equal(t, "namespace-id", request.GetId())
				require.Empty(t, request.GetNamespace())
				if test.mutateFinal != nil {
					test.mutateFinal(response)
				}
				return response, test.finalErr
			}
			peerReads := 0
			client.remoteClusterNamespaceFn = func(request *adminservice.GetRemoteClusterNamespaceRequest) (*adminservice.GetRemoteClusterNamespaceResponse, error) {
				peerReads++
				if peerReads == 1 {
					require.Equal(t, "namespace-id", request.GetRemoteNamespaceId())
				} else {
					require.Equal(t, "namespace", request.GetRemoteNamespaceName())
				}
				return &adminservice.GetRemoteClusterNamespaceResponse{ClusterName: "cluster-b", Namespace: proto.CloneOf(source)}, nil
			}
			verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{"source": client})
			result, err := verifier.Verify(t.Context(), namespaceReplicationVerifyRequest{
				ExpectedNamespaceName:          "namespace",
				ExpectedNamespaceNameInherited: test.inherited,
			})
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.Zero(t, peerReads)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, reads)
				require.Equal(t, 2, peerReads)
				require.Equal(t, "namespace-id", result.NamespaceID)
			}
			require.Equal(t, test.wantStatus, result.Status)
		})
	}
}

func TestNamespaceReplicationVerifierIdentifiesReplicaByID(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	renamed := proto.CloneOf(source)
	renamed.Info.Name = "other-name"
	otherNamespace := testNamespaceResponse("namespace", "other-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	for _, test := range []struct {
		name            string
		byID, byName    *adminservice.GetNamespaceResponse
		wantPresence    string
		wantDifferences []string
		wantError       string
	}{
		{
			name:            "renamed replica",
			byID:            renamed,
			wantPresence:    namespaceReplicationPresencePresent,
			wantDifferences: []string{"info.name"},
			wantError:       "replica has a different namespace name",
		},
		{
			name:            "renamed replica and source name reused",
			byID:            renamed,
			byName:          otherNamespace,
			wantPresence:    namespaceReplicationPresencePresent,
			wantDifferences: []string{"info.name"},
			wantError:       "replica has a different namespace name; the source name is used by a different namespace ID",
		},
		{
			name:         "ID missing and name found with the same ID",
			byName:       source,
			wantPresence: namespaceReplicationPresenceUnavailable,
			wantError:    "namespace identity lookup was inconsistent between name and ID",
		},
		{
			name:         "ID found and name found with another ID",
			byID:         source,
			byName:       otherNamespace,
			wantPresence: namespaceReplicationPresenceUnavailable,
			wantError:    "namespace identity lookup was inconsistent between name and ID",
		},
		{
			name:         "ID found and name missing",
			byID:         source,
			wantPresence: namespaceReplicationPresenceUnavailable,
			wantError:    "namespace identity lookup was inconsistent between name and ID",
		},
		{
			name:         "renamed replica and name found with the same ID",
			byID:         renamed,
			byName:       source,
			wantPresence: namespaceReplicationPresenceUnavailable,
			wantError:    "namespace identity lookup was inconsistent between name and ID",
		},
		{
			name:         "ID lookup returns another namespace",
			byID:         otherNamespace,
			wantPresence: namespaceReplicationPresenceUnavailable,
			wantError:    "lookup namespace by ID: response has a different namespace ID",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := testNamespaceReplicationTargetClient("cluster-b", nil)
			client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
				response := test.byName
				if request.GetId() != "" {
					response = test.byID
				}
				if response == nil {
					return nil, serviceerror.NewNamespaceNotFound("namespace")
				}
				return proto.CloneOf(response), nil
			}
			verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
				"cluster-b": client,
			})

			result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
			target := result.Clusters[1]
			require.Equal(t, test.wantPresence, target.Presence)
			require.Equal(t, test.wantDifferences, target.Differences)
			require.Equal(t, test.wantError, target.Error)
			if test.wantPresence == namespaceReplicationPresencePresent {
				require.Equal(t, namespaceReplicationMatchMismatch, target.ConfigMatch)
				require.Equal(t, namespaceReplicationMatchMatch, target.FailoverMatch)
			}
		})
	}
}

func TestNamespaceReplicationVerifierPeerReadsThroughRelay(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	for _, test := range []struct {
		name       string
		relay      func(*adminservice.GetRemoteClusterNamespaceRequest) (*adminservice.GetRemoteClusterNamespaceResponse, error)
		peers      map[string]*testNamespaceReplicationAdminClient
		wantDetail string
		wantError  string
	}{
		{
			name: "source frontend without the relay",
			relay: func(*adminservice.GetRemoteClusterNamespaceRequest) (*adminservice.GetRemoteClusterNamespaceResponse, error) {
				return nil, status.Error(codes.Unimplemented, "unknown method GetRemoteClusterNamespace")
			},
			wantDetail: errNamespaceReplicationRelayUnsupported.Error(),
			wantError:  "lookup namespace by ID: " + errNamespaceReplicationRelayUnsupported.Error(),
		},
		{
			name: "relay reports another cluster",
			relay: func(*adminservice.GetRemoteClusterNamespaceRequest) (*adminservice.GetRemoteClusterNamespaceResponse, error) {
				return &adminservice.GetRemoteClusterNamespaceResponse{ClusterName: "cluster-c", Namespace: proto.CloneOf(source)}, nil
			},
			wantDetail: "replicas block safe repair: cluster-b (UNAVAILABLE)",
			wantError:  `lookup namespace by ID: cluster identity mismatch: expected "cluster-b", got "cluster-c"`,
		},
		{
			// Metadata for cluster-b reaches cluster-c, which lacks the namespace.
			// The relay checks identity first, so this must not look like MISSING.
			name:       "metadata routes to another cluster without the namespace",
			peers:      map[string]*testNamespaceReplicationAdminClient{"cluster-b": testNamespaceReplicationTargetClient("cluster-c", nil)},
			wantDetail: "replicas block safe repair: cluster-b (UNAVAILABLE)",
			wantError:  `remote cluster identity mismatch: expected "cluster-b", got "cluster-c"`,
		},
		{
			name:       "unknown peer",
			peers:      map[string]*testNamespaceReplicationAdminClient{},
			wantDetail: "replicas block safe repair: cluster-b (UNAVAILABLE)",
			wantError:  `unknown remote cluster "cluster-b"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sourceClient := testNamespaceReplicationSourceClient(source)
			sourceClient.remoteClusterNamespaceFn = test.relay
			clients := map[string]*testNamespaceReplicationAdminClient{"source": sourceClient}
			maps.Copy(clients, test.peers)
			verifier := testNamespaceReplicationVerifierWithClients(source, clients)

			result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
			require.NoError(t, err)
			require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
			require.Equal(t, test.wantDetail, result.StatusDetail)
			target := result.Clusters[1]
			require.Equal(t, namespaceReplicationPresenceUnavailable, target.Presence)
			require.Contains(t, target.Error, test.wantError)
		})
	}
}

func TestNamespaceReplicationVerifierRejectsSourceWithDifferentID(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		badRead       int
		wantErr       string
		wantStatus    string
		wantDetail    string
		wantNamespace bool
	}{
		{name: "first read", badRead: 1, wantErr: "read source namespace: response has a different namespace ID", wantStatus: namespaceReplicationStatusBlocked},
		{name: "final read", badRead: 2, wantStatus: namespaceReplicationStatusInconclusiveSourceChanged, wantDetail: "re-read source namespace after scanning replicas: response has a different namespace ID", wantNamespace: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
			other := testNamespaceResponse("namespace", "other-id", "cluster-a", []string{"cluster-a"}, 10, 20)
			client := testNamespaceReplicationSourceClient(source)
			reads := 0
			client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
				require.Equal(t, "namespace-id", request.GetId())
				reads++
				if reads == test.badRead {
					return proto.CloneOf(other), nil
				}
				return proto.CloneOf(source), nil
			}
			verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
				"source": client,
			})

			result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantStatus, result.Status)
			require.Equal(t, test.wantDetail, result.StatusDetail)
			require.Equal(t, test.wantNamespace, result.NamespaceID != "")
			require.NotContains(t, result.StatusDetail, "other-id")
		})
	}
}

func TestNamespaceReplicationVerifierWireNamespaceNotFoundRequiresRepair(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	client := testNamespaceReplicationTargetClient("cluster-b", nil)
	client.getNamespaceFn = func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
		return nil, serviceerror.ToStatus(serviceerror.NewNamespaceNotFound("namespace")).Err()
	}
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{
		"cluster-b": client,
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusRepairRequired, result.Status)
	require.Equal(t, namespaceReplicationPresenceMissing, result.Clusters[1].Presence)
}

func TestNamespaceReplicationVerifierUnavailableTargetBlocksRepair(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	verifier := testNamespaceReplicationVerifierWithClients(source, map[string]*testNamespaceReplicationAdminClient{})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusBlocked, result.Status)
	require.Equal(t, namespaceReplicationPresenceUnavailable, result.Clusters[1].Presence)
}

func TestNamespaceReplicationVerifierSourceMovementIsInconclusive(t *testing.T) {
	t.Parallel()
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
		"source":    sourceClient,
		"cluster-b": testNamespaceReplicationTargetClient("cluster-b", target),
	})

	result, err := verifier.Verify(context.Background(), testNamespaceReplicationVerifyRequest())
	require.NoError(t, err)
	require.Equal(t, namespaceReplicationStatusInconclusiveSourceChanged, result.Status)
}

func TestValidateSourceNamespace(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			response := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 1, 1)
			test.mutate(response)
			require.ErrorContains(t, validateSourceNamespace(response, "cluster-a"), test.contains)
		})
	}
}

func TestValidateExpectedClusters(t *testing.T) {
	t.Parallel()
	response := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-b", "cluster-a"}, 1, 1)
	clusters, err := validateExpectedClusters(response, "cluster-a")
	require.NoError(t, err)
	require.Equal(t, []string{"cluster-a", "cluster-b"}, clusters)

	response.ReplicationConfig.Clusters = append(
		response.ReplicationConfig.Clusters,
		&replicationpb.ClusterReplicationConfig{ClusterName: "cluster-b"},
	)
	_, err = validateExpectedClusters(response, "cluster-a")
	require.ErrorContains(t, err, "appears more than once")

	response.ReplicationConfig.Clusters = []*replicationpb.ClusterReplicationConfig{{ClusterName: "cluster-b"}}
	_, err = validateExpectedClusters(response, "cluster-a")
	require.ErrorContains(t, err, "exactly once")
}

func TestNamespaceReplicationProjectionsCoverReplicatedFields(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	tests := []struct {
		name           string
		difference     string
		configChange   bool
		failoverChange bool
		mutate         func(*adminservice.GetNamespaceResponse)
	}{
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

	sourceConfig, sourceFailover, err := namespaceReplicationProjections(source, nil)
	require.NoError(t, err)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			target := proto.Clone(source).(*adminservice.GetNamespaceResponse)
			test.mutate(target)
			targetConfig, targetFailover, err := namespaceReplicationProjections(target, nil)
			require.NoError(t, err)
			require.Equal(t, !test.configChange, proto.Equal(sourceConfig, targetConfig))
			require.Equal(t, !test.failoverChange, proto.Equal(sourceFailover, targetFailover))
			require.Contains(t, namespaceReplicationDifferences(
				sourceConfig,
				targetConfig,
				sourceFailover,
				targetFailover,
			), test.difference)
		})
	}

	versionOnly := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	versionOnly.ConfigVersion++
	versionOnly.FailoverVersion++
	versionConfig, versionFailover, err := namespaceReplicationProjections(versionOnly, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(sourceConfig, versionConfig))
	require.True(t, proto.Equal(sourceFailover, versionFailover))
	require.Empty(t, namespaceReplicationDifferences(
		sourceConfig,
		versionConfig,
		sourceFailover,
		versionFailover,
	))
}

func TestNamespaceReplicationProjectionsDiscardNestedUnknownFields(t *testing.T) {
	t.Parallel()
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	target := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	unknown := protowire.AppendTag(nil, 1000, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	target.GetConfig().GetBadBinaries().GetBinaries()["checksum"].ProtoReflect().SetUnknown(unknown)
	target.GetFailoverHistory()[0].ProtoReflect().SetUnknown(unknown)

	sourceConfig, sourceFailover, err := namespaceReplicationProjections(source, nil)
	require.NoError(t, err)
	targetConfig, targetFailover, err := namespaceReplicationProjections(target, nil)
	require.NoError(t, err)

	require.True(t, proto.Equal(sourceConfig, targetConfig))
	require.True(t, proto.Equal(sourceFailover, targetFailover))
	require.Empty(t, namespaceReplicationDifferences(
		sourceConfig,
		targetConfig,
		sourceFailover,
		targetFailover,
	))
	sourceConfigFingerprint, err := namespaceReplicationFingerprint(sourceConfig)
	require.NoError(t, err)
	targetConfigFingerprint, err := namespaceReplicationFingerprint(targetConfig)
	require.NoError(t, err)
	require.Equal(t, sourceConfigFingerprint, targetConfigFingerprint)
	sourceFailoverFingerprint, err := namespaceReplicationFingerprint(sourceFailover)
	require.NoError(t, err)
	targetFailoverFingerprint, err := namespaceReplicationFingerprint(targetFailover)
	require.NoError(t, err)
	require.Equal(t, sourceFailoverFingerprint, targetFailoverFingerprint)
}

func TestTDBGExitCode(t *testing.T) {
	t.Parallel()
	require.Equal(t, 1, tdbgExitCode(errors.New("ordinary")))
	require.Equal(t, 2, tdbgExitCode(cli.Exit("blocked", 2)))
	require.Equal(t, namespaceReplicationRepairRequiredExitCode, tdbgExitCode(cli.Exit(
		"repair required",
		namespaceReplicationRepairRequiredExitCode,
	)))
}

func TestNamespaceReplicationVerificationJSONRedactsNamespaceValues(t *testing.T) {
	t.Parallel()
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

// The CLI tests below run sequentially: running a urfave/cli app mutates tdbg's
// shared global flag definitions, so parallel apps would race.
func TestNamespaceReplicationVerifyRequiresSelector(t *testing.T) {
	app := NewCliApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}
	app.ExitErrHandler = func(*cli.Context, error) {}

	err := app.Run([]string{"tdbg", "namespace", "replication", "verify"})
	require.ErrorContains(t, err, "--namespace or --namespace-id is required")
	require.Equal(t, 2, tdbgExitCode(err))
}

func TestNamespaceReplicationVerifyNamespaceFlags(t *testing.T) {
	// urfave/cli mutates shared built-in flags when running an app.
	for _, test := range []struct {
		name       string
		global     []string
		local      []string
		wantError  string
		wantCalls  int
		wantOutput string
		byName     bool
	}{
		{name: "ID", local: []string{"--namespace-id", "namespace-id"}},
		{name: "ID with matching inherited name", global: []string{"-n", "namespace"}, local: []string{"--namespace-id", "namespace-id"}},
		{name: "ID with mismatched inherited name", global: []string{"-n", "other"}, local: []string{"--namespace-id", "namespace-id"}, wantError: "blocked", wantCalls: 1, wantOutput: `not "other" from tdbg --namespace or TEMPORAL_CLI_NAMESPACE` + "\n"},
		{name: "ID ignores blank inherited name", global: []string{"-n", " "}, local: []string{"--namespace-id", "namespace-id"}},
		{name: "ID with conflicting names", global: []string{"-n", "other"}, local: []string{"-n", "namespace", "--namespace-id", "namespace-id"}, wantError: `--namespace "namespace" conflicts with "other"`},
		{name: "ID with matching name", local: []string{"--namespace", "namespace", "--namespace-id", "namespace-id"}},
		{name: "ID with matching alias", local: []string{"-n", "namespace", "--namespace-id", "namespace-id"}},
		{name: "ID with blank name", local: []string{"--namespace", " ", "--namespace-id", "namespace-id"}, wantError: "--namespace is blank"},
		{name: "ID with mismatched name", local: []string{"--namespace", "other", "--namespace-id", "namespace-id"}, wantError: "blocked", wantCalls: 1, wantOutput: `not "other"` + "\n"},
		{name: "global name", global: []string{"--namespace", "namespace"}, byName: true},
		{name: "global alias", global: []string{"-n", "namespace"}, byName: true},
		{name: "local name", local: []string{"--namespace", "namespace"}, byName: true},
		{name: "local alias", local: []string{"-n", "namespace"}, byName: true},
		{name: "local name matches inherited name", global: []string{"-n", "namespace"}, local: []string{"-n", "namespace"}, byName: true},
		{name: "local name conflicts with inherited name", global: []string{"-n", "other"}, local: []string{"-n", "namespace"}, wantError: `--namespace "namespace" conflicts with "other"`},
		{name: "blank name", local: []string{"-n", " "}, wantError: "--namespace is blank"},
		{name: "blank inherited name is not a selector", global: []string{"-n", " "}, wantError: "--namespace or --namespace-id is required"},
		{name: "blank inherited name with local name", global: []string{"-n", " "}, local: []string{"-n", "namespace"}, byName: true},
		{name: "blank ID", local: []string{"--namespace-id", " "}, wantError: "--namespace-id is blank"},
		{name: "blank ID with name", local: []string{"--namespace-id", " ", "-n", "namespace"}, wantError: "--namespace-id is blank"},
		{name: "no implicit default", wantError: "--namespace or --namespace-id is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
			client := testNamespaceReplicationSourceClient(source)
			reads := 0
			client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
				if test.byName && reads == 0 {
					require.Equal(t, "namespace", request.GetNamespace())
					require.Empty(t, request.GetId())
				} else {
					require.Equal(t, "namespace-id", request.GetId())
					require.Empty(t, request.GetNamespace())
				}
				reads++
				return proto.CloneOf(source), nil
			}
			factory := &testNamespaceReplicationCLIClientFactory{
				clients: map[string]*testNamespaceReplicationAdminClient{"source": client},
			}
			app, output := testNamespaceReplicationCLIApp(factory)
			args := append([]string{"tdbg"}, test.global...)
			args = append(args, "namespace", "replication", "verify")
			args = append(args, test.local...)
			err := app.Run(args)
			require.Contains(t, output.String(), test.wantOutput)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.Equal(t, 2, tdbgExitCode(err))
				require.Equal(t, test.wantCalls, factory.adminClientCalls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, factory.adminClientCalls)
				require.Equal(t, 2, reads)
			}
		})
	}
}

func TestNamespaceReplicationVerifyNamespaceEnvironment(t *testing.T) {
	// The environment is process-wide, so this test cannot run in parallel.
	for _, test := range []struct {
		name       string
		args       []string
		env        string
		source     string
		byID       bool
		wantError  string
		wantCalls  int
		wantOutput string
	}{
		{name: "environment name", env: "environment", source: "environment", args: []string{"tdbg", "namespace", "replication", "verify"}},
		{name: "ID with matching environment", env: "namespace", source: "namespace", byID: true, args: []string{"tdbg", "namespace", "replication", "verify", "--namespace-id", "namespace-id"}},
		{name: "ID with mismatched environment", env: "other", source: "namespace", byID: true, args: []string{"tdbg", "namespace", "replication", "verify", "--namespace-id", "namespace-id"}, wantError: "blocked", wantCalls: 1, wantOutput: `not "other" from tdbg --namespace or TEMPORAL_CLI_NAMESPACE` + "\n"},
		{name: "local name matches environment", env: "namespace", source: "namespace", args: []string{"tdbg", "namespace", "replication", "verify", "-n", "namespace"}},
		{name: "local name conflicts with environment", env: "other", source: "namespace", args: []string{"tdbg", "namespace", "replication", "verify", "-n", "namespace"}, wantError: `--namespace "namespace" conflicts with "other"`},
		{name: "blank environment name is not a selector", env: " ", args: []string{"tdbg", "namespace", "replication", "verify"}, wantError: "--namespace or --namespace-id is required"},
		// urfave/cli counts an exported but empty variable as set.
		{name: "ID with empty environment", env: "", source: "namespace", byID: true, args: []string{"tdbg", "namespace", "replication", "verify", "--namespace-id", "namespace-id"}},
		{name: "local name with empty environment", env: "", source: "namespace", args: []string{"tdbg", "namespace", "replication", "verify", "-n", "namespace"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TEMPORAL_CLI_NAMESPACE", test.env)
			source := testNamespaceResponse(test.source, "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
			client := testNamespaceReplicationSourceClient(source)
			reads := 0
			client.getNamespaceFn = func(request *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
				if reads == 0 && !test.byID {
					require.Equal(t, test.source, request.GetNamespace())
				} else {
					require.Equal(t, "namespace-id", request.GetId())
				}
				reads++
				return proto.CloneOf(source), nil
			}
			factory := &testNamespaceReplicationCLIClientFactory{
				clients: map[string]*testNamespaceReplicationAdminClient{"source": client},
			}
			app, output := testNamespaceReplicationCLIApp(factory)
			err := app.Run(test.args)
			require.Contains(t, output.String(), test.wantOutput)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.Equal(t, 2, tdbgExitCode(err))
				require.Equal(t, test.wantCalls, factory.adminClientCalls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, factory.adminClientCalls)
			require.Equal(t, 2, reads)
		})
	}
}

func TestNamespaceReplicationVerifyCommandReadsPeersThroughSource(t *testing.T) {
	const ignoredKey = "__MCNConfig"
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a", "cluster-b"}, 10, 20)
	source.Info.Data[ignoredKey] = "source-value"
	target := proto.Clone(source).(*adminservice.GetNamespaceResponse)
	target.Info.Data[ignoredKey] = "target-local-value"
	factory := &testNamespaceReplicationCLIClientFactory{
		source: source,
		clients: map[string]*testNamespaceReplicationAdminClient{
			"cluster-b": testNamespaceReplicationTargetClient("cluster-b", target),
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory, ignoredKey)

	err := app.Run([]string{"tdbg", "namespace", "replication", "verify", "--namespace-id", "namespace-id", "--print-json"})
	require.NoError(t, err)
	// One admin client, for the local frontend; peers are read through it.
	require.Equal(t, 1, factory.adminClientCalls)
	var result namespaceReplicationVerificationResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, namespaceReplicationStatusHealthy, result.Status)
}

func TestNamespaceReplicationVerifyCommandPrintsBlockedResult(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
	source.ReplicationConfig.State = enumspb.REPLICATION_STATE_HANDOVER
	factory := &testNamespaceReplicationCLIClientFactory{
		clients: map[string]*testNamespaceReplicationAdminClient{
			"source": testNamespaceReplicationSourceClient(source),
		},
	}
	app, output := testNamespaceReplicationCLIApp(factory)

	err := app.Run([]string{
		"tdbg",
		"namespace",
		"replication",
		"verify",
		"--namespace-id", "namespace-id",
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
		"verify",
		"--namespace-id", "namespace-id",
	})
	require.Error(t, err)
	require.Equal(t, namespaceReplicationRepairRequiredExitCode, tdbgExitCode(err))
	require.Contains(t, output.String(), "REPAIR_REQUIRED")
	require.Contains(t, output.String(), "info.description")
}

func TestNamespaceReplicationVerifyCommandPropagatesJSONWriteFailure(t *testing.T) {
	source := testNamespaceResponse("namespace", "namespace-id", "cluster-a", []string{"cluster-a"}, 10, 20)
	factory := &testNamespaceReplicationCLIClientFactory{
		clients: map[string]*testNamespaceReplicationAdminClient{
			"source": testNamespaceReplicationSourceClient(source),
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
		"--namespace-id", "namespace-id",
		"--print-json",
	})
	require.ErrorContains(t, err, "write failed")
	require.Equal(t, 2, tdbgExitCode(err))
}

func testNamespaceReplicationCLIApp(
	factory ClientFactory,
	dataKeysToIgnore ...string,
) (*cli.App, *bytes.Buffer) {
	output := &bytes.Buffer{}
	app := NewCliApp(
		WithNamespaceReplicationDataKeysToIgnore(dataKeysToIgnore...),
		func(params *Params) {
			params.ClientFactory = factory
			params.Writer = output
			params.ErrWriter = &bytes.Buffer{}
		},
	)
	app.ExitErrHandler = func(*cli.Context, error) {}
	return app, output
}

func testNamespaceReplicationVerifier(
	source *adminservice.GetNamespaceResponse,
	targets map[string]*adminservice.GetNamespaceResponse,
) *namespaceReplicationVerifier {
	clients := map[string]*testNamespaceReplicationAdminClient{}
	for cluster, target := range targets {
		clients[cluster] = testNamespaceReplicationTargetClient(cluster, target)
	}
	return testNamespaceReplicationVerifierWithClients(source, clients)
}

// testNamespaceReplicationVerifierWithClients builds a verifier whose reader
// talks to clients["source"] (the local frontend), which relays peer reads to
// the other clients, keyed by cluster name.
func testNamespaceReplicationVerifierWithClients(
	source *adminservice.GetNamespaceResponse,
	clients map[string]*testNamespaceReplicationAdminClient,
) *namespaceReplicationVerifier {
	verifier := newNamespaceReplicationVerifier(adminNamespaceReplicationReader{
		client:     testNamespaceReplicationSourceWithPeers(source, clients),
		rpcTimeout: time.Second,
	}, namespaceReplicationOptions{})
	verifier.now = func() time.Time { return time.Unix(123, 0).UTC() }
	return verifier
}

func testNamespaceReplicationSourceWithPeers(
	source *adminservice.GetNamespaceResponse,
	clients map[string]*testNamespaceReplicationAdminClient,
) *testNamespaceReplicationAdminClient {
	sourceClient := clients["source"]
	if sourceClient == nil {
		sourceClient = testNamespaceReplicationSourceClient(source)
	}
	sourceClient.peers = make(map[string]*testNamespaceReplicationAdminClient)
	for cluster, client := range clients {
		if cluster != "source" {
			sourceClient.peers[cluster] = client
		}
	}
	return sourceClient
}

func testNamespaceReplicationVerifyRequest() namespaceReplicationVerifyRequest {
	return namespaceReplicationVerifyRequest{NamespaceID: "namespace-id"}
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
		ObservationTime:   timestamppb.New(time.Unix(102, 0)),
	}
}

type testNamespaceReplicationCLIClientFactory struct {
	ClientFactory
	source           *adminservice.GetNamespaceResponse
	clients          map[string]*testNamespaceReplicationAdminClient
	adminClientCalls int
}

func (f *testNamespaceReplicationCLIClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	f.adminClientCalls++
	return testNamespaceReplicationSourceWithPeers(f.source, f.clients)
}

type namespaceReplicationErrorWriter struct{}

func (namespaceReplicationErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

type testNamespaceReplicationAdminClient struct {
	adminservice.AdminServiceClient
	describeClusterFn                      func() (*adminservice.DescribeClusterResponse, error)
	getNamespaceFn                         func(*adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error)
	applyNamespaceMutationFn               func(*adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error)
	reconcileNamespaceMutationComponentsFn func(*adminservice.ReconcileNamespaceMutationComponentsRequest) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error)
	// remoteClusterNamespaceFn and remoteClusterRepairFn override the relays;
	// otherwise peers serve them.
	remoteClusterNamespaceFn func(*adminservice.GetRemoteClusterNamespaceRequest) (*adminservice.GetRemoteClusterNamespaceResponse, error)
	remoteClusterRepairFn    func(*adminservice.ApplyRemoteClusterNamespaceRepairRequest) (*adminservice.ApplyRemoteClusterNamespaceRepairResponse, error)
	peers                    map[string]*testNamespaceReplicationAdminClient
}

func (c *testNamespaceReplicationAdminClient) ApplyNamespaceMutation(
	_ context.Context,
	request *adminservice.ApplyNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*adminservice.ApplyNamespaceMutationResponse, error) {
	return c.applyNamespaceMutationFn(request)
}

func (c *testNamespaceReplicationAdminClient) ReconcileNamespaceMutationComponents(
	_ context.Context,
	request *adminservice.ReconcileNamespaceMutationComponentsRequest,
	_ ...grpc.CallOption,
) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
	return c.reconcileNamespaceMutationComponentsFn(request)
}

// relayPeer mirrors the server relays' routing: an unknown peer and a peer
// reporting another identity fail before the peer is read or written.
func (c *testNamespaceReplicationAdminClient) relayPeer(clusterName string) (*testNamespaceReplicationAdminClient, string, error) {
	peer := c.peers[clusterName]
	if peer == nil {
		return nil, "", serviceerror.NewFailedPreconditionf("unknown remote cluster %q", clusterName)
	}
	describe, err := peer.describeClusterFn()
	if err != nil {
		return nil, "", err
	}
	if describe.GetClusterName() != clusterName {
		return nil, "", serviceerror.NewFailedPreconditionf(
			"remote cluster identity mismatch: expected %q, got %q",
			clusterName,
			describe.GetClusterName(),
		)
	}
	return peer, describe.GetClusterName(), nil
}

// ApplyRemoteClusterNamespaceRepair mirrors the server write relay.
func (c *testNamespaceReplicationAdminClient) ApplyRemoteClusterNamespaceRepair(
	_ context.Context,
	request *adminservice.ApplyRemoteClusterNamespaceRepairRequest,
	_ ...grpc.CallOption,
) (*adminservice.ApplyRemoteClusterNamespaceRepairResponse, error) {
	if c.remoteClusterRepairFn != nil {
		return c.remoteClusterRepairFn(request)
	}
	peer, clusterName, err := c.relayPeer(request.GetClusterName())
	if err != nil {
		return nil, err
	}
	response, err := peer.applyNamespaceMutationFn(request.GetMutation())
	if err != nil {
		return nil, err
	}
	return &adminservice.ApplyRemoteClusterNamespaceRepairResponse{ClusterName: clusterName, Response: response}, nil
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

// GetRemoteClusterNamespace mirrors the server relay: it checks the peer's
// identity before reading the namespace, then passes the peer's response or
// error through.
func (c *testNamespaceReplicationAdminClient) GetRemoteClusterNamespace(
	_ context.Context,
	request *adminservice.GetRemoteClusterNamespaceRequest,
	_ ...grpc.CallOption,
) (*adminservice.GetRemoteClusterNamespaceResponse, error) {
	if c.remoteClusterNamespaceFn != nil {
		return c.remoteClusterNamespaceFn(request)
	}
	peer, clusterName, err := c.relayPeer(request.GetClusterName())
	if err != nil {
		return nil, err
	}
	namespaceRequest := &adminservice.GetNamespaceRequest{}
	if request.GetRemoteNamespaceId() != "" {
		namespaceRequest.Attributes = &adminservice.GetNamespaceRequest_Id{Id: request.GetRemoteNamespaceId()}
	} else {
		namespaceRequest.Attributes = &adminservice.GetNamespaceRequest_Namespace{Namespace: request.GetRemoteNamespaceName()}
	}
	response, err := peer.getNamespaceFn(namespaceRequest)
	if err != nil {
		return nil, err
	}
	return &adminservice.GetRemoteClusterNamespaceResponse{ClusterName: clusterName, Namespace: response}, nil
}
