package tdbg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const namespaceReplicationWarningInitialHistory = "target is missing the initial failover history entry; configuration repair will not change failover history"

const (
	namespaceReplicationRoleSource                      = "SOURCE"
	namespaceReplicationRoleTarget                      = "TARGET"
	namespaceReplicationStatusHealthy                   = "HEALTHY"
	namespaceReplicationStatusRepairRequired            = "REPAIR_REQUIRED"
	namespaceReplicationStatusBlocked                   = "BLOCKED"
	namespaceReplicationStatusInconclusiveSourceChanged = "INCONCLUSIVE_SOURCE_CHANGED"

	namespaceReplicationPresencePresent       = "PRESENT"
	namespaceReplicationPresenceMissing       = "MISSING"
	namespaceReplicationPresenceNameCollision = "NAME_COLLISION"
	namespaceReplicationPresenceUnavailable   = "UNAVAILABLE"

	namespaceReplicationMatchMatch    = "MATCH"
	namespaceReplicationMatchMismatch = "MISMATCH"
	namespaceReplicationMatchUnknown  = "UNKNOWN"
)

type namespaceReplicationVerifyRequest struct {
	// NamespaceID selects the namespace. Names can be reused, so they never
	// select the source.
	NamespaceID string
}

type namespaceReplicationVerificationResult struct {
	VerificationTime          time.Time                                 `json:"verification_time"`
	NamespaceName             string                                    `json:"namespace_name,omitempty"`
	NamespaceID               string                                    `json:"namespace_id,omitempty"`
	SourceCluster             string                                    `json:"source_cluster,omitempty"`
	ExpectedClusters          []string                                  `json:"expected_clusters,omitempty"`
	SourceConfigFingerprint   string                                    `json:"source_config_fingerprint,omitempty"`
	SourceFailoverFingerprint string                                    `json:"source_failover_fingerprint,omitempty"`
	Status                    string                                    `json:"status"`
	StatusDetail              string                                    `json:"status_detail,omitempty"`
	Clusters                  []namespaceReplicationClusterVerification `json:"clusters,omitempty"`
}

type namespaceReplicationClusterVerification struct {
	Cluster             string   `json:"cluster"`
	Role                string   `json:"role"`
	Presence            string   `json:"presence"`
	ConfigVersion       *int64   `json:"config_version,omitempty"`
	ConfigFingerprint   string   `json:"config_fingerprint,omitempty"`
	ConfigMatch         string   `json:"config_match"`
	FailoverVersion     *int64   `json:"failover_version,omitempty"`
	FailoverFingerprint string   `json:"failover_fingerprint,omitempty"`
	FailoverMatch       string   `json:"failover_match"`
	Differences         []string `json:"differences,omitempty"`
	Warnings            []string `json:"warnings,omitempty"`
	Error               string   `json:"error,omitempty"`

	initialHistoryMissing bool
	relayUnsupported      bool
}

// namespaceReplicationObservation is one verification pass: the reported result
// plus the raw snapshots it was derived from, for callers that act on them.
type namespaceReplicationObservation struct {
	result *namespaceReplicationVerificationResult
	// sourceSnapshot is the confirmed source state when result.Status was
	// derived from a source that did not change during the scan; otherwise it is
	// the first source read and must not be used as a baseline.
	sourceSnapshot  *adminservice.GetNamespaceResponse
	targetSnapshots map[string]*adminservice.GetNamespaceResponse
	// sourceStable is set only after the final source read matches the source
	// snapshot used to compare targets. Repair must not reconcile CHASM peers
	// from a partial observation whose authority may have moved mid-scan.
	sourceStable bool
}

// namespaceReplicationReader reads namespace state for verification. The source
// is the local frontend; peers are read through it with the
// GetRemoteClusterNamespace relay, so tdbg needs only the local frontend's
// address and TLS identity.
type namespaceReplicationReader interface {
	SourceCluster(ctx context.Context) (string, error)
	SourceNamespaceByID(ctx context.Context, id string) (*adminservice.GetNamespaceResponse, error)
	PeerNamespaceByID(ctx context.Context, cluster string, id string) (*adminservice.GetNamespaceResponse, error)
	PeerNamespaceByName(ctx context.Context, cluster string, name string) (*adminservice.GetNamespaceResponse, error)
}

var errNamespaceReplicationRelayUnsupported = errors.New(
	"the source frontend does not support GetRemoteClusterNamespace; upgrade it before verifying replicas",
)

type adminNamespaceReplicationReader struct {
	client     adminservice.AdminServiceClient
	rpcTimeout time.Duration
}

func (r adminNamespaceReplicationReader) SourceCluster(ctx context.Context) (string, error) {
	response, err := namespaceReplicationRPCCall(ctx, r.rpcTimeout, func(rpcCtx context.Context) (*adminservice.DescribeClusterResponse, error) {
		return r.client.DescribeCluster(rpcCtx, &adminservice.DescribeClusterRequest{})
	})
	if err != nil {
		return "", err
	}
	return response.GetClusterName(), nil
}

func (r adminNamespaceReplicationReader) SourceNamespaceByID(
	ctx context.Context,
	id string,
) (*adminservice.GetNamespaceResponse, error) {
	response, err := namespaceReplicationRPCCall(ctx, r.rpcTimeout, func(rpcCtx context.Context) (*adminservice.GetNamespaceResponse, error) {
		return r.client.GetNamespace(rpcCtx, &adminservice.GetNamespaceRequest{
			Attributes: &adminservice.GetNamespaceRequest_Id{Id: id},
		})
	})
	if err != nil {
		return nil, err
	}
	return requireNamespaceID(response, id)
}

func (r adminNamespaceReplicationReader) PeerNamespaceByID(
	ctx context.Context,
	cluster string,
	id string,
) (*adminservice.GetNamespaceResponse, error) {
	response, err := r.peerNamespace(ctx, &adminservice.GetRemoteClusterNamespaceRequest{
		ClusterName: cluster,
		Selector:    &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: id},
	})
	if err != nil {
		return nil, err
	}
	return requireNamespaceID(response, id)
}

func (r adminNamespaceReplicationReader) PeerNamespaceByName(
	ctx context.Context,
	cluster string,
	name string,
) (*adminservice.GetNamespaceResponse, error) {
	return r.peerNamespace(ctx, &adminservice.GetRemoteClusterNamespaceRequest{
		ClusterName: cluster,
		Selector:    &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceName{RemoteNamespaceName: name},
	})
}

// peerNamespace relays the read through the source frontend. The server checks
// the peer's identity before reading; this also checks the reported name on
// successful reads.
func (r adminNamespaceReplicationReader) peerNamespace(
	ctx context.Context,
	request *adminservice.GetRemoteClusterNamespaceRequest,
) (*adminservice.GetNamespaceResponse, error) {
	response, err := namespaceReplicationRPCCall(ctx, r.rpcTimeout, func(rpcCtx context.Context) (*adminservice.GetRemoteClusterNamespaceResponse, error) {
		return r.client.GetRemoteClusterNamespace(rpcCtx, request)
	})
	if err != nil {
		if grpcstatus.Code(err) == codes.Unimplemented {
			return nil, fmt.Errorf("%w: %v", errNamespaceReplicationRelayUnsupported, err)
		}
		return nil, err
	}
	if response.GetClusterName() != request.GetClusterName() {
		return nil, fmt.Errorf(
			"cluster identity mismatch: expected %q, got %q",
			request.GetClusterName(),
			response.GetClusterName(),
		)
	}
	return response.GetNamespace(), nil
}

// requireNamespaceID rejects a response for a different namespace. Some stores
// resolve an ID through an ID-to-name mapping and read the record by name, so a
// stale mapping left by an interrupted delete can return a namespace that
// later reused the name. Incomplete responses are rejected later with a more
// specific error.
func requireNamespaceID(
	response *adminservice.GetNamespaceResponse,
	id string,
) (*adminservice.GetNamespaceResponse, error) {
	if responseID := response.GetInfo().GetId(); responseID != "" && responseID != id {
		return nil, errors.New("response has a different namespace ID")
	}
	return response, nil
}

type namespaceReplicationVerifier struct {
	reader           namespaceReplicationReader
	dataKeysToIgnore map[string]struct{}
	now              func() time.Time
}

func newNamespaceReplicationVerifier(
	reader namespaceReplicationReader,
	options namespaceReplicationOptions,
) *namespaceReplicationVerifier {
	ignoredDataKeys := make(map[string]struct{}, len(options.dataKeysToIgnore))
	for _, key := range options.dataKeysToIgnore {
		ignoredDataKeys[key] = struct{}{}
	}
	return &namespaceReplicationVerifier{
		reader:           reader,
		dataKeysToIgnore: ignoredDataKeys,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func namespaceReplicationRPCCall[T any](
	ctx context.Context,
	timeout time.Duration,
	call func(context.Context) (T, error),
) (T, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return call(rpcCtx)
}

func (v *namespaceReplicationVerifier) Verify(
	ctx context.Context,
	request namespaceReplicationVerifyRequest,
) (*namespaceReplicationVerificationResult, error) {
	observation, err := v.observe(ctx, request)
	if observation == nil {
		return nil, err
	}
	return observation.result, err
}

// observe reads the source namespace, compares every current replica against
// it, then reads the source again. Replica reads are not atomic with the
// source read, so the status is derived only when the source held still for
// the whole scan. The result starts BLOCKED, so any early return fails closed.
// A returned error means the observation could not be completed; the
// observation may still carry a partial result for printing.
func (v *namespaceReplicationVerifier) observe(
	ctx context.Context,
	request namespaceReplicationVerifyRequest,
) (*namespaceReplicationObservation, error) {
	result := &namespaceReplicationVerificationResult{
		VerificationTime: v.now(),
		Status:           namespaceReplicationStatusBlocked,
	}
	observation := &namespaceReplicationObservation{
		result:          result,
		targetSnapshots: make(map[string]*adminservice.GetNamespaceResponse),
	}

	sourceCluster, err := v.reader.SourceCluster(ctx)
	if err != nil {
		return observation, fmt.Errorf("describe source cluster: %w", err)
	}
	result.SourceCluster = sourceCluster
	if result.SourceCluster == "" {
		return observation, errors.New("source DescribeCluster returned an empty cluster name")
	}

	source, err := v.reader.SourceNamespaceByID(ctx, request.NamespaceID)
	if err != nil {
		return observation, fmt.Errorf("read source namespace: %w", err)
	}
	if err := validateNamespaceResponse(source); err != nil {
		return observation, fmt.Errorf("invalid source namespace: %w", err)
	}
	observation.sourceSnapshot = source
	result.NamespaceName = source.GetInfo().GetName()
	result.NamespaceID = source.GetInfo().GetId()

	configProjection, failoverProjection, err := namespaceReplicationProjections(source, v.dataKeysToIgnore)
	if err != nil {
		blockNamespaceReplicationVerification(result, fmt.Errorf("project source namespace: %w", err))
		return observation, nil
	}
	result.SourceConfigFingerprint, err = namespaceReplicationFingerprint(configProjection)
	if err != nil {
		return observation, err
	}
	result.SourceFailoverFingerprint, err = namespaceReplicationFingerprint(failoverProjection)
	if err != nil {
		return observation, err
	}
	sourceConfigVersion := source.GetConfigVersion()
	sourceFailoverVersion := source.GetFailoverVersion()
	result.Clusters = append(result.Clusters, namespaceReplicationClusterVerification{
		Cluster:             result.SourceCluster,
		Role:                namespaceReplicationRoleSource,
		Presence:            namespaceReplicationPresencePresent,
		ConfigVersion:       &sourceConfigVersion,
		ConfigFingerprint:   result.SourceConfigFingerprint,
		ConfigMatch:         namespaceReplicationMatchMatch,
		FailoverVersion:     &sourceFailoverVersion,
		FailoverFingerprint: result.SourceFailoverFingerprint,
		FailoverMatch:       namespaceReplicationMatchMatch,
	})

	if err := validateSourceNamespace(source, result.SourceCluster); err != nil {
		blockNamespaceReplicationVerification(result, err)
		return observation, nil
	}
	expectedClusters, err := validateExpectedClusters(source, result.SourceCluster)
	if err != nil {
		blockNamespaceReplicationVerification(result, err)
		return observation, nil
	}
	result.ExpectedClusters = expectedClusters
	for _, cluster := range expectedClusters {
		if cluster == result.SourceCluster {
			continue
		}
		verification, target := v.inspectTarget(
			ctx,
			cluster,
			source,
			configProjection,
			failoverProjection,
		)
		observation.targetSnapshots[cluster] = target
		result.Clusters = append(result.Clusters, verification)
		if verification.relayUnsupported {
			// Every peer read would fail the same way; say what to fix instead
			// of reporting each replica as unavailable.
			blockNamespaceReplicationVerification(result, errNamespaceReplicationRelayUnsupported)
			return observation, nil
		}
	}

	// Targets were read after the source, possibly seconds later. If the source
	// changed meanwhile, a target that already applied the change would look
	// stale and one that had not could look current, so neither HEALTHY nor
	// REPAIR_REQUIRED would be trustworthy. Re-read the source to confirm the
	// comparison baseline held for the whole scan.
	finalSource, err := v.reader.SourceNamespaceByID(ctx, result.NamespaceID)
	if err != nil {
		result.Status = namespaceReplicationStatusInconclusiveSourceChanged
		result.StatusDetail = fmt.Sprintf("final source read failed: %v", err)
		return observation, nil
	}
	equal, err := namespaceReplicationSnapshotsEqual(source, finalSource, v.dataKeysToIgnore)
	if err != nil {
		return observation, err
	}
	if !equal {
		result.Status = namespaceReplicationStatusInconclusiveSourceChanged
		result.StatusDetail = "source namespace changed while replicas were scanned"
		return observation, nil
	}
	// The source is unchanged across the scan, so this snapshot is the baseline
	// that callers such as repair may act on.
	observation.sourceSnapshot = finalSource
	observation.sourceStable = true

	deriveNamespaceReplicationStatus(result)
	return observation, nil
}

func (v *namespaceReplicationVerifier) inspectTarget(
	ctx context.Context,
	cluster string,
	source *adminservice.GetNamespaceResponse,
	sourceConfigProjection *replicationspb.NamespaceTaskAttributes,
	sourceFailoverProjection *replicationspb.NamespaceTaskAttributes,
) (result namespaceReplicationClusterVerification, namespace *adminservice.GetNamespaceResponse) {
	result = namespaceReplicationClusterVerification{
		Cluster:       cluster,
		Role:          namespaceReplicationRoleTarget,
		Presence:      namespaceReplicationPresenceUnavailable,
		ConfigMatch:   namespaceReplicationMatchUnknown,
		FailoverMatch: namespaceReplicationMatchUnknown,
	}
	identity := v.lookupReplica(ctx, cluster, source)
	if identity.replica == nil {
		if identity.presence != "" {
			result.Presence = identity.presence
		}
		result.Error = identity.err
		result.relayUnsupported = errors.Is(identity.cause, errNamespaceReplicationRelayUnsupported)
		return result, nil
	}
	byID := identity.replica
	renamed := identity.renamed
	if !byID.GetIsGlobalNamespace() {
		configVersion := byID.GetConfigVersion()
		failoverVersion := byID.GetFailoverVersion()
		result.Presence = namespaceReplicationPresencePresent
		result.ConfigVersion = &configVersion
		result.FailoverVersion = &failoverVersion
		result.Differences = []string{"namespace.is_global"}
		if renamed {
			result.Differences = append([]string{"info.name"}, result.Differences...)
		}
		result.Error = "target namespace is not global"
		return result, byID
	}

	configProjection, failoverProjection, err := namespaceReplicationProjections(byID, v.dataKeysToIgnore)
	if err != nil {
		result.Error = fmt.Sprintf("project namespace: %v", err)
		return result, nil
	}
	configFingerprint, err := namespaceReplicationFingerprint(configProjection)
	if err != nil {
		result.Error = fmt.Sprintf("fingerprint config: %v", err)
		return result, nil
	}
	failoverFingerprint, err := namespaceReplicationFingerprint(failoverProjection)
	if err != nil {
		result.Error = fmt.Sprintf("fingerprint failover: %v", err)
		return result, nil
	}

	configVersion := byID.GetConfigVersion()
	failoverVersion := byID.GetFailoverVersion()
	result.Presence = namespaceReplicationPresencePresent
	result.ConfigVersion = &configVersion
	result.ConfigFingerprint = configFingerprint
	result.FailoverVersion = &failoverVersion
	result.FailoverFingerprint = failoverFingerprint
	result.ConfigMatch = namespaceReplicationMatchMismatch
	if proto.Equal(sourceConfigProjection, configProjection) {
		result.ConfigMatch = namespaceReplicationMatchMatch
	}
	result.FailoverMatch = namespaceReplicationMatchMismatch
	if proto.Equal(sourceFailoverProjection, failoverProjection) {
		result.FailoverMatch = namespaceReplicationMatchMatch
	}
	result.Differences = namespaceReplicationDifferences(
		sourceConfigProjection,
		configProjection,
		sourceFailoverProjection,
		failoverProjection,
	)
	// A topology update can initialize history at the source without advancing the
	// failover version. Existing peers only accept history with a newer failover.
	result.initialHistoryMissing = namespaceReplicationInitialHistoryMissing(source, byID, sourceFailoverProjection, failoverProjection)
	if result.initialHistoryMissing {
		result.Warnings = []string{namespaceReplicationWarningInitialHistory}
	}
	result.Error = ""
	if renamed {
		// Replication applies updates by name, so it cannot rename this replica.
		result.Error = "replica has a different namespace name"
		if identity.nameHeldByOther {
			result.Error += "; the source name is used by a different namespace ID"
		}
	}
	return result, byID
}

type namespaceReplicaIdentity struct {
	// replica is nil when the target has no namespace with the source ID or
	// the lookups cannot identify one.
	replica         *adminservice.GetNamespaceResponse
	presence        string
	err             string
	cause           error
	renamed         bool
	nameHeldByOther bool
}

// lookupReplica identifies the target replica by namespace ID. Names are unique
// per cluster but can be reused, so the name lookup only detects another
// namespace holding the source name.
func (v *namespaceReplicationVerifier) lookupReplica(
	ctx context.Context,
	cluster string,
	source *adminservice.GetNamespaceResponse,
) namespaceReplicaIdentity {
	byID, idErr := v.reader.PeerNamespaceByID(ctx, cluster, source.GetInfo().GetId())
	if idErr != nil && !isNamespaceNotFound(idErr) {
		return namespaceReplicaIdentity{err: fmt.Sprintf("lookup namespace by ID: %v", idErr), cause: idErr}
	}
	byName, nameErr := v.reader.PeerNamespaceByName(ctx, cluster, source.GetInfo().GetName())
	idMissing := isNamespaceNotFound(idErr)
	nameMissing := isNamespaceNotFound(nameErr)
	if nameErr != nil && !nameMissing {
		return namespaceReplicaIdentity{err: fmt.Sprintf("lookup namespace by name: %v", nameErr), cause: nameErr}
	}
	nameHeldByOther := !nameMissing && byName.GetInfo().GetId() != source.GetInfo().GetId()
	const inconsistent = "namespace identity lookup was inconsistent between name and ID"
	if idMissing {
		switch {
		case nameHeldByOther:
			return namespaceReplicaIdentity{
				presence: namespaceReplicationPresenceNameCollision,
				err:      "namespace name is used by a different namespace ID",
			}
		case nameMissing:
			return namespaceReplicaIdentity{presence: namespaceReplicationPresenceMissing}
		default:
			return namespaceReplicaIdentity{err: inconsistent}
		}
	}
	renamed := byID.GetInfo().GetName() != source.GetInfo().GetName()
	// The two reads must agree: an unrenamed replica must also resolve by name,
	// and a renamed one must not. Anything else means a rename or delete landed
	// between the reads, or the store's ID and name mappings disagree.
	nameResolvesToReplica := !nameMissing && !nameHeldByOther
	if renamed == nameResolvesToReplica {
		return namespaceReplicaIdentity{err: inconsistent}
	}
	return namespaceReplicaIdentity{replica: byID, renamed: renamed, nameHeldByOther: nameHeldByOther}
}

func namespaceReplicationInitialHistoryMissing(
	source, target *adminservice.GetNamespaceResponse,
	sourceFailover, targetFailover *replicationspb.NamespaceTaskAttributes,
) bool {
	return source.GetFailoverVersion() == target.GetFailoverVersion() &&
		len(source.GetFailoverHistory()) == 1 && len(target.GetFailoverHistory()) == 0 &&
		source.GetFailoverHistory()[0].GetFailoverVersion() == source.GetFailoverVersion() &&
		proto.Equal(sourceFailover.GetReplicationConfig(), targetFailover.GetReplicationConfig())
}

func validateSourceNamespace(response *adminservice.GetNamespaceResponse, sourceCluster string) error {
	if !response.GetIsGlobalNamespace() {
		return errors.New("source namespace is not global")
	}
	if response.GetReplicationConfig().GetActiveClusterName() != sourceCluster {
		return fmt.Errorf(
			"selected source cluster %q is not active; active cluster is %q",
			sourceCluster,
			response.GetReplicationConfig().GetActiveClusterName(),
		)
	}
	state := response.GetReplicationConfig().GetState()
	if state != enumspb.REPLICATION_STATE_NORMAL && state != enumspb.REPLICATION_STATE_UNSPECIFIED {
		return fmt.Errorf(
			"source namespace replication state is %s, not NORMAL",
			response.GetReplicationConfig().GetState(),
		)
	}
	switch response.GetInfo().GetState() {
	case enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED:
		return nil
	default:
		return fmt.Errorf(
			"source namespace state is %s; only REGISTERED or DEPRECATED can be repaired",
			response.GetInfo().GetState(),
		)
	}
}

func validateNamespaceResponse(response *adminservice.GetNamespaceResponse) error {
	if response == nil {
		return errors.New("empty namespace response")
	}
	if response.GetInfo() == nil || response.GetInfo().GetId() == "" || response.GetInfo().GetName() == "" {
		return errors.New("namespace response has incomplete identity")
	}
	if response.GetConfig() == nil {
		return errors.New("namespace response has no config")
	}
	if response.GetReplicationConfig() == nil {
		return errors.New("namespace response has no replication config")
	}
	return nil
}

func validateExpectedClusters(
	response *adminservice.GetNamespaceResponse,
	sourceCluster string,
) ([]string, error) {
	if err := validateReplicationClusterNames(response.GetReplicationConfig().GetClusters()); err != nil {
		return nil, err
	}
	clusters := clusterNames(response.GetReplicationConfig().GetClusters())
	if !slices.Contains(clusters, sourceCluster) {
		return nil, fmt.Errorf("source replication cluster list must contain %q exactly once", sourceCluster)
	}
	return clusters, nil
}

func validateReplicationClusterNames(clusters []*replicationpb.ClusterReplicationConfig) error {
	seen := make(map[string]struct{}, len(clusters))
	for _, cluster := range clusters {
		name := cluster.GetClusterName()
		if name == "" || name != strings.TrimSpace(name) {
			return fmt.Errorf("invalid replication cluster name %q: names must be nonempty without surrounding whitespace", name)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("replication cluster %q appears more than once", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func namespaceReplicationProjections(
	response *adminservice.GetNamespaceResponse,
	dataKeysToIgnore map[string]struct{},
) (configProjection *replicationspb.NamespaceTaskAttributes, failoverProjection *replicationspb.NamespaceTaskAttributes, err error) {
	if err := validateNamespaceResponse(response); err != nil {
		return nil, nil, err
	}
	if err := validateReplicationClusterNames(response.GetReplicationConfig().GetClusters()); err != nil {
		return nil, nil, err
	}
	info := response.GetInfo()
	config := response.GetConfig()
	replicationConfig := response.GetReplicationConfig()

	var badBinaries *namespacepb.BadBinaries
	if len(config.GetBadBinaries().GetBinaries()) != 0 {
		badBinaries = proto.CloneOf(config.GetBadBinaries())
	}
	configProjection = &replicationspb.NamespaceTaskAttributes{
		Id: info.GetId(),
		Info: &namespacepb.NamespaceInfo{
			Name:        info.GetName(),
			State:       info.GetState(),
			Description: info.GetDescription(),
			OwnerEmail:  info.GetOwnerEmail(),
			Data:        cloneNamespaceDataExcluding(info.GetData(), dataKeysToIgnore),
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: proto.CloneOf(config.GetWorkflowExecutionRetentionTtl()),
			BadBinaries:                   badBinaries,
			HistoryArchivalState:          config.GetHistoryArchivalState(),
			HistoryArchivalUri:            config.GetHistoryArchivalUri(),
			VisibilityArchivalState:       config.GetVisibilityArchivalState(),
			VisibilityArchivalUri:         config.GetVisibilityArchivalUri(),
			CustomSearchAttributeAliases:  cloneNonEmptyStringMap(config.GetCustomSearchAttributeAliases()),
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: normalizedClusterReplicationConfigs(replicationConfig.GetClusters()),
		},
	}
	var failoverHistory []*replicationpb.FailoverStatus
	for _, status := range response.GetFailoverHistory() {
		failoverHistory = append(failoverHistory, proto.CloneOf(status))
	}
	failoverProjection = &replicationspb.NamespaceTaskAttributes{
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: replicationConfig.GetActiveClusterName(),
			State:             replicationConfig.GetState(),
		},
		FailoverHistory: failoverHistory,
	}
	if failoverProjection.ReplicationConfig.State == enumspb.REPLICATION_STATE_UNSPECIFIED {
		failoverProjection.ReplicationConfig.State = enumspb.REPLICATION_STATE_NORMAL
	}
	if err := common.DiscardUnknownProto(configProjection); err != nil {
		return nil, nil, fmt.Errorf("discard unknown config projection fields: %w", err)
	}
	if err := common.DiscardUnknownProto(failoverProjection); err != nil {
		return nil, nil, fmt.Errorf("discard unknown failover projection fields: %w", err)
	}
	return configProjection, failoverProjection, nil
}

func cloneNonEmptyStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	return maps.Clone(input)
}

func cloneNamespaceDataExcluding(
	input map[string]string,
	keysToIgnore map[string]struct{},
) map[string]string {
	if len(input) == 0 {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		if _, ignored := keysToIgnore[key]; !ignored {
			result[key] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func normalizedClusterReplicationConfigs(
	clusters []*replicationpb.ClusterReplicationConfig,
) []*replicationpb.ClusterReplicationConfig {
	names := clusterNames(clusters)
	if len(names) == 0 {
		return nil
	}
	result := make([]*replicationpb.ClusterReplicationConfig, 0, len(names))
	for _, name := range names {
		result = append(result, &replicationpb.ClusterReplicationConfig{ClusterName: name})
	}
	return result
}

func namespaceReplicationFingerprint(message proto.Message) (string, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return "", fmt.Errorf("marshal normalized namespace state: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func namespaceReplicationDifferences(
	expectedConfig *replicationspb.NamespaceTaskAttributes,
	actualConfig *replicationspb.NamespaceTaskAttributes,
	expectedFailover *replicationspb.NamespaceTaskAttributes,
	actualFailover *replicationspb.NamespaceTaskAttributes,
) []string {
	var differences []string
	if expectedConfig.GetInfo().GetName() != actualConfig.GetInfo().GetName() {
		differences = append(differences, "info.name")
	}
	if expectedConfig.GetInfo().GetState() != actualConfig.GetInfo().GetState() {
		differences = append(differences, "info.state")
	}
	if expectedConfig.GetInfo().GetDescription() != actualConfig.GetInfo().GetDescription() {
		differences = append(differences, "info.description")
	}
	if expectedConfig.GetInfo().GetOwnerEmail() != actualConfig.GetInfo().GetOwnerEmail() {
		differences = append(differences, "info.owner_email")
	}
	if !maps.Equal(expectedConfig.GetInfo().GetData(), actualConfig.GetInfo().GetData()) {
		differences = append(differences, "info.data")
	}
	if !proto.Equal(expectedConfig.GetConfig().GetWorkflowExecutionRetentionTtl(), actualConfig.GetConfig().GetWorkflowExecutionRetentionTtl()) {
		differences = append(differences, "config.workflow_execution_retention_ttl")
	}
	if !proto.Equal(expectedConfig.GetConfig().GetBadBinaries(), actualConfig.GetConfig().GetBadBinaries()) {
		differences = append(differences, "config.bad_binaries")
	}
	if expectedConfig.GetConfig().GetHistoryArchivalState() != actualConfig.GetConfig().GetHistoryArchivalState() {
		differences = append(differences, "config.history_archival_state")
	}
	if expectedConfig.GetConfig().GetHistoryArchivalUri() != actualConfig.GetConfig().GetHistoryArchivalUri() {
		differences = append(differences, "config.history_archival_uri")
	}
	if expectedConfig.GetConfig().GetVisibilityArchivalState() != actualConfig.GetConfig().GetVisibilityArchivalState() {
		differences = append(differences, "config.visibility_archival_state")
	}
	if expectedConfig.GetConfig().GetVisibilityArchivalUri() != actualConfig.GetConfig().GetVisibilityArchivalUri() {
		differences = append(differences, "config.visibility_archival_uri")
	}
	if !maps.Equal(expectedConfig.GetConfig().GetCustomSearchAttributeAliases(), actualConfig.GetConfig().GetCustomSearchAttributeAliases()) {
		differences = append(differences, "config.custom_search_attribute_aliases")
	}
	if !slices.Equal(
		clusterNames(expectedConfig.GetReplicationConfig().GetClusters()),
		clusterNames(actualConfig.GetReplicationConfig().GetClusters()),
	) {
		differences = append(differences, "replication.clusters")
	}
	if expectedFailover.GetReplicationConfig().GetActiveClusterName() != actualFailover.GetReplicationConfig().GetActiveClusterName() {
		differences = append(differences, "replication.active_cluster")
	}
	if expectedFailover.GetReplicationConfig().GetState() != actualFailover.GetReplicationConfig().GetState() {
		differences = append(differences, "replication.state")
	}
	if !slices.EqualFunc(
		expectedFailover.GetFailoverHistory(),
		actualFailover.GetFailoverHistory(),
		func(expected, actual *replicationpb.FailoverStatus) bool {
			return proto.Equal(expected, actual)
		},
	) {
		differences = append(differences, "replication.failover_history")
	}
	return differences
}

func clusterNames(clusters []*replicationpb.ClusterReplicationConfig) []string {
	result := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster.GetClusterName() != "" {
			result = append(result, cluster.GetClusterName())
		}
	}
	slices.Sort(result)
	return slices.Compact(result)
}

// namespaceReplicationSnapshotsEqual reports whether two reads of the same
// namespace agree on everything verification compares: replicated config and
// failover state, plus the versions and global flag. Cluster-local fields and
// ignored data keys do not count. Every replicated update advances the config
// or failover version, so a change that was reverted between the reads is
// still detected.
func namespaceReplicationSnapshotsEqual(
	first *adminservice.GetNamespaceResponse,
	second *adminservice.GetNamespaceResponse,
	dataKeysToIgnore map[string]struct{},
) (bool, error) {
	firstConfig, firstFailover, err := namespaceReplicationProjections(first, dataKeysToIgnore)
	if err != nil {
		return false, err
	}
	secondConfig, secondFailover, err := namespaceReplicationProjections(second, dataKeysToIgnore)
	if err != nil {
		return false, err
	}
	return first.GetIsGlobalNamespace() == second.GetIsGlobalNamespace() &&
		first.GetConfigVersion() == second.GetConfigVersion() &&
		first.GetFailoverVersion() == second.GetFailoverVersion() &&
		proto.Equal(firstConfig, secondConfig) &&
		proto.Equal(firstFailover, secondFailover), nil
}

func deriveNamespaceReplicationStatus(result *namespaceReplicationVerificationResult) {
	source := result.Clusters[0]

	repairRequired := false
	blocked := false
	for _, cluster := range result.Clusters[1:] {
		switch cluster.Presence {
		case namespaceReplicationPresenceMissing:
			repairRequired = true
		case namespaceReplicationPresencePresent:
			if cluster.Error != "" || cluster.ConfigVersion == nil || cluster.FailoverVersion == nil ||
				namespaceReplicationFailoverBlocks(&cluster, *source.FailoverVersion) {
				blocked = true
				continue
			}
			if cluster.ConfigMatch != namespaceReplicationMatchMatch ||
				*cluster.ConfigVersion > *source.ConfigVersion {
				repairRequired = true
			}
		default:
			blocked = true
		}
	}

	if blocked {
		result.Status = namespaceReplicationStatusBlocked
		result.StatusDetail = "one or more replicas block safe repair"
	} else if repairRequired {
		result.Status = namespaceReplicationStatusRepairRequired
	} else {
		result.Status = namespaceReplicationStatusHealthy
	}
}

func namespaceReplicationFailoverBlocks(
	cluster *namespaceReplicationClusterVerification,
	sourceFailoverVersion int64,
) bool {
	return (cluster.FailoverMatch != namespaceReplicationMatchMatch &&
		!cluster.initialHistoryMissing) ||
		(cluster.FailoverVersion != nil && *cluster.FailoverVersion > sourceFailoverVersion)
}

func blockNamespaceReplicationVerification(
	result *namespaceReplicationVerificationResult,
	err error,
) {
	result.Status = namespaceReplicationStatusBlocked
	result.StatusDetail = err.Error()
}

func isNamespaceNotFound(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
		return true
	}
	_, ok := serviceerror.FromStatus(grpcstatus.Convert(err)).(*serviceerror.NamespaceNotFound)
	return ok
}
