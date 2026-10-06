package tdbg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const namespaceReplicationClusterListPageSize = 1000

const (
	namespaceReplicationStatusHealthy                   = "HEALTHY"
	namespaceReplicationStatusRepairRequired            = "REPAIR_REQUIRED"
	namespaceReplicationStatusBlocked                   = "BLOCKED"
	namespaceReplicationStatusInconclusiveSourceChanged = "INCONCLUSIVE_SOURCE_CHANGED"

	namespaceReplicationPresencePresent       = "PRESENT"
	namespaceReplicationPresenceMissing       = "MISSING"
	namespaceReplicationPresenceNameCollision = "NAME_COLLISION"
	namespaceReplicationPresenceIDCollision   = "ID_COLLISION"
	namespaceReplicationPresenceUnavailable   = "UNAVAILABLE"

	namespaceReplicationMatchMatch    = "MATCH"
	namespaceReplicationMatchMismatch = "MISMATCH"
	namespaceReplicationMatchUnknown  = "UNKNOWN"
)

type namespaceReplicationSelector struct {
	Name string
	ID   string
}

type namespaceReplicationVerifyRequest struct {
	SourceAddress    string
	Selector         namespaceReplicationSelector
	AddressOverrides map[string]string
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
	Error               string   `json:"error,omitempty"`
}

type namespaceReplicationAdminClientConnection struct {
	client adminservice.AdminServiceClient
	closer io.Closer
}

type namespaceReplicationAdminClientProvider interface {
	OpenSource(address string) (*namespaceReplicationAdminClientConnection, error)
	OpenTarget(address string) (*namespaceReplicationAdminClientConnection, error)
}

type cliNamespaceReplicationAdminClientProvider struct {
	cliContext *cli.Context
	factory    namespaceReplicationClientFactory
}

func (p cliNamespaceReplicationAdminClientProvider) OpenSource(
	address string,
) (*namespaceReplicationAdminClientConnection, error) {
	return p.open(address, p.cliContext.String(FlagTLSServerName))
}

func (p cliNamespaceReplicationAdminClientProvider) OpenTarget(
	address string,
) (*namespaceReplicationAdminClientConnection, error) {
	return p.open(address, "")
}

func (p cliNamespaceReplicationAdminClientProvider) open(
	address string,
	tlsServerName string,
) (*namespaceReplicationAdminClientConnection, error) {
	client, closer, err := p.factory.AdminClientForAddress(p.cliContext, address, tlsServerName)
	if err != nil {
		return nil, err
	}
	return &namespaceReplicationAdminClientConnection{client: client, closer: closer}, nil
}

type namespaceReplicationVerifier struct {
	clients          namespaceReplicationAdminClientProvider
	dataKeysToIgnore map[string]struct{}
	rpcTimeout       time.Duration
	now              func() time.Time
}

func newNamespaceReplicationVerifier(
	clients namespaceReplicationAdminClientProvider,
	options namespaceReplicationOptions,
) *namespaceReplicationVerifier {
	ignoredDataKeys := make(map[string]struct{}, len(options.dataKeysToIgnore))
	for _, key := range options.dataKeysToIgnore {
		ignoredDataKeys[key] = struct{}{}
	}
	return &namespaceReplicationVerifier{
		clients:          clients,
		dataKeysToIgnore: ignoredDataKeys,
		rpcTimeout:       defaultContextTimeout,
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
) (_ *namespaceReplicationVerificationResult, retErr error) {
	result := &namespaceReplicationVerificationResult{
		VerificationTime: v.now(),
		Status:           namespaceReplicationStatusBlocked,
	}

	sourceConnection, err := v.clients.OpenSource(request.SourceAddress)
	if err != nil {
		return result, fmt.Errorf("connect to source: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, sourceConnection.closer.Close())
	}()

	describeSource, err := namespaceReplicationRPCCall(ctx, v.rpcTimeout, func(rpcCtx context.Context) (*adminservice.DescribeClusterResponse, error) {
		return sourceConnection.client.DescribeCluster(rpcCtx, &adminservice.DescribeClusterRequest{})
	})
	if err != nil {
		return result, fmt.Errorf("describe source cluster: %w", err)
	}
	result.SourceCluster = describeSource.GetClusterName()
	if result.SourceCluster == "" {
		return result, errors.New("source DescribeCluster returned an empty cluster name")
	}

	source, err := getNamespace(ctx, v.rpcTimeout, sourceConnection.client, request.Selector)
	if err != nil {
		return result, fmt.Errorf("read source namespace: %w", err)
	}
	if err := validateNamespaceResponse(source); err != nil {
		return result, fmt.Errorf("invalid source namespace: %w", err)
	}
	result.NamespaceName = source.GetInfo().GetName()
	result.NamespaceID = source.GetInfo().GetId()

	configProjection, failoverProjection, err := namespaceReplicationProjections(source, v.dataKeysToIgnore)
	if err != nil {
		return result, fmt.Errorf("project source namespace: %w", err)
	}
	result.SourceConfigFingerprint, err = namespaceReplicationFingerprint(configProjection)
	if err != nil {
		return result, err
	}
	result.SourceFailoverFingerprint, err = namespaceReplicationFingerprint(failoverProjection)
	if err != nil {
		return result, err
	}
	sourceConfigVersion := source.GetConfigVersion()
	sourceFailoverVersion := source.GetFailoverVersion()
	result.Clusters = append(result.Clusters, namespaceReplicationClusterVerification{
		Cluster:             result.SourceCluster,
		Role:                "SOURCE",
		Presence:            namespaceReplicationPresencePresent,
		ConfigVersion:       &sourceConfigVersion,
		ConfigFingerprint:   result.SourceConfigFingerprint,
		ConfigMatch:         namespaceReplicationMatchMatch,
		FailoverVersion:     &sourceFailoverVersion,
		FailoverFingerprint: result.SourceFailoverFingerprint,
		FailoverMatch:       namespaceReplicationMatchMatch,
	})

	if err := validateSourceNamespace(source, result.SourceCluster); err != nil {
		return blockNamespaceReplicationVerification(result, err), nil
	}
	expectedClusters, err := validateExpectedClusters(source, result.SourceCluster)
	if err != nil {
		return blockNamespaceReplicationVerification(result, err), nil
	}
	result.ExpectedClusters = expectedClusters
	if err := validateNamespaceReplicationAddressOverrides(
		expectedClusters,
		result.SourceCluster,
		request.AddressOverrides,
	); err != nil {
		return blockNamespaceReplicationVerification(result, err), nil
	}

	targetsRequiringMetadata := namespaceReplicationTargetsRequiringMetadata(
		expectedClusters,
		result.SourceCluster,
		request.AddressOverrides,
	)
	clusterMetadata := make(map[string]*persistencespb.ClusterMetadata)
	if len(targetsRequiringMetadata) != 0 {
		clusterMetadata, err = listClusterMetadata(
			ctx,
			v.rpcTimeout,
			sourceConnection.client,
			targetsRequiringMetadata,
		)
		if err != nil {
			return blockNamespaceReplicationVerification(
				result,
				fmt.Errorf("list source cluster metadata: %w", err),
			), nil
		}
	}
	addresses, err := resolveClusterAddresses(
		expectedClusters,
		result.SourceCluster,
		request.SourceAddress,
		clusterMetadata,
		request.AddressOverrides,
	)
	if err != nil {
		return blockNamespaceReplicationVerification(result, err), nil
	}

	for _, cluster := range expectedClusters {
		if cluster == result.SourceCluster {
			continue
		}
		verification := v.inspectTarget(
			ctx,
			addresses[cluster],
			cluster,
			source,
			configProjection,
			failoverProjection,
		)
		result.Clusters = append(result.Clusters, verification)
	}

	finalSource, err := getNamespace(
		ctx,
		v.rpcTimeout,
		sourceConnection.client,
		namespaceReplicationSelector{ID: result.NamespaceID},
	)
	if err != nil {
		result.Status = namespaceReplicationStatusInconclusiveSourceChanged
		result.StatusDetail = fmt.Sprintf("final source read failed: %v", err)
		return result, nil
	}
	equal, err := namespaceReplicationSnapshotsEqual(source, finalSource, v.dataKeysToIgnore)
	if err != nil {
		return result, err
	}
	if !equal {
		result.Status = namespaceReplicationStatusInconclusiveSourceChanged
		result.StatusDetail = "source namespace changed while replicas were scanned"
		return result, nil
	}

	if err := deriveNamespaceReplicationStatus(result); err != nil {
		result.Status = namespaceReplicationStatusBlocked
		result.StatusDetail = err.Error()
	}
	return result, nil
}

func (v *namespaceReplicationVerifier) inspectTarget(
	ctx context.Context,
	address string,
	cluster string,
	source *adminservice.GetNamespaceResponse,
	sourceConfigProjection *replicationspb.NamespaceTaskAttributes,
	sourceFailoverProjection *replicationspb.NamespaceTaskAttributes,
) (result namespaceReplicationClusterVerification) {
	result = namespaceReplicationClusterVerification{
		Cluster:       cluster,
		Role:          "TARGET",
		Presence:      namespaceReplicationPresenceUnavailable,
		ConfigMatch:   namespaceReplicationMatchUnknown,
		FailoverMatch: namespaceReplicationMatchUnknown,
	}
	connection, err := v.clients.OpenTarget(address)
	if err != nil {
		result.Error = fmt.Sprintf("connect: %v", err)
		return result
	}
	defer func() {
		if err := connection.closer.Close(); err != nil && result.Error == "" {
			result.Presence = namespaceReplicationPresenceUnavailable
			result.ConfigMatch = namespaceReplicationMatchUnknown
			result.FailoverMatch = namespaceReplicationMatchUnknown
			result.Error = fmt.Sprintf("close connection: %v", err)
		}
	}()

	describe, err := namespaceReplicationRPCCall(ctx, v.rpcTimeout, func(rpcCtx context.Context) (*adminservice.DescribeClusterResponse, error) {
		return connection.client.DescribeCluster(rpcCtx, &adminservice.DescribeClusterRequest{})
	})
	if err != nil {
		result.Error = fmt.Sprintf("describe cluster: %v", err)
		return result
	}
	if describe.GetClusterName() != cluster {
		result.Error = fmt.Sprintf(
			"cluster identity mismatch: expected %q, got %q",
			cluster,
			describe.GetClusterName(),
		)
		return result
	}

	byName, nameErr := getNamespace(ctx, v.rpcTimeout, connection.client, namespaceReplicationSelector{
		Name: source.GetInfo().GetName(),
	})
	byID, idErr := getNamespace(ctx, v.rpcTimeout, connection.client, namespaceReplicationSelector{
		ID: source.GetInfo().GetId(),
	})
	nameMissing := isNamespaceNotFound(nameErr)
	idMissing := isNamespaceNotFound(idErr)
	if nameErr != nil && !nameMissing {
		result.Error = fmt.Sprintf("lookup namespace by name: %v", nameErr)
		return result
	}
	if idErr != nil && !idMissing {
		result.Error = fmt.Sprintf("lookup namespace by ID: %v", idErr)
		return result
	}
	if nameMissing && idMissing {
		result.Presence = namespaceReplicationPresenceMissing
		result.Error = ""
		return result
	}
	if !nameMissing && byName.GetInfo().GetId() != source.GetInfo().GetId() {
		result.Presence = namespaceReplicationPresenceNameCollision
		result.Error = "namespace name resolves to a different namespace ID"
		return result
	}
	if !idMissing && byID.GetInfo().GetName() != source.GetInfo().GetName() {
		result.Presence = namespaceReplicationPresenceIDCollision
		result.Error = "namespace ID resolves to a different namespace name"
		return result
	}
	if nameMissing || idMissing {
		result.Error = "namespace identity lookup was inconsistent between name and ID"
		return result
	}
	if !byID.GetIsGlobalNamespace() {
		configVersion := byID.GetConfigVersion()
		failoverVersion := byID.GetFailoverVersion()
		result.Presence = namespaceReplicationPresencePresent
		result.ConfigVersion = &configVersion
		result.FailoverVersion = &failoverVersion
		result.Differences = []string{"namespace.is_global"}
		result.Error = "target namespace is not global"
		return result
	}

	configProjection, failoverProjection, err := namespaceReplicationProjections(byID, v.dataKeysToIgnore)
	if err != nil {
		result.Error = fmt.Sprintf("project namespace: %v", err)
		return result
	}
	configFingerprint, err := namespaceReplicationFingerprint(configProjection)
	if err != nil {
		result.Error = fmt.Sprintf("fingerprint config: %v", err)
		return result
	}
	failoverFingerprint, err := namespaceReplicationFingerprint(failoverProjection)
	if err != nil {
		result.Error = fmt.Sprintf("fingerprint failover: %v", err)
		return result
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
	result.Error = ""
	return result
}

func getNamespace(
	ctx context.Context,
	rpcTimeout time.Duration,
	client adminservice.AdminServiceClient,
	selector namespaceReplicationSelector,
) (*adminservice.GetNamespaceResponse, error) {
	request := &adminservice.GetNamespaceRequest{}
	switch {
	case selector.Name != "":
		request.Attributes = &adminservice.GetNamespaceRequest_Namespace{Namespace: selector.Name}
	case selector.ID != "":
		request.Attributes = &adminservice.GetNamespaceRequest_Id{Id: selector.ID}
	default:
		return nil, errors.New("namespace name or ID is required")
	}
	return namespaceReplicationRPCCall(ctx, rpcTimeout, func(rpcCtx context.Context) (*adminservice.GetNamespaceResponse, error) {
		return client.GetNamespace(rpcCtx, request)
	})
}

func validateSourceNamespace(response *adminservice.GetNamespaceResponse, sourceCluster string) error {
	if err := validateNamespaceResponse(response); err != nil {
		return fmt.Errorf("invalid source namespace: %w", err)
	}
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
	if response.GetReplicationConfig().GetState() != enumspb.REPLICATION_STATE_NORMAL {
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
	counts := make(map[string]int)
	for _, cluster := range response.GetReplicationConfig().GetClusters() {
		name := strings.TrimSpace(cluster.GetClusterName())
		if name == "" {
			return nil, errors.New("source namespace contains an empty replication cluster name")
		}
		counts[name]++
	}
	if counts[sourceCluster] != 1 {
		return nil, fmt.Errorf(
			"source replication cluster list must contain %q exactly once; found %d",
			sourceCluster,
			counts[sourceCluster],
		)
	}
	for cluster, count := range counts {
		if count != 1 {
			return nil, fmt.Errorf("replication cluster %q appears %d times", cluster, count)
		}
	}
	clusters := slices.Sorted(maps.Keys(counts))
	return clusters, nil
}

func namespaceReplicationTargetsRequiringMetadata(
	expectedClusters []string,
	sourceCluster string,
	overrides map[string]string,
) []string {
	var targets []string
	for _, cluster := range expectedClusters {
		if cluster == sourceCluster {
			continue
		}
		if _, overridden := overrides[cluster]; !overridden {
			targets = append(targets, cluster)
		}
	}
	return targets
}

func listClusterMetadata(
	ctx context.Context,
	rpcTimeout time.Duration,
	client adminservice.AdminServiceClient,
	expectedTargets []string,
) (map[string]*persistencespb.ClusterMetadata, error) {
	expectedTargetSet := make(map[string]struct{}, len(expectedTargets))
	for _, cluster := range expectedTargets {
		expectedTargetSet[cluster] = struct{}{}
	}
	clusters := make(map[string]*persistencespb.ClusterMetadata)
	seenTokens := make(map[string]struct{})
	var token []byte
	for {
		response, err := namespaceReplicationRPCCall(ctx, rpcTimeout, func(rpcCtx context.Context) (*adminservice.ListClustersResponse, error) {
			return client.ListClusters(rpcCtx, &adminservice.ListClustersRequest{
				PageSize:      namespaceReplicationClusterListPageSize,
				NextPageToken: token,
			})
		})
		if err != nil {
			return nil, err
		}
		for _, cluster := range response.GetClusters() {
			name := cluster.GetClusterName()
			if _, expected := expectedTargetSet[name]; !expected {
				continue
			}
			if _, exists := clusters[name]; exists {
				return nil, fmt.Errorf("ListClusters returned duplicate metadata for cluster %q", name)
			}
			clusters[name] = cluster
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return clusters, nil
		}
		key := string(token)
		if _, exists := seenTokens[key]; exists {
			return nil, errors.New("ListClusters returned a repeated next-page token")
		}
		seenTokens[key] = struct{}{}
	}
}

func resolveClusterAddresses(
	expectedClusters []string,
	sourceCluster string,
	sourceAddress string,
	metadata map[string]*persistencespb.ClusterMetadata,
	overrides map[string]string,
) (map[string]string, error) {
	if err := validateNamespaceReplicationAddressOverrides(expectedClusters, sourceCluster, overrides); err != nil {
		return nil, err
	}

	addresses := map[string]string{sourceCluster: sourceAddress}
	for _, cluster := range expectedClusters {
		if cluster == sourceCluster {
			continue
		}
		if override, ok := overrides[cluster]; ok {
			addresses[cluster] = override
			continue
		}
		clusterMetadata := metadata[cluster]
		if clusterMetadata == nil {
			return nil, fmt.Errorf("no cluster metadata or address override for target %q", cluster)
		}
		if strings.TrimSpace(clusterMetadata.GetClusterAddress()) == "" {
			return nil, fmt.Errorf("cluster metadata for target %q has no frontend address", cluster)
		}
		addresses[cluster] = clusterMetadata.GetClusterAddress()
	}
	return addresses, nil
}

func validateNamespaceReplicationAddressOverrides(
	expectedClusters []string,
	sourceCluster string,
	overrides map[string]string,
) error {
	expected := make(map[string]struct{}, len(expectedClusters))
	for _, cluster := range expectedClusters {
		expected[cluster] = struct{}{}
	}
	for cluster, address := range overrides {
		if _, ok := expected[cluster]; !ok {
			return fmt.Errorf("address override refers to cluster %q outside the namespace replica set", cluster)
		}
		if cluster == sourceCluster {
			return errors.New("the source cluster address cannot be overridden with --cluster-address")
		}
		if strings.TrimSpace(address) == "" {
			return fmt.Errorf("address override for cluster %q is empty", cluster)
		}
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
	info := response.GetInfo()
	config := response.GetConfig()
	replicationConfig := response.GetReplicationConfig()

	var badBinaries *namespacepb.BadBinaries
	if len(config.GetBadBinaries().GetBinaries()) != 0 {
		badBinaries = proto.CloneOf(config.GetBadBinaries())
	}
	var retention *durationpb.Duration
	if config.GetWorkflowExecutionRetentionTtl() != nil {
		retention = proto.CloneOf(config.GetWorkflowExecutionRetentionTtl())
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
			BadBinaries:                  badBinaries,
			HistoryArchivalState:         config.GetHistoryArchivalState(),
			HistoryArchivalUri:           config.GetHistoryArchivalUri(),
			VisibilityArchivalState:      config.GetVisibilityArchivalState(),
			VisibilityArchivalUri:        config.GetVisibilityArchivalUri(),
			CustomSearchAttributeAliases: cloneNonEmptyStringMap(config.GetCustomSearchAttributeAliases()),
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: normalizedClusterReplicationConfigs(replicationConfig.GetClusters()),
		},
	}
	if retention != nil {
		configProjection.Config.WorkflowExecutionRetentionTtl = retention
	}

	failoverHistory := make([]*replicationpb.FailoverStatus, 0, len(response.GetFailoverHistory()))
	for _, status := range response.GetFailoverHistory() {
		failoverHistory = append(failoverHistory, proto.CloneOf(status))
	}
	if len(failoverHistory) == 0 {
		failoverHistory = nil
	}
	failoverProjection = &replicationspb.NamespaceTaskAttributes{
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: replicationConfig.GetActiveClusterName(),
			State:             replicationConfig.GetState(),
		},
		FailoverHistory: failoverHistory,
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
	names := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster.GetClusterName() != "" {
			names = append(names, cluster.GetClusterName())
		}
	}
	slices.Sort(names)
	names = slices.Compact(names)
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
	if expectedConfig.GetId() != actualConfig.GetId() {
		differences = append(differences, "namespace.id")
	}
	if expectedConfig.GetInfo().GetName() != actualConfig.GetInfo().GetName() {
		differences = append(differences, "namespace.name")
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

func deriveNamespaceReplicationStatus(result *namespaceReplicationVerificationResult) error {
	if len(result.Clusters) == 0 || result.Clusters[0].Role != "SOURCE" {
		return errors.New("verification result has no source cluster")
	}
	source := result.Clusters[0]
	if source.ConfigVersion == nil || source.FailoverVersion == nil {
		return errors.New("verification result has incomplete source versions")
	}

	repairRequired := false
	blocked := false
	for _, cluster := range result.Clusters[1:] {
		switch cluster.Presence {
		case namespaceReplicationPresenceMissing:
			repairRequired = true
		case namespaceReplicationPresencePresent:
			if cluster.ConfigVersion == nil || cluster.FailoverVersion == nil ||
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
	return nil
}

func namespaceReplicationFailoverBlocks(
	cluster *namespaceReplicationClusterVerification,
	sourceFailoverVersion int64,
) bool {
	return cluster.FailoverMatch != namespaceReplicationMatchMatch ||
		(cluster.FailoverVersion != nil && *cluster.FailoverVersion > sourceFailoverVersion)
}

func blockNamespaceReplicationVerification(
	result *namespaceReplicationVerificationResult,
	err error,
) *namespaceReplicationVerificationResult {
	result.Status = namespaceReplicationStatusBlocked
	result.StatusDetail = err.Error()
	return result
}

func isNamespaceNotFound(err error) bool {
	if err == nil {
		return false
	}
	var namespaceNotFound *serviceerror.NamespaceNotFound
	return errors.As(err, &namespaceNotFound) || grpcstatus.Code(err) == codes.NotFound
}
