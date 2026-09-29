package frontend

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"

	"go.opentelemetry.io/otel/log"
	"go.temporal.io/api/serviceerror"
	versionpb "go.temporal.io/api/version/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
)

const (
	remoteClusterLifecycleNotApplicable = "N/A"

	remoteClusterAPIAdmin    = "admin"
	remoteClusterAPIOperator = "operator"

	remoteClusterOutcomeSucceeded = "succeeded"
	remoteClusterOutcomeFailed    = "failed"

	remoteClusterMutationCreated = "created"
	remoteClusterMutationUpdated = "updated"
	remoteClusterMutationRemoved = "removed"
	remoteClusterMutationUnknown = "unknown"

	remoteClusterTransitionEnabled             = "enabled"
	remoteClusterTransitionDisabled            = "disabled"
	remoteClusterTransitionUnchanged           = "unchanged"
	remoteClusterTransitionInitializedEnabled  = "initialized_enabled"
	remoteClusterTransitionInitializedDisabled = "initialized_disabled"
)

type remoteClusterLifecycleEvent struct {
	logger        log.Logger
	phase         string
	details       map[string]any
	upsertRequest remoteClusterUpsertRequestFields
	removeRequest remoteClusterRemoveRequestFields
}

func newRemoteClusterUpsertLifecycleEvent(
	ctx context.Context,
	logger log.Logger,
	clusterMetadata cluster.Metadata,
	config *Config,
	api string,
	request remoteClusterUpsertRequestFields,
) *remoteClusterLifecycleEvent {
	event := newRemoteClusterLifecycleEvent(
		ctx,
		logger,
		clusterMetadata,
		config,
		api,
		wideevents.PhaseRemoteClusterUpsert,
		request,
	)
	if event != nil {
		event.upsertRequest = request
	}
	return event
}

func newRemoteClusterRemoveLifecycleEvent(
	ctx context.Context,
	logger log.Logger,
	clusterMetadata cluster.Metadata,
	config *Config,
	api string,
	request remoteClusterRemoveRequestFields,
) *remoteClusterLifecycleEvent {
	event := newRemoteClusterLifecycleEvent(
		ctx,
		logger,
		clusterMetadata,
		config,
		api,
		wideevents.PhaseRemoteClusterRemove,
		request,
	)
	if event != nil {
		event.removeRequest = request
	}
	return event
}

type remoteClusterUpsertRequestFields struct {
	FrontendAddress               string `json:"frontend_address"`
	FrontendHTTPAddress           string `json:"frontend_http_address"`
	EnableRemoteClusterConnection bool   `json:"enable_remote_cluster_connection"`
	EnableReplication             bool   `json:"enable_replication"`
}

type remoteClusterRemoveRequestFields struct {
	ClusterName string `json:"cluster_name"`
}

type cachedRemoteClusterLookup struct {
	information     cluster.ClusterInformation
	lookupPerformed bool
	found           bool
}

type remoteClusterMetadataFields struct {
	ClusterName              string                                           `json:"cluster_name"`
	HistoryShardCount        int32                                            `json:"history_shard_count"`
	ClusterID                string                                           `json:"cluster_id"`
	VersionInfo              *versionpb.VersionInfo                           `json:"version_info"`
	IndexSearchAttributes    map[string]*persistencespb.IndexSearchAttributes `json:"index_search_attributes"`
	ClusterAddress           string                                           `json:"cluster_address"`
	HTTPAddress              string                                           `json:"http_address"`
	FailoverVersionIncrement int64                                            `json:"failover_version_increment"`
	InitialFailoverVersion   int64                                            `json:"initial_failover_version"`
	IsGlobalNamespaceEnabled bool                                             `json:"is_global_namespace_enabled"`
	IsConnectionEnabled      bool                                             `json:"is_connection_enabled"`
	UseClusterIDMembership   bool                                             `json:"use_cluster_id_membership"`
	Tags                     map[string]string                                `json:"tags"`
	IsReplicationEnabled     bool                                             `json:"is_replication_enabled"`
}

type persistedRemoteClusterFields struct {
	*remoteClusterMetadataFields
	Version int64 `json:"version"`
}

type cachedRemoteClusterFields struct {
	ClusterName            string            `json:"cluster_name"`
	ClusterID              string            `json:"cluster_id"`
	RPCAddress             string            `json:"rpc_address"`
	HTTPAddress            string            `json:"http_address"`
	IsConnectionEnabled    bool              `json:"is_connection_enabled"`
	IsReplicationEnabled   bool              `json:"is_replication_enabled"`
	InitialFailoverVersion int64             `json:"initial_failover_version"`
	ShardCount             int32             `json:"shard_count"`
	Tags                   map[string]string `json:"tags"`
}

type saveClusterMetadataFields struct {
	ClusterMetadata *remoteClusterMetadataFields `json:"cluster_metadata"`
	Version         int64                        `json:"version"`
}

type deleteClusterMetadataFields struct {
	ClusterName string `json:"cluster_name"`
}

type remoteClusterRequestFingerprintFields struct {
	LocalCluster string `json:"local_cluster"`
	API          string `json:"api"`
	Phase        string `json:"phase"`
	Request      any    `json:"request"`
}

func newRemoteClusterLifecycleEvent(
	ctx context.Context,
	logger log.Logger,
	clusterMetadata cluster.Metadata,
	config *Config,
	api string,
	phase string,
	request any,
) *remoteClusterLifecycleEvent {
	if !remoteClusterLifecycleEventsEnabled(config) {
		return nil
	}
	localCluster := clusterMetadata.GetCurrentClusterName()
	details := map[string]any{
		"api":                 api,
		"local_cluster":       localCluster,
		"mutation":            remoteClusterMutationUnknown,
		"request":             request,
		"request_fingerprint": remoteClusterRequestFingerprint(localCluster, api, phase, request),
	}
	callerInfo := headers.GetCallerInfo(ctx)
	setStringDetailIfNotEmpty(details, "caller_type", callerInfo.CallerType)
	setStringDetailIfNotEmpty(details, "call_origin", callerInfo.CallOrigin)
	if claims, ok := ctx.Value(authorization.MappedClaims).(*authorization.Claims); ok {
		setStringDetailIfNotEmpty(details, "auth_subject", claims.Subject)
		setStringDetailIfNotEmpty(details, "auth_type", claims.AuthType)
	}
	return &remoteClusterLifecycleEvent{
		logger:  logger,
		phase:   phase,
		details: details,
	}
}

func remoteClusterLifecycleEventsEnabled(config *Config) bool {
	// Remote cluster lifecycle events intentionally share the namespace lifecycle gate.
	return config != nil &&
		config.EmitNamespaceLifecycleEvents != nil &&
		config.EmitNamespaceLifecycleEvents()
}

func (e *remoteClusterLifecycleEvent) emitUpsertSuccess(
	persistedBefore *persistence.GetClusterMetadataResponse,
	saveRequest *persistence.SaveClusterMetadataRequest,
) {
	if e == nil {
		return
	}
	if saveRequest != nil {
		e.setRemoteCluster(saveRequest.GetClusterName(), saveRequest.GetClusterId())
	}
	e.populateUpsertPersistence(persistedBefore, saveRequest)
	e.emit(nil)
}

func (e *remoteClusterLifecycleEvent) emitUpsertFailure(
	err error,
	remoteResponse *adminservice.DescribeClusterResponse,
	persistedBefore *persistence.GetClusterMetadataResponse,
	saveRequest *persistence.SaveClusterMetadataRequest,
) {
	if e == nil || err == nil {
		return
	}
	if remoteResponse != nil {
		e.setRemoteCluster(remoteResponse.GetClusterName(), remoteResponse.GetClusterId())
	} else if saveRequest != nil {
		e.setRemoteCluster(saveRequest.GetClusterName(), saveRequest.GetClusterId())
	}
	e.populateUpsertPersistence(persistedBefore, saveRequest)
	e.emit(err)
}

func (e *remoteClusterLifecycleEvent) populateUpsertPersistence(
	persistedBefore *persistence.GetClusterMetadataResponse,
	saveRequest *persistence.SaveClusterMetadataRequest,
) {
	// A nil persistedBefore only means the mutation is a create once the save request exists.
	// If both are nil, the handler failed before determining the persistence mutation.
	persistenceMutationKnown := persistedBefore != nil || saveRequest != nil
	if !persistenceMutationKnown {
		return
	}
	if persistedBefore == nil {
		e.setUpsertCreated()
	} else {
		e.setUpsertUpdated(persistedBefore)
	}
	if saveRequest != nil {
		e.setSaveRequest(saveRequest)
	}
}

func (e *remoteClusterLifecycleEvent) emitRemoveSuccess(
	cachedBefore cachedRemoteClusterLookup,
	deleteRequest *persistence.DeleteClusterMetadataRequest,
) {
	if e == nil {
		return
	}
	e.populateRemove(cachedBefore, deleteRequest)
	e.setRemoved()
	e.emit(nil)
}

func (e *remoteClusterLifecycleEvent) emitRemoveFailure(
	err error,
	cachedBefore cachedRemoteClusterLookup,
	deleteRequest *persistence.DeleteClusterMetadataRequest,
) {
	if e == nil || err == nil {
		return
	}
	e.populateRemove(cachedBefore, deleteRequest)
	e.emit(err)
}

func (e *remoteClusterLifecycleEvent) populateRemove(
	cachedBefore cachedRemoteClusterLookup,
	deleteRequest *persistence.DeleteClusterMetadataRequest,
) {
	e.setRemoteCluster(e.removeRequest.ClusterName, "")
	if cachedBefore.lookupPerformed {
		e.setCachedBefore(e.removeRequest.ClusterName, cachedBefore.information, cachedBefore.found)
	}
	if deleteRequest != nil {
		e.setDeleteRequest(deleteRequest)
	}
}

func lookupCachedRemoteCluster(
	clusterMetadata cluster.Metadata,
	clusterName string,
) cachedRemoteClusterLookup {
	information, found := clusterMetadata.GetAllClusterInfo()[clusterName]
	return cachedRemoteClusterLookup{
		information:     information,
		lookupPerformed: true,
		found:           found,
	}
}

func (e *remoteClusterLifecycleEvent) emit(err error) {
	if err == nil {
		e.details["outcome"] = remoteClusterOutcomeSucceeded
	} else {
		e.details["outcome"] = remoteClusterOutcomeFailed
		e.details["error_code"] = serviceerror.ToStatus(err).Code().String()
		e.details["error_type"] = fmt.Sprintf("%T", err)
		e.details["error"] = err.Error()
	}
	wideevents.Emit(e.logger, wideevents.RemoteClusterLifecyclePayload{
		Phase:       e.phase,
		Namespace:   remoteClusterLifecycleNotApplicable,
		NamespaceID: remoteClusterLifecycleNotApplicable,
		Details:     e.details,
	})
}

func (e *remoteClusterLifecycleEvent) setRemoteCluster(clusterName string, clusterID string) {
	setStringDetailIfNotEmpty(e.details, "remote_cluster", clusterName)
	setStringDetailIfNotEmpty(e.details, "remote_cluster_id", clusterID)
}

func (e *remoteClusterLifecycleEvent) setUpsertCreated() {
	e.details["persisted_before"] = nil
	e.details["mutation"] = remoteClusterMutationCreated
	e.details["requested_connection_transition"] = transitionForCreate(
		e.upsertRequest.EnableRemoteClusterConnection,
	)
	e.details["requested_replication_transition"] = transitionForCreate(
		e.upsertRequest.EnableReplication,
	)
}

func (e *remoteClusterLifecycleEvent) setUpsertUpdated(
	persistedBefore *persistence.GetClusterMetadataResponse,
) {
	e.details["persisted_before"] = persistedRemoteClusterFields{
		remoteClusterMetadataFields: clusterMetadataEventFields(persistedBefore.ClusterMetadata),
		Version:                     persistedBefore.Version,
	}
	e.details["mutation"] = remoteClusterMutationUpdated
	e.details["requested_connection_transition"] = transitionForUpdate(
		persistedBefore.GetIsConnectionEnabled(),
		e.upsertRequest.EnableRemoteClusterConnection,
	)
	e.details["requested_replication_transition"] = transitionForUpdate(
		persistedBefore.GetIsReplicationEnabled(),
		e.upsertRequest.EnableReplication,
	)
}

func (e *remoteClusterLifecycleEvent) setSaveRequest(request *persistence.SaveClusterMetadataRequest) {
	e.details["persistence_request"] = saveClusterMetadataFields{
		ClusterMetadata: clusterMetadataEventFields(request.ClusterMetadata),
		Version:         request.Version,
	}
}

func (e *remoteClusterLifecycleEvent) setCachedBefore(
	clusterName string,
	info cluster.ClusterInformation,
	found bool,
) {
	e.setRemoteCluster(clusterName, info.ClusterID)
	if !found {
		e.details["cached_before"] = nil
		return
	}
	e.details["cached_before"] = cachedRemoteClusterFields{
		ClusterName:            clusterName,
		ClusterID:              info.ClusterID,
		RPCAddress:             info.RPCAddress,
		HTTPAddress:            info.HTTPAddress,
		IsConnectionEnabled:    info.Enabled,
		IsReplicationEnabled:   info.ReplicationEnabled,
		InitialFailoverVersion: info.InitialFailoverVersion,
		ShardCount:             info.ShardCount,
		Tags:                   maps.Clone(info.Tags),
	}
}

func (e *remoteClusterLifecycleEvent) setDeleteRequest(request *persistence.DeleteClusterMetadataRequest) {
	e.details["persistence_request"] = deleteClusterMetadataFields{ClusterName: request.ClusterName}
}

func (e *remoteClusterLifecycleEvent) setRemoved() {
	e.details["mutation"] = remoteClusterMutationRemoved
}

func remoteClusterRequestFingerprint(localCluster string, api string, phase string, request any) string {
	encoded, err := json.Marshal(remoteClusterRequestFingerprintFields{
		LocalCluster: localCluster,
		API:          api,
		Phase:        phase,
		Request:      request,
	})
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

func clusterMetadataEventFields(
	metadata *persistencespb.ClusterMetadata,
) *remoteClusterMetadataFields {
	if metadata == nil {
		return &remoteClusterMetadataFields{}
	}
	return &remoteClusterMetadataFields{
		ClusterName:              metadata.GetClusterName(),
		HistoryShardCount:        metadata.GetHistoryShardCount(),
		ClusterID:                metadata.GetClusterId(),
		VersionInfo:              metadata.GetVersionInfo(),
		IndexSearchAttributes:    maps.Clone(metadata.GetIndexSearchAttributes()),
		ClusterAddress:           metadata.GetClusterAddress(),
		HTTPAddress:              metadata.GetHttpAddress(),
		FailoverVersionIncrement: metadata.GetFailoverVersionIncrement(),
		InitialFailoverVersion:   metadata.GetInitialFailoverVersion(),
		IsGlobalNamespaceEnabled: metadata.GetIsGlobalNamespaceEnabled(),
		IsConnectionEnabled:      metadata.GetIsConnectionEnabled(),
		UseClusterIDMembership:   metadata.GetUseClusterIdMembership(),
		Tags:                     maps.Clone(metadata.GetTags()),
		IsReplicationEnabled:     metadata.GetIsReplicationEnabled(),
	}
}

func transitionForCreate(enabled bool) string {
	if enabled {
		return remoteClusterTransitionInitializedEnabled
	}
	return remoteClusterTransitionInitializedDisabled
}

func transitionForUpdate(before bool, requested bool) string {
	if before == requested {
		return remoteClusterTransitionUnchanged
	}
	if requested {
		return remoteClusterTransitionEnabled
	}
	return remoteClusterTransitionDisabled
}

func setStringDetailIfNotEmpty(details map[string]any, key string, value string) {
	if value != "" {
		details[key] = value
	}
}
