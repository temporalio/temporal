//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination namespace_handler_mock.go

package frontend

import (
	"context"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsmanager"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// namespaceHandler implements the namespace-related APIs specified by the [workflowservice] package,
	// such as registering, updating, and querying namespaces.
	namespaceHandler struct {
		logger                 log.Logger
		metadataMgr            persistence.MetadataManager
		clusterMetadata        cluster.Metadata
		namespaceReplicator    nsreplication.Replicator
		namespaceAttrValidator *nsmanager.Validator
		archivalMetadata       archiver.ArchivalMetadata
		archiverProvider       provider.ArchiverProvider
		timeSource             clock.TimeSource
		config                 *Config
	}
)

const (
	maxReplicationHistorySize = 10
)

var (
	// err indicating that this cluster is not the master, so cannot do namespace registration or update
	errNotMasterCluster                   = serviceerror.NewInvalidArgument("Cluster is not master cluster, cannot do namespace registration or namespace update.")
	errCannotDoNamespaceFailoverAndUpdate = serviceerror.NewInvalidArgument("Cannot set active cluster to current cluster when other parameters are set.")
	errInvalidRetentionPeriod             = serviceerror.NewInvalidArgument("A valid retention period is not set on request.")
	errInvalidNamespaceStateUpdate        = serviceerror.NewInvalidArgument("Invalid namespace state update.")

	errCustomSearchAttributeFieldAlreadyAllocated = serviceerror.NewInvalidArgument("Custom search attribute field name already allocated.")
)

// newNamespaceHandler create a new namespace handler
func newNamespaceHandler(
	logger log.Logger,
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	namespaceReplicator nsreplication.Replicator,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
	timeSource clock.TimeSource,
	config *Config,
) *namespaceHandler {
	return &namespaceHandler{
		logger:                 logger,
		metadataMgr:            metadataMgr,
		clusterMetadata:        clusterMetadata,
		namespaceReplicator:    namespaceReplicator,
		namespaceAttrValidator: nsmanager.NewValidator(clusterMetadata),
		archivalMetadata:       archivalMetadata,
		archiverProvider:       archiverProvider,
		timeSource:             timeSource,
		config:                 config,
	}
}

// RegisterNamespace register a new namespace
//
//nolint:revive // cognitive complexity grandfathered
func (d *namespaceHandler) RegisterNamespace(
	ctx context.Context,
	registerRequest *workflowservice.RegisterNamespaceRequest,
) (*workflowservice.RegisterNamespaceResponse, error) {

	if !d.clusterMetadata.IsGlobalNamespaceEnabled() {
		if registerRequest.GetIsGlobalNamespace() {
			return nil, serviceerror.NewInvalidArgument("Cannot register global namespace when not enabled")
		}

		registerRequest.SetIsGlobalNamespace(false)
	} else {
		// cluster global namespace enabled
		if !d.clusterMetadata.IsMasterCluster() && registerRequest.GetIsGlobalNamespace() {
			return nil, errNotMasterCluster
		}
	}

	if err := d.validateRetentionDuration(
		registerRequest.GetWorkflowExecutionRetentionPeriod(),
		registerRequest.GetIsGlobalNamespace(),
	); err != nil {
		return nil, err
	}

	// first check if the name is already registered as the local namespace
	_, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: registerRequest.GetNamespace()})
	switch err.(type) {
	case nil:
		// namespace already exists, cannot proceed
		return nil, serviceerror.NewNamespaceAlreadyExistsf("Namespace %q already exists", registerRequest.GetNamespace())
	case *serviceerror.NamespaceNotFound:
		// namespace does not exists, proceeds
	default:
		// other err
		return nil, err
	}

	var activeClusterName string
	// input validation on cluster names
	if registerRequest.GetActiveClusterName() != "" {
		activeClusterName = registerRequest.GetActiveClusterName()
	} else {
		activeClusterName = d.clusterMetadata.GetCurrentClusterName()
	}
	var clusters []string
	for _, clusterConfig := range registerRequest.GetClusters() {
		clusterName := clusterConfig.GetClusterName()
		clusters = append(clusters, clusterName)
	}
	clusters = persistence.GetOrUseDefaultClusters(activeClusterName, clusters)

	currentHistoryArchivalState := namespace.NeverEnabledState()
	nextHistoryArchivalState := currentHistoryArchivalState
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(
			registerRequest.GetHistoryArchivalState(),
			registerRequest.GetHistoryArchivalUri(),
			clusterHistoryArchivalConfig.GetNamespaceDefaultState(),
			clusterHistoryArchivalConfig.GetNamespaceDefaultURI(),
		)
		if err != nil {
			return nil, err
		}

		nextHistoryArchivalState, _, err = currentHistoryArchivalState.GetNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	currentVisibilityArchivalState := namespace.NeverEnabledState()
	nextVisibilityArchivalState := currentVisibilityArchivalState
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(
			registerRequest.GetVisibilityArchivalState(),
			registerRequest.GetVisibilityArchivalUri(),
			clusterVisibilityArchivalConfig.GetNamespaceDefaultState(),
			clusterVisibilityArchivalConfig.GetNamespaceDefaultURI(),
		)
		if err != nil {
			return nil, err
		}

		nextVisibilityArchivalState, _, err = currentVisibilityArchivalState.GetNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	info := persistencespb.NamespaceInfo_builder{
		Id:          uuid.NewString(),
		Name:        registerRequest.GetNamespace(),
		State:       enumspb.NAMESPACE_STATE_REGISTERED,
		Owner:       registerRequest.GetOwnerEmail(),
		Description: registerRequest.GetDescription(),
		Data:        registerRequest.GetData(),
	}.Build()
	config := persistencespb.NamespaceConfig_builder{
		Retention:                    registerRequest.GetWorkflowExecutionRetentionPeriod(),
		HistoryArchivalState:         nextHistoryArchivalState.State,
		HistoryArchivalUri:           nextHistoryArchivalState.URI,
		VisibilityArchivalState:      nextVisibilityArchivalState.State,
		VisibilityArchivalUri:        nextVisibilityArchivalState.URI,
		BadBinaries:                  namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
		CustomSearchAttributeAliases: nil,
	}.Build()
	replicationConfig := persistencespb.NamespaceReplicationConfig_builder{
		ActiveClusterName: activeClusterName,
		Clusters:          clusters,
		State:             enumspb.REPLICATION_STATE_NORMAL,
	}.Build()
	isGlobalNamespace := registerRequest.GetIsGlobalNamespace()

	if err := d.namespaceAttrValidator.ValidateNamespaceConfig(config); err != nil {
		return nil, err
	}
	if isGlobalNamespace {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForGlobalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	} else {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForLocalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	failoverVersion := common.EmptyVersion
	if registerRequest.GetIsGlobalNamespace() {
		failoverVersion = d.clusterMetadata.GetNextFailoverVersion(activeClusterName, 0)
	}

	namespaceRequest := &persistence.CreateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info:              info,
			Config:            config,
			ReplicationConfig: replicationConfig,
			ConfigVersion:     0,
			FailoverVersion:   failoverVersion,
		}.Build(),
		IsGlobalNamespace: isGlobalNamespace,
	}

	namespaceResponse, err := d.metadataMgr.CreateNamespace(ctx, namespaceRequest)
	if err != nil {
		return nil, err
	}

	err = d.namespaceReplicator.HandleTransmissionTask(
		ctx,
		enumsspb.NAMESPACE_OPERATION_CREATE,
		namespaceRequest.Namespace.GetInfo(),
		namespaceRequest.Namespace.GetConfig(),
		namespaceRequest.Namespace.GetReplicationConfig(),
		false,
		namespaceRequest.Namespace.GetConfigVersion(),
		namespaceRequest.Namespace.GetFailoverVersion(),
		namespaceRequest.IsGlobalNamespace,
		nil,
	)
	if err != nil {
		return nil, err
	}

	d.logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(registerRequest.GetNamespace()),
		tag.WorkflowNamespaceID(namespaceResponse.ID),
	)

	return &workflowservice.RegisterNamespaceResponse{}, nil
}

// ListNamespaces list all namespaces
func (d *namespaceHandler) ListNamespaces(
	ctx context.Context,
	listRequest *workflowservice.ListNamespacesRequest,
) (*workflowservice.ListNamespacesResponse, error) {

	pageSize := 100
	if listRequest.GetPageSize() != 0 {
		pageSize = int(listRequest.GetPageSize())
	}

	resp, err := d.metadataMgr.ListNamespaces(ctx, &persistence.ListNamespacesRequest{
		PageSize:       pageSize,
		NextPageToken:  listRequest.GetNextPageToken(),
		IncludeDeleted: listRequest.GetNamespaceFilter().GetIncludeDeleted(),
	})

	if err != nil {
		return nil, err
	}

	var namespaces []*workflowservice.DescribeNamespaceResponse
	for _, namespace := range resp.Namespaces {
		namespaceInfo, config, replicationConfig, failoverHistory :=
			d.createResponse(
				namespace.Namespace.GetInfo(),
				namespace.Namespace.GetConfig(),
				namespace.Namespace.GetReplicationConfig())
		desc := workflowservice.DescribeNamespaceResponse_builder{
			IsGlobalNamespace: namespace.IsGlobalNamespace,
			FailoverVersion:   namespace.Namespace.GetFailoverVersion(),
			NamespaceInfo:     namespaceInfo,
			Config:            config,
			ReplicationConfig: replicationConfig,
			FailoverHistory:   failoverHistory,
		}.Build()
		namespaces = append(namespaces, desc)
	}

	response := workflowservice.ListNamespacesResponse_builder{
		Namespaces:    namespaces,
		NextPageToken: resp.NextPageToken,
	}.Build()

	return response, nil
}

// DescribeNamespace describe the namespace
func (d *namespaceHandler) DescribeNamespace(
	ctx context.Context,
	describeRequest *workflowservice.DescribeNamespaceRequest,
) (*workflowservice.DescribeNamespaceResponse, error) {

	// TODO, we should migrate the non global namespace to new table, see #773
	req := &persistence.GetNamespaceRequest{
		Name: describeRequest.GetNamespace(),
		ID:   describeRequest.GetId(),
	}
	resp, err := d.metadataMgr.GetNamespace(ctx, req)
	if err != nil {
		return nil, err
	}

	namespaceInfo, config, replicationConfig, failoverHistory :=
		d.createResponse(resp.Namespace.GetInfo(), resp.Namespace.GetConfig(), resp.Namespace.GetReplicationConfig())
	response := workflowservice.DescribeNamespaceResponse_builder{
		IsGlobalNamespace: resp.IsGlobalNamespace,
		FailoverVersion:   resp.Namespace.GetFailoverVersion(),
		NamespaceInfo:     namespaceInfo,
		Config:            config,
		ReplicationConfig: replicationConfig,
		FailoverHistory:   failoverHistory,
	}.Build()
	return response, nil
}

// UpdateNamespace update the namespace
//
//nolint:revive // cognitive complexity grandfathered
func (d *namespaceHandler) UpdateNamespace(
	ctx context.Context,
	updateRequest *workflowservice.UpdateNamespaceRequest,
) (*workflowservice.UpdateNamespaceResponse, error) {

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 namespace table
	// and since we do not know which table will return the namespace afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: updateRequest.GetNamespace()})
	if err != nil {
		return nil, err
	}

	info := getResponse.Namespace.GetInfo()
	config := getResponse.Namespace.GetConfig()
	replicationConfig := getResponse.Namespace.GetReplicationConfig()
	failoverHistory := getResponse.Namespace.GetReplicationConfig().GetFailoverHistory()
	configVersion := getResponse.Namespace.GetConfigVersion()
	failoverVersion := getResponse.Namespace.GetFailoverVersion()
	failoverNotificationVersion := getResponse.Namespace.GetFailoverNotificationVersion()
	isGlobalNamespace := getResponse.IsGlobalNamespace || updateRequest.GetPromoteNamespace()
	needsNamespacePromotion := !getResponse.IsGlobalNamespace && updateRequest.GetPromoteNamespace()

	currentHistoryArchivalState := &namespace.ArchivalConfigState{
		State: config.GetHistoryArchivalState(),
		URI:   config.GetHistoryArchivalUri(),
	}
	nextHistoryArchivalState := currentHistoryArchivalState
	historyArchivalConfigChanged := false
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if updateRequest.HasConfig() && clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfig()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.GetHistoryArchivalState(), cfg.GetHistoryArchivalUri(), clusterHistoryArchivalConfig.GetNamespaceDefaultURI())
		if err != nil {
			return nil, err
		}
		nextHistoryArchivalState, historyArchivalConfigChanged, err = currentHistoryArchivalState.GetNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	currentVisibilityArchivalState := &namespace.ArchivalConfigState{
		State: config.GetVisibilityArchivalState(),
		URI:   config.GetVisibilityArchivalUri(),
	}
	nextVisibilityArchivalState := currentVisibilityArchivalState
	visibilityArchivalConfigChanged := false
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if updateRequest.HasConfig() && clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfig()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.GetVisibilityArchivalState(), cfg.GetVisibilityArchivalUri(), clusterVisibilityArchivalConfig.GetNamespaceDefaultURI())
		if err != nil {
			return nil, err
		}
		nextVisibilityArchivalState, visibilityArchivalConfigChanged, err = currentVisibilityArchivalState.GetNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	// whether active cluster is changed
	activeClusterChanged := false
	// whether anything other than active cluster is changed
	configurationChanged := false
	// whether replication cluster list is changed
	clusterListChanged := false

	if updateRequest.HasUpdateInfo() {
		updatedInfo := updateRequest.GetUpdateInfo()
		if updatedInfo.GetDescription() != "" {
			configurationChanged = true
			info.SetDescription(updatedInfo.GetDescription())
		}
		if updatedInfo.GetOwnerEmail() != "" {
			configurationChanged = true
			info.SetOwner(updatedInfo.GetOwnerEmail())
		}
		if updatedInfo.GetData() != nil {
			configurationChanged = true
			// only do merging
			info.SetData(d.mergeNamespaceData(info.GetData(), updatedInfo.GetData()))
		}
		if updatedInfo.GetState() != enumspb.NAMESPACE_STATE_UNSPECIFIED && info.GetState() != updatedInfo.GetState() {
			configurationChanged = true
			if err := validateStateUpdate(getResponse, updateRequest); err != nil {
				return nil, err
			}
			info.SetState(updatedInfo.GetState())
		}
	}
	if updateRequest.HasConfig() {
		updatedConfig := updateRequest.GetConfig()
		if updatedConfig.GetWorkflowExecutionRetentionTtl() != nil {
			configurationChanged = true

			config.SetRetention(updatedConfig.GetWorkflowExecutionRetentionTtl())
			if err := d.validateRetentionDuration(
				config.GetRetention(),
				isGlobalNamespace,
			); err != nil {
				return nil, err
			}
		}
		if historyArchivalConfigChanged {
			configurationChanged = true
			config.SetHistoryArchivalState(nextHistoryArchivalState.State)
			config.SetHistoryArchivalUri(nextHistoryArchivalState.URI)
		}
		if visibilityArchivalConfigChanged {
			configurationChanged = true
			config.SetVisibilityArchivalState(nextVisibilityArchivalState.State)
			config.SetVisibilityArchivalUri(nextVisibilityArchivalState.URI)
		}
		if updatedConfig.HasBadBinaries() {
			maxLength := d.config.MaxBadBinaries(updateRequest.GetNamespace())
			// only do merging
			bb := d.mergeBadBinaries(config.GetBadBinaries().GetBinaries(), updatedConfig.GetBadBinaries().GetBinaries(), time.Now().UTC())
			config.SetBadBinaries(bb)
			if len(config.GetBadBinaries().GetBinaries()) > maxLength {
				return nil, serviceerror.NewInvalidArgumentf("Total resetBinaries cannot exceed the max limit: %v", maxLength)
			}
		}
		if len(updatedConfig.GetCustomSearchAttributeAliases()) > 0 {
			configurationChanged = true
			csaAliases, err := d.upsertCustomSearchAttributesAliases(
				config.GetCustomSearchAttributeAliases(),
				updatedConfig.GetCustomSearchAttributeAliases(),
			)
			if err != nil {
				return nil, err
			}
			config.SetCustomSearchAttributeAliases(csaAliases)
		}
	}

	if updateRequest.GetDeleteBadBinary() != "" {
		binChecksum := updateRequest.GetDeleteBadBinary()
		_, ok := config.GetBadBinaries().GetBinaries()[binChecksum]
		if !ok {
			return nil, serviceerror.NewInvalidArgumentf("Bad binary checksum %v doesn't exists.", binChecksum)
		}
		configurationChanged = true
		delete(config.GetBadBinaries().GetBinaries(), binChecksum)
	}

	if updateRequest.HasReplicationConfig() {
		updateReplicationConfig := updateRequest.GetReplicationConfig()
		if len(updateReplicationConfig.GetClusters()) != 0 {
			configurationChanged = true
			clusterListChanged = true
			var clustersNew []string
			for _, clusterConfig := range updateReplicationConfig.GetClusters() {
				clustersNew = append(clustersNew, clusterConfig.GetClusterName())
			}
			replicationConfig.SetClusters(clustersNew)
		}
		if updateReplicationConfig.GetState() != enumspb.REPLICATION_STATE_UNSPECIFIED &&
			updateReplicationConfig.GetState() != replicationConfig.GetState() {
			if err := validateReplicationStateUpdate(getResponse, updateRequest); err != nil {
				return nil, err
			}
			configurationChanged = true
			replicationConfig.SetState(updateReplicationConfig.GetState())
		}

		if updateReplicationConfig.GetActiveClusterName() != "" {
			activeClusterChanged = true
			replicationConfig.SetActiveClusterName(updateReplicationConfig.GetActiveClusterName())
			replicationConfig.SetState(enumspb.REPLICATION_STATE_NORMAL)
		}
	}

	if err := d.namespaceAttrValidator.ValidateNamespaceConfig(config); err != nil {
		return nil, err
	}
	if isGlobalNamespace {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForGlobalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
		if !d.clusterMetadata.IsGlobalNamespaceEnabled() {
			return nil, serviceerror.NewInvalidArgumentf("global namespace is not enabled on this "+
				"cluster, cannot update global namespace or promote local namespace: %v", updateRequest.GetNamespace())
		}
	} else {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForLocalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	if configurationChanged && activeClusterChanged && isGlobalNamespace {
		return nil, errCannotDoNamespaceFailoverAndUpdate
	} else if configurationChanged || activeClusterChanged || needsNamespacePromotion {
		if (needsNamespacePromotion || activeClusterChanged) && isGlobalNamespace {
			failoverVersion = d.clusterMetadata.GetNextFailoverVersion(
				replicationConfig.GetActiveClusterName(),
				failoverVersion,
			)
			failoverNotificationVersion = notificationVersion
		}
		// set the versions
		if configurationChanged {
			configVersion++
		}

		if (configurationChanged || activeClusterChanged) && isGlobalNamespace {
			// N.B., it should be sufficient to check only for activeClusterChanged. In order to be defensive, we also
			// check for configurationChanged. If nothing needs to be updated this will be a no-op.
			failoverHistory = d.maybeUpdateFailoverHistory(
				failoverHistory,
				updateRequest.GetReplicationConfig(),
				getResponse.Namespace,
				failoverVersion,
			)
		}

		replicationConfig.SetFailoverHistory(failoverHistory)
		updateReq := &persistence.UpdateNamespaceRequest{
			Namespace: persistencespb.NamespaceDetail_builder{
				Info:                        info,
				Config:                      config,
				ReplicationConfig:           replicationConfig,
				ConfigVersion:               configVersion,
				FailoverVersion:             failoverVersion,
				FailoverNotificationVersion: failoverNotificationVersion,
			}.Build(),
			IsGlobalNamespace:   isGlobalNamespace,
			NotificationVersion: notificationVersion,
		}
		err = d.metadataMgr.UpdateNamespace(ctx, updateReq)
		if err != nil {
			return nil, err
		}
	}

	err = d.namespaceReplicator.HandleTransmissionTask(
		ctx,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		info,
		config,
		replicationConfig,
		clusterListChanged,
		configVersion,
		failoverVersion,
		isGlobalNamespace,
		failoverHistory,
	)
	if err != nil {
		return nil, err
	}

	namespaceInfo, nsConfig, nsReplicationConfig, _ := d.createResponse(info, config, replicationConfig)
	response := workflowservice.UpdateNamespaceResponse_builder{
		IsGlobalNamespace: isGlobalNamespace,
		FailoverVersion:   failoverVersion,
		NamespaceInfo:     namespaceInfo,
		Config:            nsConfig,
		ReplicationConfig: nsReplicationConfig,
	}.Build()

	d.logger.Info("Update namespace succeeded",
		tag.WorkflowNamespace(info.GetName()),
		tag.WorkflowNamespaceID(info.GetId()),
	)
	return response, nil
}

// DeprecateNamespace deprecates a namespace
// Deprecated.
func (d *namespaceHandler) DeprecateNamespace(
	ctx context.Context,
	deprecateRequest *workflowservice.DeprecateNamespaceRequest,
) (*workflowservice.DeprecateNamespaceResponse, error) {

	clusterMetadata := d.clusterMetadata
	// TODO remove the IsGlobalNamespaceEnabled check once cross DC is public
	if clusterMetadata.IsGlobalNamespaceEnabled() && !clusterMetadata.IsMasterCluster() {
		return nil, errNotMasterCluster
	}

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 namespace table
	// and since we do not know which table will return the namespace afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: deprecateRequest.GetNamespace()})
	if err != nil {
		return nil, err
	}

	getResponse.Namespace.SetConfigVersion(getResponse.Namespace.GetConfigVersion() + 1)
	getResponse.Namespace.GetInfo().SetState(enumspb.NAMESPACE_STATE_DEPRECATED)
	updateReq := &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info:                        getResponse.Namespace.GetInfo(),
			Config:                      getResponse.Namespace.GetConfig(),
			ReplicationConfig:           getResponse.Namespace.GetReplicationConfig(),
			ConfigVersion:               getResponse.Namespace.GetConfigVersion(),
			FailoverVersion:             getResponse.Namespace.GetFailoverVersion(),
			FailoverNotificationVersion: getResponse.Namespace.GetFailoverNotificationVersion(),
		}.Build(),
		NotificationVersion: notificationVersion,
		IsGlobalNamespace:   getResponse.IsGlobalNamespace,
	}
	err = d.metadataMgr.UpdateNamespace(ctx, updateReq)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *namespaceHandler) CreateWorkflowRule(
	ctx context.Context,
	ruleSpec *rulespb.WorkflowRuleSpec,
	createdByIdentity string,
	description string,
	nsName string,
) (*rulespb.WorkflowRule, error) {

	if ruleSpec.GetId() == "" {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule ID is not set.")
	}

	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return nil, err
	}

	existingNamespace := getNamespaceResponse.Namespace
	config := getNamespaceResponse.Namespace.GetConfig()

	if config.GetWorkflowRules() == nil {
		config.SetWorkflowRules(make(map[string]*rulespb.WorkflowRule))
	} else {
		maxRules := d.config.MaxWorkflowRulesPerNamespace(nsName)
		if len(config.GetWorkflowRules()) >= maxRules {
			d.removeOldestExpiredWorkflowRule(nsName, config.GetWorkflowRules())
		}
		if len(config.GetWorkflowRules()) >= maxRules {
			return nil, serviceerror.NewInvalidArgumentf("Workflow Rule limit exceeded. Max: %v", maxRules)
		}
	}

	_, ok := config.GetWorkflowRules()[ruleSpec.GetId()]
	if ok {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule with this ID already exists.")
	}

	workflowRule := rulespb.WorkflowRule_builder{
		Spec:              ruleSpec,
		CreateTime:        timestamppb.New(d.timeSource.Now()),
		CreatedByIdentity: createdByIdentity,
		Description:       description,
	}.Build()
	config.GetWorkflowRules()[ruleSpec.GetId()] = workflowRule

	updateReq := &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info:                        existingNamespace.GetInfo(),
			Config:                      config,
			ReplicationConfig:           existingNamespace.GetReplicationConfig(),
			ConfigVersion:               existingNamespace.GetConfigVersion() + 1,
			FailoverVersion:             existingNamespace.GetFailoverVersion(),
			FailoverNotificationVersion: existingNamespace.GetFailoverNotificationVersion(),
		}.Build(),
		IsGlobalNamespace:   getNamespaceResponse.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}
	err = d.metadataMgr.UpdateNamespace(ctx, updateReq)
	if err != nil {
		return nil, err
	}

	return workflowRule, nil
}

func (d *namespaceHandler) removeOldestExpiredWorkflowRule(nsName string, rules map[string]*rulespb.WorkflowRule) {
	oldestTime := d.timeSource.Now()
	var oldestKey string
	found := false

	for key, rule := range rules {
		expirationTime := rule.GetSpec().GetExpirationTime()
		if expirationTime == nil {
			continue
		}
		if !found || expirationTime.AsTime().Before(oldestTime) {
			oldestTime = expirationTime.AsTime()
			oldestKey = key
			found = true
		}
	}

	if found {
		d.logger.Info(
			"Removed expired workflow rule",
			tag.WorkflowRuleID(oldestKey),
			tag.WorkflowNamespace(nsName),
		)
		delete(rules, oldestKey)
	}
}

func (d *namespaceHandler) DescribeWorkflowRule(
	ctx context.Context, ruleID string, nsName string,
) (*rulespb.WorkflowRule, error) {
	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return nil, err
	}

	if getNamespaceResponse.Namespace.GetConfig().GetWorkflowRules() == nil {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}

	rule, ok := getNamespaceResponse.Namespace.GetConfig().GetWorkflowRules()[ruleID]
	if !ok {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}

	return rule, nil
}

func (d *namespaceHandler) DeleteWorkflowRule(
	ctx context.Context, ruleID string, nsName string,
) error {
	if ruleID == "" {
		return serviceerror.NewInvalidArgument("Workflow Rule ID is not set.")
	}

	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return err
	}

	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return err
	}

	existingNamespace := getNamespaceResponse.Namespace
	config := getNamespaceResponse.Namespace.GetConfig()
	if config.GetWorkflowRules() == nil {
		return serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}
	_, ok := config.GetWorkflowRules()[ruleID]
	if !ok {
		return serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}

	delete(config.GetWorkflowRules(), ruleID)

	updateReq := &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info:                        existingNamespace.GetInfo(),
			Config:                      config,
			ReplicationConfig:           existingNamespace.GetReplicationConfig(),
			ConfigVersion:               existingNamespace.GetConfigVersion() + 1,
			FailoverVersion:             existingNamespace.GetFailoverVersion(),
			FailoverNotificationVersion: existingNamespace.GetFailoverNotificationVersion(),
		}.Build(),
		IsGlobalNamespace:   getNamespaceResponse.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}
	return d.metadataMgr.UpdateNamespace(ctx, updateReq)
}

func (d *namespaceHandler) ListWorkflowRules(
	ctx context.Context, nsName string,
) ([]*rulespb.WorkflowRule, error) {
	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return nil, err
	}

	workflowRulesMap := getNamespaceResponse.Namespace.GetConfig().GetWorkflowRules()
	if workflowRulesMap == nil {
		return []*rulespb.WorkflowRule{}, nil
	}

	workflowRules := make([]*rulespb.WorkflowRule, 0, len(workflowRulesMap))
	for _, rule := range workflowRulesMap {
		workflowRules = append(workflowRules, rule)
	}
	return workflowRules, nil
}

func (d *namespaceHandler) createResponse(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	replicationConfig *persistencespb.NamespaceReplicationConfig,
) (*namespacepb.NamespaceInfo, *namespacepb.NamespaceConfig, *replicationpb.NamespaceReplicationConfig, []*replicationpb.FailoverStatus) {

	numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute := d.config.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute(info.GetName())

	infoResult := namespacepb.NamespaceInfo_builder{
		Name:        info.GetName(),
		State:       info.GetState(),
		Description: info.GetDescription(),
		OwnerEmail:  info.GetOwner(),
		Data:        info.GetData(),
		Id:          info.GetId(),

		Capabilities: namespacepb.NamespaceInfo_Capabilities_builder{
			EagerWorkflowStart:              d.config.EnableEagerWorkflowStart(info.GetName()),
			SyncUpdate:                      d.config.EnableUpdateWorkflowExecution(info.GetName()),
			AsyncUpdate:                     d.config.EnableUpdateWorkflowExecutionAsyncAccepted(info.GetName()),
			ReportedProblemsSearchAttribute: numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute > 0,
			WorkerHeartbeats:                d.config.WorkerHeartbeatsEnabled(info.GetName()),
			WorkflowPause:                   d.config.WorkflowPauseEnabled(info.GetName()),
			StandaloneActivities:            d.config.Activity.Enabled(info.GetName()),
		}.Build(),
		Limits: namespacepb.NamespaceInfo_Limits_builder{
			BlobSizeLimitError: int64(d.config.BlobSizeLimitError(info.GetName())),
			MemoSizeLimitError: int64(d.config.MemoSizeLimitError(info.GetName())),
		}.Build(),
		SupportsSchedules: d.config.EnableSchedules(info.GetName()),
	}.Build()

	configResult := namespacepb.NamespaceConfig_builder{
		WorkflowExecutionRetentionTtl: config.GetRetention(),
		HistoryArchivalState:          config.GetHistoryArchivalState(),
		HistoryArchivalUri:            config.GetHistoryArchivalUri(),
		VisibilityArchivalState:       config.GetVisibilityArchivalState(),
		VisibilityArchivalUri:         config.GetVisibilityArchivalUri(),
		BadBinaries:                   config.GetBadBinaries(),
		CustomSearchAttributeAliases:  config.GetCustomSearchAttributeAliases(),
	}.Build()

	var clusters []*replicationpb.ClusterReplicationConfig
	for _, cluster := range replicationConfig.GetClusters() {
		clusters = append(clusters, replicationpb.ClusterReplicationConfig_builder{
			ClusterName: cluster,
		}.Build())
	}
	replicationConfigResult := replicationpb.NamespaceReplicationConfig_builder{
		ActiveClusterName: replicationConfig.GetActiveClusterName(),
		Clusters:          clusters,
		State:             replicationConfig.GetState(),
	}.Build()

	var failoverHistory []*replicationpb.FailoverStatus
	for _, entry := range replicationConfig.GetFailoverHistory() {
		failoverHistory = append(failoverHistory, replicationpb.FailoverStatus_builder{
			FailoverTime:    entry.GetFailoverTime(),
			FailoverVersion: entry.GetFailoverVersion(),
		}.Build())
	}

	return infoResult, configResult, replicationConfigResult, failoverHistory
}

func (d *namespaceHandler) mergeBadBinaries(
	old map[string]*namespacepb.BadBinaryInfo,
	new map[string]*namespacepb.BadBinaryInfo,
	createTime time.Time,
) *namespacepb.BadBinaries {

	if old == nil {
		old = map[string]*namespacepb.BadBinaryInfo{}
	}
	for k, v := range new {
		v.SetCreateTime(timestamppb.New(createTime))
		old[k] = v
	}
	// DO NOT SUBMIT: fix callers to work with a pointer (go/goprotoapi-findings#message-value)
	return namespacepb.BadBinaries_builder{
		Binaries: old,
	}.Build()
}

func (d *namespaceHandler) mergeNamespaceData(
	old map[string]string,
	new map[string]string,
) map[string]string {

	if old == nil {
		old = map[string]string{}
	}
	for k, v := range new {
		old[k] = v
	}
	return old
}

func (d *namespaceHandler) upsertCustomSearchAttributesAliases(
	current map[string]string,
	upsert map[string]string,
) (map[string]string, error) {
	result := util.CloneMapNonNil(current)
	for key, value := range upsert {
		if value == "" {
			delete(result, key)
		} else if _, ok := current[key]; !ok {
			result[key] = value
		} else {
			return nil, errCustomSearchAttributeFieldAlreadyAllocated
		}
	}
	return result, nil
}

func (d *namespaceHandler) toArchivalRegisterEvent(
	state enumspb.ArchivalState,
	URI string,
	defaultState enumspb.ArchivalState,
	defaultURI string,
) (*namespace.ArchivalConfigEvent, error) {

	event := &namespace.ArchivalConfigEvent{
		State:      state,
		URI:        URI,
		DefaultURI: defaultURI,
	}
	if event.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED {
		event.State = defaultState
	}
	if err := event.Validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *namespaceHandler) toArchivalUpdateEvent(
	state enumspb.ArchivalState,
	URI string,
	defaultURI string,
) (*namespace.ArchivalConfigEvent, error) {

	event := &namespace.ArchivalConfigEvent{
		State:      state,
		URI:        URI,
		DefaultURI: defaultURI,
	}
	if err := event.Validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *namespaceHandler) validateHistoryArchivalURI(URIString string) error {
	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return err
	}

	a, err := d.archiverProvider.GetHistoryArchiver(URI.Scheme())
	if err != nil {
		return err
	}

	return a.ValidateURI(URI)
}

func (d *namespaceHandler) validateVisibilityArchivalURI(URIString string) error {
	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return err
	}

	a, err := d.archiverProvider.GetVisibilityArchiver(URI.Scheme())
	if err != nil {
		return err
	}

	return a.ValidateURI(URI)
}

// maybeUpdateFailoverHistory adds an entry if the Namespace is becoming active in a new cluster.
func (d *namespaceHandler) maybeUpdateFailoverHistory(
	failoverHistory []*persistencespb.FailoverStatus,
	updateReplicationConfig *replicationpb.NamespaceReplicationConfig,
	namespaceDetail *persistencespb.NamespaceDetail,
	newFailoverVersion int64,
) []*persistencespb.FailoverStatus {
	d.logger.Debug(
		"maybeUpdateFailoverHistory",
		tag.NewAnyTag("failoverHistory", failoverHistory),
		tag.NewAnyTag("updateReplConfig", updateReplicationConfig),
		tag.NewAnyTag("namespaceDetail", namespaceDetail),
	)
	if updateReplicationConfig == nil {
		d.logger.Debug("updateReplicationConfig was nil")
		return failoverHistory
	}

	lastFailoverVersion := int64(-1)
	if l := len(namespaceDetail.GetReplicationConfig().GetFailoverHistory()); l > 0 {
		lastFailoverVersion = namespaceDetail.GetReplicationConfig().GetFailoverHistory()[l-1].GetFailoverVersion()
	}
	if lastFailoverVersion != newFailoverVersion {
		now := d.timeSource.Now()
		failoverHistory = append(
			failoverHistory, persistencespb.FailoverStatus_builder{
				FailoverTime:    timestamppb.New(now),
				FailoverVersion: newFailoverVersion,
			}.Build(),
		)
	}
	if l := len(failoverHistory); l > maxReplicationHistorySize {
		failoverHistory = failoverHistory[l-maxReplicationHistorySize : l]
	}
	return failoverHistory
}

// validateRetentionDuration ensures that retention duration can't be set below a sane minimum.
func (d *namespaceHandler) validateRetentionDuration(retention *durationpb.Duration, isGlobalNamespace bool) error {
	if err := timestamp.ValidateAndCapProtoDuration(retention); err != nil {
		return errInvalidRetentionPeriod
	}

	var minRetention time.Duration
	if isGlobalNamespace {
		minRetention = d.config.NamespaceMinRetentionGlobal()
	} else {
		minRetention = d.config.NamespaceMinRetentionLocal()
	}

	if timestamp.DurationValue(retention) < minRetention {
		return errInvalidRetentionPeriod
	}
	return nil
}

func validateReplicationStateUpdate(existingNamespace *persistence.GetNamespaceResponse, nsUpdateRequest *workflowservice.UpdateNamespaceRequest) error {
	if !nsUpdateRequest.HasReplicationConfig() ||
		nsUpdateRequest.GetReplicationConfig().GetState() == enumspb.REPLICATION_STATE_UNSPECIFIED ||
		nsUpdateRequest.GetReplicationConfig().GetState() == existingNamespace.Namespace.GetReplicationConfig().GetState() {
		return nil // no change
	}

	if existingNamespace.Namespace.GetInfo().GetState() != enumspb.NAMESPACE_STATE_REGISTERED {
		return serviceerror.NewInvalidArgumentf(
			"update ReplicationState is only supported when namespace is in %s state, current state: %s",
			enumspb.NAMESPACE_STATE_REGISTERED,
			existingNamespace.Namespace.GetInfo().GetState(),
		)
	}

	if nsUpdateRequest.GetReplicationConfig().GetState() == enumspb.REPLICATION_STATE_HANDOVER {
		if !existingNamespace.IsGlobalNamespace {
			return serviceerror.NewInvalidArgumentf(
				"%s can only be set for global namespace",
				enumspb.REPLICATION_STATE_HANDOVER,
			)
		}
		// verify namespace has more than 1 replication clusters
		replicationClusterCount := len(existingNamespace.Namespace.GetReplicationConfig().GetClusters())
		if len(nsUpdateRequest.GetReplicationConfig().GetClusters()) > 0 {
			replicationClusterCount = len(nsUpdateRequest.GetReplicationConfig().GetClusters())
		}
		if replicationClusterCount < 2 {
			return serviceerror.NewInvalidArgumentf("%s require more than one replication clusters", enumspb.REPLICATION_STATE_HANDOVER)
		}
	}
	return nil
}

func validateStateUpdate(existingNamespace *persistence.GetNamespaceResponse, nsUpdateRequest *workflowservice.UpdateNamespaceRequest) error {
	if !nsUpdateRequest.HasUpdateInfo() {
		return nil // no change
	}
	oldState := existingNamespace.Namespace.GetInfo().GetState()
	newState := nsUpdateRequest.GetUpdateInfo().GetState()
	if newState == enumspb.NAMESPACE_STATE_UNSPECIFIED || oldState == newState {
		return nil // no change
	}

	if existingNamespace.Namespace.HasReplicationConfig() &&
		existingNamespace.Namespace.GetReplicationConfig().GetState() == enumspb.REPLICATION_STATE_HANDOVER {
		return serviceerror.NewInvalidArgument("cannot update namespace state while its replication state in REPLICATION_STATE_HANDOVER")
	}

	switch oldState {
	case enumspb.NAMESPACE_STATE_REGISTERED:
		switch newState {
		case enumspb.NAMESPACE_STATE_DELETED, enumspb.NAMESPACE_STATE_DEPRECATED:
			return nil
		default:
			return errInvalidNamespaceStateUpdate
		}

	case enumspb.NAMESPACE_STATE_DEPRECATED:
		switch newState {
		case enumspb.NAMESPACE_STATE_DELETED:
			return nil
		default:
			return errInvalidNamespaceStateUpdate
		}

	case enumspb.NAMESPACE_STATE_DELETED:
		return errInvalidNamespaceStateUpdate
	default:
		return errInvalidNamespaceStateUpdate
	}
}
