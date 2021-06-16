// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination handler_mock.go

package namespace

import (
	"context"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

type (
	// Handler is the namespace operation handler
	Handler interface {
		DeprecateNamespace(
			ctx context.Context,
			deprecateRequest *workflowservice.DeprecateNamespaceRequest,
		) (*workflowservice.DeprecateNamespaceResponse, error)
		DescribeNamespace(
			ctx context.Context,
			describeRequest *workflowservice.DescribeNamespaceRequest,
		) (*workflowservice.DescribeNamespaceResponse, error)
		ListNamespaces(
			ctx context.Context,
			listRequest *workflowservice.ListNamespacesRequest,
		) (*workflowservice.ListNamespacesResponse, error)
		RegisterNamespace(
			ctx context.Context,
			registerRequest *workflowservice.RegisterNamespaceRequest,
		) (*workflowservice.RegisterNamespaceResponse, error)
		UpdateNamespace(
			ctx context.Context,
			updateRequest *workflowservice.UpdateNamespaceRequest,
		) (*workflowservice.UpdateNamespaceResponse, error)
	}

	// HandlerImpl is the namespace operation handler implementation
	HandlerImpl struct {
		maxBadBinaryCount      dynamicconfig.IntPropertyFnWithNamespaceFilter
		logger                 log.Logger
		metadataMgr            persistence.MetadataManager
		clusterMetadata        cluster.Metadata
		namespaceReplicator    Replicator
		namespaceAttrValidator *AttrValidatorImpl
		archivalMetadata       archiver.ArchivalMetadata
		archiverProvider       provider.ArchiverProvider
	}
)

var _ Handler = (*HandlerImpl)(nil)

// NewHandler create a new namespace handler
func NewHandler(
	minRetention time.Duration,
	maxBadBinaryCount dynamicconfig.IntPropertyFnWithNamespaceFilter,
	logger log.Logger,
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	namespaceReplicator Replicator,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
) *HandlerImpl {
	return &HandlerImpl{
		maxBadBinaryCount:      maxBadBinaryCount,
		logger:                 logger,
		metadataMgr:            metadataMgr,
		clusterMetadata:        clusterMetadata,
		namespaceReplicator:    namespaceReplicator,
		namespaceAttrValidator: newAttrValidator(clusterMetadata, minRetention),
		archivalMetadata:       archivalMetadata,
		archiverProvider:       archiverProvider,
	}
}

// RegisterNamespace register a new namespace
func (d *HandlerImpl) RegisterNamespace(
	_ context.Context,
	registerRequest *workflowservice.RegisterNamespaceRequest,
) (*workflowservice.RegisterNamespaceResponse, error) {

	if !d.clusterMetadata.IsGlobalNamespaceEnabled() {
		if registerRequest.GetIsGlobalNamespace() {
			return nil, serviceerror.NewInvalidArgument("Cannot register global namespace when not enabled")
		}

		registerRequest.IsGlobalNamespace = false
	} else {
		// cluster global namespace enabled
		if !d.clusterMetadata.IsMasterCluster() && registerRequest.GetIsGlobalNamespace() {
			return nil, errNotMasterCluster
		}
	}

	// first check if the name is already registered as the local namespace
	_, err := d.metadataMgr.GetNamespace(&persistence.GetNamespaceRequest{Name: registerRequest.GetNamespace()})
	switch err.(type) {
	case nil:
		// namespace already exists, cannot proceed
		return nil, serviceerror.NewNamespaceAlreadyExists("Namespace already exists.")
	case *serviceerror.NotFound:
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
	for _, clusterConfig := range registerRequest.Clusters {
		clusterName := clusterConfig.GetClusterName()
		clusters = append(clusters, clusterName)
	}
	clusters = persistence.GetOrUseDefaultClusters(activeClusterName, clusters)

	currentHistoryArchivalState := neverEnabledState()
	nextHistoryArchivalState := currentHistoryArchivalState
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(
			registerRequest.HistoryArchivalState,
			registerRequest.GetHistoryArchivalUri(),
			clusterHistoryArchivalConfig.GetNamespaceDefaultState(),
			clusterHistoryArchivalConfig.GetNamespaceDefaultURI(),
		)
		if err != nil {
			return nil, err
		}

		nextHistoryArchivalState, _, err = currentHistoryArchivalState.getNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	currentVisibilityArchivalState := neverEnabledState()
	nextVisibilityArchivalState := currentVisibilityArchivalState
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(
			registerRequest.VisibilityArchivalState,
			registerRequest.GetVisibilityArchivalUri(),
			clusterVisibilityArchivalConfig.GetNamespaceDefaultState(),
			clusterVisibilityArchivalConfig.GetNamespaceDefaultURI(),
		)
		if err != nil {
			return nil, err
		}

		nextVisibilityArchivalState, _, err = currentVisibilityArchivalState.getNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	info := &persistencespb.NamespaceInfo{
		Id:          uuid.New(),
		Name:        registerRequest.GetNamespace(),
		State:       enumspb.NAMESPACE_STATE_REGISTERED,
		Owner:       registerRequest.GetOwnerEmail(),
		Description: registerRequest.GetDescription(),
		Data:        registerRequest.Data,
	}
	config := &persistencespb.NamespaceConfig{
		Retention:               registerRequest.GetWorkflowExecutionRetentionPeriod(),
		HistoryArchivalState:    nextHistoryArchivalState.State,
		HistoryArchivalUri:      nextHistoryArchivalState.URI,
		VisibilityArchivalState: nextVisibilityArchivalState.State,
		VisibilityArchivalUri:   nextVisibilityArchivalState.URI,
		BadBinaries:             &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}
	replicationConfig := &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: activeClusterName,
		Clusters:          clusters,
	}
	isGlobalNamespace := registerRequest.GetIsGlobalNamespace()

	if err := d.namespaceAttrValidator.validateNamespaceConfig(config); err != nil {
		return nil, err
	}
	if isGlobalNamespace {
		if err := d.namespaceAttrValidator.validateNamespaceReplicationConfigForGlobalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	} else {
		if err := d.namespaceAttrValidator.validateNamespaceReplicationConfigForLocalNamespace(
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
		Namespace: &persistencespb.NamespaceDetail{
			Info:              info,
			Config:            config,
			ReplicationConfig: replicationConfig,
			ConfigVersion:     0,
			FailoverVersion:   failoverVersion,
		},
		IsGlobalNamespace: isGlobalNamespace,
	}

	namespaceResponse, err := d.metadataMgr.CreateNamespace(namespaceRequest)
	if err != nil {
		return nil, err
	}

	if namespaceRequest.IsGlobalNamespace {
		err = d.namespaceReplicator.HandleTransmissionTask(
			enumsspb.NAMESPACE_OPERATION_CREATE,
			namespaceRequest.Namespace.Info,
			namespaceRequest.Namespace.Config,
			namespaceRequest.Namespace.ReplicationConfig,
			namespaceRequest.Namespace.ConfigVersion,
			namespaceRequest.Namespace.FailoverVersion,
			namespaceRequest.IsGlobalNamespace,
		)
		if err != nil {
			return nil, err
		}
	}

	d.logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(registerRequest.GetNamespace()),
		tag.WorkflowNamespaceID(namespaceResponse.ID),
	)

	return &workflowservice.RegisterNamespaceResponse{}, nil
}

// ListNamespaces list all namespaces
func (d *HandlerImpl) ListNamespaces(
	ctx context.Context,
	listRequest *workflowservice.ListNamespacesRequest,
) (*workflowservice.ListNamespacesResponse, error) {

	pageSize := 100
	if listRequest.GetPageSize() != 0 {
		pageSize = int(listRequest.GetPageSize())
	}

	resp, err := d.metadataMgr.ListNamespaces(&persistence.ListNamespacesRequest{
		PageSize:      pageSize,
		NextPageToken: listRequest.NextPageToken,
	})

	if err != nil {
		return nil, err
	}

	var namespaces []*workflowservice.DescribeNamespaceResponse
	for _, namespace := range resp.Namespaces {
		desc := &workflowservice.DescribeNamespaceResponse{
			IsGlobalNamespace: namespace.IsGlobalNamespace,
			FailoverVersion:   namespace.Namespace.FailoverVersion,
		}
		desc.NamespaceInfo, desc.Config, desc.ReplicationConfig =
			d.createResponse(ctx,
				namespace.Namespace.Info,
				namespace.Namespace.Config,
				namespace.Namespace.ReplicationConfig)
		namespaces = append(namespaces, desc)
	}

	response := &workflowservice.ListNamespacesResponse{
		Namespaces:    namespaces,
		NextPageToken: resp.NextPageToken,
	}

	return response, nil
}

// DescribeNamespace describe the namespace
func (d *HandlerImpl) DescribeNamespace(
	ctx context.Context,
	describeRequest *workflowservice.DescribeNamespaceRequest,
) (*workflowservice.DescribeNamespaceResponse, error) {

	// TODO, we should migrate the non global namespace to new table, see #773
	req := &persistence.GetNamespaceRequest{
		Name: describeRequest.GetNamespace(),
		ID:   describeRequest.GetId(),
	}
	resp, err := d.metadataMgr.GetNamespace(req)
	if err != nil {
		return nil, err
	}

	response := &workflowservice.DescribeNamespaceResponse{
		IsGlobalNamespace: resp.IsGlobalNamespace,
		FailoverVersion:   resp.Namespace.FailoverVersion,
	}
	response.NamespaceInfo, response.Config, response.ReplicationConfig =
		d.createResponse(ctx, resp.Namespace.Info, resp.Namespace.Config, resp.Namespace.ReplicationConfig)
	return response, nil
}

// UpdateNamespace update the namespace
func (d *HandlerImpl) UpdateNamespace(
	ctx context.Context,
	updateRequest *workflowservice.UpdateNamespaceRequest,
) (*workflowservice.UpdateNamespaceResponse, error) {

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 namespace table
	// and since we do not know which table will return the namespace afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata()
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetNamespace(&persistence.GetNamespaceRequest{Name: updateRequest.GetNamespace()})
	if err != nil {
		return nil, err
	}

	info := getResponse.Namespace.Info
	config := getResponse.Namespace.Config
	replicationConfig := getResponse.Namespace.ReplicationConfig
	configVersion := getResponse.Namespace.ConfigVersion
	failoverVersion := getResponse.Namespace.FailoverVersion
	failoverNotificationVersion := getResponse.Namespace.FailoverNotificationVersion
	isGlobalNamespace := getResponse.IsGlobalNamespace

	currentHistoryArchivalState := &ArchivalState{
		State: config.HistoryArchivalState,
		URI:   config.HistoryArchivalUri,
	}
	nextHistoryArchivalState := currentHistoryArchivalState
	historyArchivalConfigChanged := false
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if updateRequest.Config != nil && clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfig()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.HistoryArchivalState, cfg.GetHistoryArchivalUri(), clusterHistoryArchivalConfig.GetNamespaceDefaultURI())
		if err != nil {
			return nil, err
		}
		nextHistoryArchivalState, historyArchivalConfigChanged, err = currentHistoryArchivalState.getNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	currentVisibilityArchivalState := &ArchivalState{
		State: config.VisibilityArchivalState,
		URI:   config.VisibilityArchivalUri,
	}
	nextVisibilityArchivalState := currentVisibilityArchivalState
	visibilityArchivalConfigChanged := false
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if updateRequest.Config != nil && clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfig()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.VisibilityArchivalState, cfg.GetVisibilityArchivalUri(), clusterVisibilityArchivalConfig.GetNamespaceDefaultURI())
		if err != nil {
			return nil, err
		}
		nextVisibilityArchivalState, visibilityArchivalConfigChanged, err = currentVisibilityArchivalState.getNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	// whether active cluster is changed
	activeClusterChanged := false
	// whether anything other than active cluster is changed
	configurationChanged := false

	if updateRequest.UpdateInfo != nil {
		updatedInfo := updateRequest.UpdateInfo
		if updatedInfo.GetDescription() != "" {
			configurationChanged = true
			info.Description = updatedInfo.GetDescription()
		}
		if updatedInfo.GetOwnerEmail() != "" {
			configurationChanged = true
			info.Owner = updatedInfo.GetOwnerEmail()
		}
		if updatedInfo.Data != nil {
			configurationChanged = true
			// only do merging
			info.Data = d.mergeNamespaceData(info.Data, updatedInfo.Data)
		}
	}
	if updateRequest.Config != nil {
		updatedConfig := updateRequest.Config
		if updatedConfig.GetWorkflowExecutionRetentionTtl() != nil {
			configurationChanged = true
			config.Retention = updatedConfig.GetWorkflowExecutionRetentionTtl()
		}
		if historyArchivalConfigChanged {
			configurationChanged = true
			config.HistoryArchivalState = nextHistoryArchivalState.State
			config.HistoryArchivalUri = nextHistoryArchivalState.URI
		}
		if visibilityArchivalConfigChanged {
			configurationChanged = true
			config.VisibilityArchivalState = nextVisibilityArchivalState.State
			config.VisibilityArchivalUri = nextVisibilityArchivalState.URI
		}
		if updatedConfig.BadBinaries != nil {
			maxLength := d.maxBadBinaryCount(updateRequest.GetNamespace())
			// only do merging
			bb := d.mergeBadBinaries(config.BadBinaries.Binaries, updatedConfig.BadBinaries.Binaries, time.Now().UTC())
			config.BadBinaries = &bb
			if len(config.BadBinaries.Binaries) > maxLength {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Total resetBinaries cannot exceed the max limit: %v", maxLength))
			}
		}
	}

	if updateRequest.GetDeleteBadBinary() != "" {
		binChecksum := updateRequest.GetDeleteBadBinary()
		_, ok := config.BadBinaries.Binaries[binChecksum]
		if !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Bad binary checksum %v doesn't exists.", binChecksum))
		}
		configurationChanged = true
		delete(config.BadBinaries.Binaries, binChecksum)
	}

	if updateRequest.ReplicationConfig != nil {
		updateReplicationConfig := updateRequest.ReplicationConfig
		if len(updateReplicationConfig.Clusters) != 0 {
			configurationChanged = true
			var clustersNew []string
			for _, clusterConfig := range updateReplicationConfig.Clusters {
				clustersNew = append(clustersNew, clusterConfig.GetClusterName())
			}

			if err := d.namespaceAttrValidator.validateNamespaceReplicationConfigClustersDoesNotRemove(
				replicationConfig.Clusters,
				clustersNew,
			); err != nil {
				return nil, err
			}
			replicationConfig.Clusters = clustersNew
		}

		if updateReplicationConfig.GetActiveClusterName() != "" {
			activeClusterChanged = true
			replicationConfig.ActiveClusterName = updateReplicationConfig.GetActiveClusterName()
		}
	}

	if err := d.namespaceAttrValidator.validateNamespaceConfig(config); err != nil {
		return nil, err
	}
	if isGlobalNamespace {
		if err := d.namespaceAttrValidator.validateNamespaceReplicationConfigForGlobalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	} else {
		if err := d.namespaceAttrValidator.validateNamespaceReplicationConfigForLocalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	if configurationChanged && activeClusterChanged && isGlobalNamespace {
		return nil, errCannotDoNamespaceFailoverAndUpdate
	} else if configurationChanged || activeClusterChanged {
		if configurationChanged && isGlobalNamespace && !d.clusterMetadata.IsMasterCluster() {
			return nil, errNotMasterCluster
		}

		// set the versions
		if configurationChanged {
			configVersion++
		}
		if activeClusterChanged && isGlobalNamespace {
			failoverVersion = d.clusterMetadata.GetNextFailoverVersion(
				replicationConfig.ActiveClusterName,
				failoverVersion,
			)
			failoverNotificationVersion = notificationVersion
		}

		updateReq := &persistence.UpdateNamespaceRequest{
			Namespace: &persistencespb.NamespaceDetail{
				Info:                        info,
				Config:                      config,
				ReplicationConfig:           replicationConfig,
				ConfigVersion:               configVersion,
				FailoverVersion:             failoverVersion,
				FailoverNotificationVersion: failoverNotificationVersion,
			},
			NotificationVersion: notificationVersion,
		}
		err = d.metadataMgr.UpdateNamespace(updateReq)
		if err != nil {
			return nil, err
		}
	} else if isGlobalNamespace && !d.clusterMetadata.IsMasterCluster() {
		// although there is no attr updated, just prevent customer to use the non master cluster
		// for update namespace, ever (except if customer want to do a namespace failover)
		return nil, errNotMasterCluster
	}

	if isGlobalNamespace {
		err = d.namespaceReplicator.HandleTransmissionTask(enumsspb.NAMESPACE_OPERATION_UPDATE,
			info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
		if err != nil {
			return nil, err
		}
	}

	response := &workflowservice.UpdateNamespaceResponse{
		IsGlobalNamespace: isGlobalNamespace,
		FailoverVersion:   failoverVersion,
	}
	response.NamespaceInfo, response.Config, response.ReplicationConfig = d.createResponse(ctx, info, config, replicationConfig)

	d.logger.Info("Update namespace succeeded",
		tag.WorkflowNamespace(info.Name),
		tag.WorkflowNamespaceID(info.Id),
	)
	return response, nil
}

// DeprecateNamespace deprecates a namespace
func (d *HandlerImpl) DeprecateNamespace(
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
	metadata, err := d.metadataMgr.GetMetadata()
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetNamespace(&persistence.GetNamespaceRequest{Name: deprecateRequest.GetNamespace()})
	if err != nil {
		return nil, err
	}

	getResponse.Namespace.ConfigVersion = getResponse.Namespace.ConfigVersion + 1
	getResponse.Namespace.Info.State = enumspb.NAMESPACE_STATE_DEPRECATED
	updateReq := &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:                        getResponse.Namespace.Info,
			Config:                      getResponse.Namespace.Config,
			ReplicationConfig:           getResponse.Namespace.ReplicationConfig,
			ConfigVersion:               getResponse.Namespace.ConfigVersion,
			FailoverVersion:             getResponse.Namespace.FailoverVersion,
			FailoverNotificationVersion: getResponse.Namespace.FailoverNotificationVersion,
		},
		NotificationVersion: notificationVersion,
	}
	err = d.metadataMgr.UpdateNamespace(updateReq)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *HandlerImpl) createResponse(
	ctx context.Context,
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	replicationConfig *persistencespb.NamespaceReplicationConfig,
) (*namespacepb.NamespaceInfo, *namespacepb.NamespaceConfig, *replicationpb.NamespaceReplicationConfig) {

	infoResult := &namespacepb.NamespaceInfo{
		Name:        info.Name,
		State:       info.State,
		Description: info.Description,
		OwnerEmail:  info.Owner,
		Data:        info.Data,
		Id:          info.Id,
	}

	configResult := &namespacepb.NamespaceConfig{
		WorkflowExecutionRetentionTtl: config.Retention,
		HistoryArchivalState:          config.HistoryArchivalState,
		HistoryArchivalUri:            config.HistoryArchivalUri,
		VisibilityArchivalState:       config.VisibilityArchivalState,
		VisibilityArchivalUri:         config.VisibilityArchivalUri,
		BadBinaries:                   config.BadBinaries,
	}

	var clusters []*replicationpb.ClusterReplicationConfig
	for _, cluster := range replicationConfig.Clusters {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster,
		})
	}
	replicationConfigResult := &replicationpb.NamespaceReplicationConfig{
		ActiveClusterName: replicationConfig.ActiveClusterName,
		Clusters:          clusters,
	}

	return infoResult, configResult, replicationConfigResult
}

func (d *HandlerImpl) mergeBadBinaries(
	old map[string]*namespacepb.BadBinaryInfo,
	new map[string]*namespacepb.BadBinaryInfo,
	createTime time.Time,
) namespacepb.BadBinaries {

	if old == nil {
		old = map[string]*namespacepb.BadBinaryInfo{}
	}
	for k, v := range new {
		v.CreateTime = &createTime
		old[k] = v
	}
	return namespacepb.BadBinaries{
		Binaries: old,
	}
}

func (d *HandlerImpl) mergeNamespaceData(
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

func (d *HandlerImpl) toArchivalRegisterEvent(
	state enumspb.ArchivalState,
	URI string,
	defaultState enumspb.ArchivalState,
	defaultURI string,
) (*ArchivalEvent, error) {

	event := &ArchivalEvent{
		state:      state,
		URI:        URI,
		defaultURI: defaultURI,
	}
	if event.state == enumspb.ARCHIVAL_STATE_UNSPECIFIED {
		event.state = defaultState
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *HandlerImpl) toArchivalUpdateEvent(
	state enumspb.ArchivalState,
	URI string,
	defaultURI string,
) (*ArchivalEvent, error) {

	event := &ArchivalEvent{
		state:      state,
		URI:        URI,
		defaultURI: defaultURI,
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *HandlerImpl) validateHistoryArchivalURI(URIString string) error {
	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return err
	}

	archiver, err := d.archiverProvider.GetHistoryArchiver(URI.Scheme(), common.FrontendServiceName)
	if err != nil {
		return err
	}

	return archiver.ValidateURI(URI)
}

func (d *HandlerImpl) validateVisibilityArchivalURI(URIString string) error {
	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return err
	}

	archiver, err := d.archiverProvider.GetVisibilityArchiver(URI.Scheme(), common.FrontendServiceName)
	if err != nil {
		return err
	}

	return archiver.ValidateURI(URI)
}
