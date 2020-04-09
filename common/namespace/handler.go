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

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	replicationpb "go.temporal.io/temporal-proto/replication"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
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
	minRetentionDays int,
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
		namespaceAttrValidator: newAttrValidator(clusterMetadata, int32(minRetentionDays)),
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
	_, err := d.metadataMgr.GetNamespace(&persistence.GetNamespaceRequest{Name: registerRequest.GetName()})
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
	var clusters []*persistence.ClusterReplicationConfig
	for _, clusterConfig := range registerRequest.Clusters {
		clusterName := clusterConfig.GetClusterName()
		clusters = append(clusters, &persistence.ClusterReplicationConfig{ClusterName: clusterName})
	}
	clusters = persistence.GetOrUseDefaultClusters(activeClusterName, clusters)

	currentHistoryArchivalState := neverEnabledState()
	nextHistoryArchivalState := currentHistoryArchivalState
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(
			registerRequest.HistoryArchivalStatus,
			registerRequest.GetHistoryArchivalURI(),
			clusterHistoryArchivalConfig.GetNamespaceDefaultStatus(),
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
			registerRequest.VisibilityArchivalStatus,
			registerRequest.GetVisibilityArchivalURI(),
			clusterVisibilityArchivalConfig.GetNamespaceDefaultStatus(),
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

	info := &persistence.NamespaceInfo{
		ID:          uuid.New(),
		Name:        registerRequest.GetName(),
		Status:      persistence.NamespaceStatusRegistered,
		OwnerEmail:  registerRequest.GetOwnerEmail(),
		Description: registerRequest.GetDescription(),
		Data:        registerRequest.Data,
	}
	config := &persistence.NamespaceConfig{
		Retention:                registerRequest.GetWorkflowExecutionRetentionPeriodInDays(),
		EmitMetric:               registerRequest.GetEmitMetric(),
		HistoryArchivalStatus:    nextHistoryArchivalState.Status,
		HistoryArchivalURI:       nextHistoryArchivalState.URI,
		VisibilityArchivalStatus: nextVisibilityArchivalState.Status,
		VisibilityArchivalURI:    nextVisibilityArchivalState.URI,
		BadBinaries:              namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}
	replicationConfig := &persistence.NamespaceReplicationConfig{
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
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,
		IsGlobalNamespace: isGlobalNamespace,
		ConfigVersion:     0,
		FailoverVersion:   failoverVersion,
	}

	namespaceResponse, err := d.metadataMgr.CreateNamespace(namespaceRequest)
	if err != nil {
		return nil, err
	}

	if namespaceRequest.IsGlobalNamespace {
		err = d.namespaceReplicator.HandleTransmissionTask(
			replicationgenpb.NamespaceOperation_Create,
			namespaceRequest.Info,
			namespaceRequest.Config,
			namespaceRequest.ReplicationConfig,
			namespaceRequest.ConfigVersion,
			namespaceRequest.FailoverVersion,
			namespaceRequest.IsGlobalNamespace,
		)
		if err != nil {
			return nil, err
		}
	}

	d.logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(registerRequest.GetName()),
		tag.WorkflowNamespaceID(namespaceResponse.ID),
	)

	return nil, nil
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
			FailoverVersion:   namespace.FailoverVersion,
		}
		desc.NamespaceInfo, desc.Configuration, desc.ReplicationConfiguration = d.createResponse(ctx, namespace.Info, namespace.Config, namespace.ReplicationConfig)
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
		Name: describeRequest.GetName(),
		ID:   describeRequest.GetId(),
	}
	resp, err := d.metadataMgr.GetNamespace(req)
	if err != nil {
		return nil, err
	}

	response := &workflowservice.DescribeNamespaceResponse{
		IsGlobalNamespace: resp.IsGlobalNamespace,
		FailoverVersion:   resp.FailoverVersion,
	}
	response.NamespaceInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(ctx, resp.Info, resp.Config, resp.ReplicationConfig)
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
	getResponse, err := d.metadataMgr.GetNamespace(&persistence.GetNamespaceRequest{Name: updateRequest.GetName()})
	if err != nil {
		return nil, err
	}

	info := getResponse.Info
	config := getResponse.Config
	replicationConfig := getResponse.ReplicationConfig
	configVersion := getResponse.ConfigVersion
	failoverVersion := getResponse.FailoverVersion
	failoverNotificationVersion := getResponse.FailoverNotificationVersion
	isGlobalNamespace := getResponse.IsGlobalNamespace

	currentHistoryArchivalState := &ArchivalState{
		Status: config.HistoryArchivalStatus,
		URI:    config.HistoryArchivalURI,
	}
	nextHistoryArchivalState := currentHistoryArchivalState
	historyArchivalConfigChanged := false
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if updateRequest.Configuration != nil && clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfiguration()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.HistoryArchivalStatus, cfg.GetHistoryArchivalURI(), clusterHistoryArchivalConfig.GetNamespaceDefaultURI())
		if err != nil {
			return nil, err
		}
		nextHistoryArchivalState, historyArchivalConfigChanged, err = currentHistoryArchivalState.getNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	currentVisibilityArchivalState := &ArchivalState{
		Status: config.VisibilityArchivalStatus,
		URI:    config.VisibilityArchivalURI,
	}
	nextVisibilityArchivalState := currentVisibilityArchivalState
	visibilityArchivalConfigChanged := false
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if updateRequest.Configuration != nil && clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfiguration()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.VisibilityArchivalStatus, cfg.GetVisibilityArchivalURI(), clusterVisibilityArchivalConfig.GetNamespaceDefaultURI())
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

	if updateRequest.UpdatedInfo != nil {
		updatedInfo := updateRequest.UpdatedInfo
		if updatedInfo.GetDescription() != "" {
			configurationChanged = true
			info.Description = updatedInfo.GetDescription()
		}
		if updatedInfo.GetOwnerEmail() != "" {
			configurationChanged = true
			info.OwnerEmail = updatedInfo.GetOwnerEmail()
		}
		if updatedInfo.Data != nil {
			configurationChanged = true
			// only do merging
			info.Data = d.mergeNamespaceData(info.Data, updatedInfo.Data)
		}
	}
	if updateRequest.Configuration != nil {
		updatedConfig := updateRequest.Configuration
		if updatedConfig.EmitMetric != nil {
			configurationChanged = true
			config.EmitMetric = updatedConfig.GetEmitMetric().GetValue()
		}
		if updatedConfig.GetWorkflowExecutionRetentionPeriodInDays() != 0 {
			configurationChanged = true
			config.Retention = updatedConfig.GetWorkflowExecutionRetentionPeriodInDays()
		}
		if historyArchivalConfigChanged {
			configurationChanged = true
			config.HistoryArchivalStatus = nextHistoryArchivalState.Status
			config.HistoryArchivalURI = nextHistoryArchivalState.URI
		}
		if visibilityArchivalConfigChanged {
			configurationChanged = true
			config.VisibilityArchivalStatus = nextVisibilityArchivalState.Status
			config.VisibilityArchivalURI = nextVisibilityArchivalState.URI
		}
		if updatedConfig.BadBinaries != nil {
			maxLength := d.maxBadBinaryCount(updateRequest.GetName())
			// only do merging
			config.BadBinaries = d.mergeBadBinaries(config.BadBinaries.Binaries, updatedConfig.BadBinaries.Binaries, time.Now().UnixNano())
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

	if updateRequest.ReplicationConfiguration != nil {
		updateReplicationConfig := updateRequest.ReplicationConfiguration
		if len(updateReplicationConfig.Clusters) != 0 {
			configurationChanged = true
			var clustersNew []*persistence.ClusterReplicationConfig
			for _, clusterConfig := range updateReplicationConfig.Clusters {
				clustersNew = append(clustersNew, &persistence.ClusterReplicationConfig{
					ClusterName: clusterConfig.GetClusterName(),
				})
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
			Info:                        info,
			Config:                      config,
			ReplicationConfig:           replicationConfig,
			ConfigVersion:               configVersion,
			FailoverVersion:             failoverVersion,
			FailoverNotificationVersion: failoverNotificationVersion,
			NotificationVersion:         notificationVersion,
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
		err = d.namespaceReplicator.HandleTransmissionTask(replicationgenpb.NamespaceOperation_Update,
			info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
		if err != nil {
			return nil, err
		}
	}

	response := &workflowservice.UpdateNamespaceResponse{
		IsGlobalNamespace: isGlobalNamespace,
		FailoverVersion:   failoverVersion,
	}
	response.NamespaceInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(ctx, info, config, replicationConfig)

	d.logger.Info("Update namespace succeeded",
		tag.WorkflowNamespace(info.Name),
		tag.WorkflowNamespaceID(info.ID),
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
	getResponse, err := d.metadataMgr.GetNamespace(&persistence.GetNamespaceRequest{Name: deprecateRequest.GetName()})
	if err != nil {
		return nil, err
	}

	getResponse.ConfigVersion = getResponse.ConfigVersion + 1
	getResponse.Info.Status = persistence.NamespaceStatusDeprecated
	updateReq := &persistence.UpdateNamespaceRequest{
		Info:                        getResponse.Info,
		Config:                      getResponse.Config,
		ReplicationConfig:           getResponse.ReplicationConfig,
		ConfigVersion:               getResponse.ConfigVersion,
		FailoverVersion:             getResponse.FailoverVersion,
		FailoverNotificationVersion: getResponse.FailoverNotificationVersion,
		NotificationVersion:         notificationVersion,
	}
	err = d.metadataMgr.UpdateNamespace(updateReq)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *HandlerImpl) createResponse(
	ctx context.Context,
	info *persistence.NamespaceInfo,
	config *persistence.NamespaceConfig,
	replicationConfig *persistence.NamespaceReplicationConfig,
) (*namespacepb.NamespaceInfo, *namespacepb.NamespaceConfiguration, *replicationpb.NamespaceReplicationConfiguration) {

	infoResult := &namespacepb.NamespaceInfo{
		Name:        info.Name,
		Status:      getNamespaceStatus(info),
		Description: info.Description,
		OwnerEmail:  info.OwnerEmail,
		Data:        info.Data,
		Id:          info.ID,
	}

	configResult := &namespacepb.NamespaceConfiguration{
		EmitMetric:                             &types.BoolValue{Value: config.EmitMetric},
		WorkflowExecutionRetentionPeriodInDays: config.Retention,
		HistoryArchivalStatus:                  config.HistoryArchivalStatus,
		HistoryArchivalURI:                     config.HistoryArchivalURI,
		VisibilityArchivalStatus:               config.VisibilityArchivalStatus,
		VisibilityArchivalURI:                  config.VisibilityArchivalURI,
		BadBinaries:                            &config.BadBinaries,
	}

	var clusters []*replicationpb.ClusterReplicationConfiguration
	for _, cluster := range replicationConfig.Clusters {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfiguration{
			ClusterName: cluster.ClusterName,
		})
	}

	replicationConfigResult := &replicationpb.NamespaceReplicationConfiguration{
		ActiveClusterName: replicationConfig.ActiveClusterName,
		Clusters:          clusters,
	}

	return infoResult, configResult, replicationConfigResult
}

func (d *HandlerImpl) mergeBadBinaries(
	old map[string]*namespacepb.BadBinaryInfo,
	new map[string]*namespacepb.BadBinaryInfo,
	createTimeNano int64,
) namespacepb.BadBinaries {

	if old == nil {
		old = map[string]*namespacepb.BadBinaryInfo{}
	}
	for k, v := range new {
		v.CreatedTimeNano = createTimeNano
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
	status namespacepb.ArchivalStatus,
	URI string,
	defaultStatus namespacepb.ArchivalStatus,
	defaultURI string,
) (*ArchivalEvent, error) {

	event := &ArchivalEvent{
		status:     status,
		URI:        URI,
		defaultURI: defaultURI,
	}
	if event.status == namespacepb.ArchivalStatus_Default {
		event.status = defaultStatus
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *HandlerImpl) toArchivalUpdateEvent(
	status namespacepb.ArchivalStatus,
	URI string,
	defaultURI string,
) (*ArchivalEvent, error) {

	event := &ArchivalEvent{
		status:     status,
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

func getNamespaceStatus(info *persistence.NamespaceInfo) namespacepb.NamespaceStatus {
	switch info.Status {
	case persistence.NamespaceStatusRegistered:
		return namespacepb.NamespaceStatus_Registered
	case persistence.NamespaceStatusDeprecated:
		return namespacepb.NamespaceStatus_Deprecated
	case persistence.NamespaceStatusDeleted:
		return namespacepb.NamespaceStatus_Deleted
	}

	// TODO: panic, log, ...?
	return namespacepb.NamespaceStatus_Registered
}
