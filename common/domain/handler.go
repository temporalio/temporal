// Copyright (c) 2017 Uber Technologies, Inc.
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

package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

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
	// Handler is the domain operation handler
	Handler interface {
		DeprecateDomain(
			ctx context.Context,
			deprecateRequest *workflowservice.DeprecateDomainRequest,
		) (*workflowservice.DeprecateDomainResponse, error)
		DescribeDomain(
			ctx context.Context,
			describeRequest *workflowservice.DescribeDomainRequest,
		) (*workflowservice.DescribeDomainResponse, error)
		ListDomains(
			ctx context.Context,
			listRequest *workflowservice.ListDomainsRequest,
		) (*workflowservice.ListDomainsResponse, error)
		RegisterDomain(
			ctx context.Context,
			registerRequest *workflowservice.RegisterDomainRequest,
		) (*workflowservice.RegisterDomainResponse, error)
		UpdateDomain(
			ctx context.Context,
			updateRequest *workflowservice.UpdateDomainRequest,
		) (*workflowservice.UpdateDomainResponse, error)
	}

	// HandlerImpl is the domain operation handler implementation
	HandlerImpl struct {
		maxBadBinaryCount   dynamicconfig.IntPropertyFnWithDomainFilter
		logger              log.Logger
		metadataMgr         persistence.MetadataManager
		clusterMetadata     cluster.Metadata
		domainReplicator    Replicator
		domainAttrValidator *AttrValidatorImpl
		archivalMetadata    archiver.ArchivalMetadata
		archiverProvider    provider.ArchiverProvider
	}
)

var _ Handler = (*HandlerImpl)(nil)

// NewHandler create a new domain handler
func NewHandler(
	minRetentionDays int,
	maxBadBinaryCount dynamicconfig.IntPropertyFnWithDomainFilter,
	logger log.Logger,
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	domainReplicator Replicator,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
) *HandlerImpl {
	return &HandlerImpl{
		maxBadBinaryCount:   maxBadBinaryCount,
		logger:              logger,
		metadataMgr:         metadataMgr,
		clusterMetadata:     clusterMetadata,
		domainReplicator:    domainReplicator,
		domainAttrValidator: newAttrValidator(clusterMetadata, int32(minRetentionDays)),
		archivalMetadata:    archivalMetadata,
		archiverProvider:    archiverProvider,
	}
}

// RegisterDomain register a new domain
func (d *HandlerImpl) RegisterDomain(
	_ context.Context,
	registerRequest *workflowservice.RegisterDomainRequest,
) (*workflowservice.RegisterDomainResponse, error) {

	if !d.clusterMetadata.IsGlobalDomainEnabled() {
		if registerRequest.GetIsGlobalDomain() {
			return nil, serviceerror.NewInvalidArgument("Cannot register global domain when not enabled")
		}

		registerRequest.IsGlobalDomain = false
	} else {
		// cluster global domain enabled
		if !d.clusterMetadata.IsMasterCluster() && registerRequest.GetIsGlobalDomain() {
			return nil, errNotMasterCluster
		}
	}

	// first check if the name is already registered as the local domain
	_, err := d.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: registerRequest.GetName()})
	switch err.(type) {
	case nil:
		// domain already exists, cannot proceed
		return nil, serviceerror.NewDomainAlreadyExists("Domain already exists.")
	case *serviceerror.NotFound:
		// domain does not exists, proceeds
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
			clusterHistoryArchivalConfig.GetDomainDefaultStatus(),
			clusterHistoryArchivalConfig.GetDomainDefaultURI(),
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
			clusterVisibilityArchivalConfig.GetDomainDefaultStatus(),
			clusterVisibilityArchivalConfig.GetDomainDefaultURI(),
		)
		if err != nil {
			return nil, err
		}

		nextVisibilityArchivalState, _, err = currentVisibilityArchivalState.getNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	info := &persistence.DomainInfo{
		ID:          uuid.New(),
		Name:        registerRequest.GetName(),
		Status:      persistence.DomainStatusRegistered,
		OwnerEmail:  registerRequest.GetOwnerEmail(),
		Description: registerRequest.GetDescription(),
		Data:        registerRequest.Data,
	}
	config := &persistence.DomainConfig{
		Retention:                registerRequest.GetWorkflowExecutionRetentionPeriodInDays(),
		EmitMetric:               registerRequest.GetEmitMetric(),
		HistoryArchivalStatus:    nextHistoryArchivalState.Status,
		HistoryArchivalURI:       nextHistoryArchivalState.URI,
		VisibilityArchivalStatus: nextVisibilityArchivalState.Status,
		VisibilityArchivalURI:    nextVisibilityArchivalState.URI,
		BadBinaries:              commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
	}
	replicationConfig := &persistence.DomainReplicationConfig{
		ActiveClusterName: activeClusterName,
		Clusters:          clusters,
	}
	isGlobalDomain := registerRequest.GetIsGlobalDomain()

	if err := d.domainAttrValidator.validateDomainConfig(config); err != nil {
		return nil, err
	}
	if isGlobalDomain {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForGlobalDomain(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	} else {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForLocalDomain(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	failoverVersion := common.EmptyVersion
	if registerRequest.GetIsGlobalDomain() {
		failoverVersion = d.clusterMetadata.GetNextFailoverVersion(activeClusterName, 0)
	}

	domainRequest := &persistence.CreateDomainRequest{
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,
		IsGlobalDomain:    isGlobalDomain,
		ConfigVersion:     0,
		FailoverVersion:   failoverVersion,
	}

	domainResponse, err := d.metadataMgr.CreateDomain(domainRequest)
	if err != nil {
		return nil, err
	}

	if domainRequest.IsGlobalDomain {
		err = d.domainReplicator.HandleTransmissionTask(
			enums.DomainOperationCreate,
			domainRequest.Info,
			domainRequest.Config,
			domainRequest.ReplicationConfig,
			domainRequest.ConfigVersion,
			domainRequest.FailoverVersion,
			domainRequest.IsGlobalDomain,
		)
		if err != nil {
			return nil, err
		}
	}

	d.logger.Info("Register domain succeeded",
		tag.WorkflowDomainName(registerRequest.GetName()),
		tag.WorkflowDomainID(domainResponse.ID),
	)

	return nil, nil
}

// ListDomains list all domains
func (d *HandlerImpl) ListDomains(
	ctx context.Context,
	listRequest *workflowservice.ListDomainsRequest,
) (*workflowservice.ListDomainsResponse, error) {

	pageSize := 100
	if listRequest.GetPageSize() != 0 {
		pageSize = int(listRequest.GetPageSize())
	}

	resp, err := d.metadataMgr.ListDomains(&persistence.ListDomainsRequest{
		PageSize:      pageSize,
		NextPageToken: listRequest.NextPageToken,
	})

	if err != nil {
		return nil, err
	}

	var domains []*workflowservice.DescribeDomainResponse
	for _, domain := range resp.Domains {
		desc := &workflowservice.DescribeDomainResponse{
			IsGlobalDomain:  domain.IsGlobalDomain,
			FailoverVersion: domain.FailoverVersion,
		}
		desc.DomainInfo, desc.Configuration, desc.ReplicationConfiguration = d.createResponse(ctx, domain.Info, domain.Config, domain.ReplicationConfig)
		domains = append(domains, desc)
	}

	response := &workflowservice.ListDomainsResponse{
		Domains:       domains,
		NextPageToken: resp.NextPageToken,
	}

	return response, nil
}

// DescribeDomain describe the domain
func (d *HandlerImpl) DescribeDomain(
	ctx context.Context,
	describeRequest *workflowservice.DescribeDomainRequest,
) (*workflowservice.DescribeDomainResponse, error) {

	// TODO, we should migrate the non global domain to new table, see #773
	req := &persistence.GetDomainRequest{
		Name: describeRequest.GetName(),
		ID:   describeRequest.GetUuid(),
	}
	resp, err := d.metadataMgr.GetDomain(req)
	if err != nil {
		return nil, err
	}

	response := &workflowservice.DescribeDomainResponse{
		IsGlobalDomain:  resp.IsGlobalDomain,
		FailoverVersion: resp.FailoverVersion,
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(ctx, resp.Info, resp.Config, resp.ReplicationConfig)
	return response, nil
}

// UpdateDomain update the domain
func (d *HandlerImpl) UpdateDomain(
	ctx context.Context,
	updateRequest *workflowservice.UpdateDomainRequest,
) (*workflowservice.UpdateDomainResponse, error) {

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 domain table
	// and since we do not know which table will return the domain afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata()
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: updateRequest.GetName()})
	if err != nil {
		return nil, err
	}

	info := getResponse.Info
	config := getResponse.Config
	replicationConfig := getResponse.ReplicationConfig
	configVersion := getResponse.ConfigVersion
	failoverVersion := getResponse.FailoverVersion
	failoverNotificationVersion := getResponse.FailoverNotificationVersion
	isGlobalDomain := getResponse.IsGlobalDomain

	currentHistoryArchivalState := &ArchivalState{
		Status: config.HistoryArchivalStatus,
		URI:    config.HistoryArchivalURI,
	}
	nextHistoryArchivalState := currentHistoryArchivalState
	historyArchivalConfigChanged := false
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if updateRequest.Configuration != nil && clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfiguration()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.HistoryArchivalStatus, cfg.GetHistoryArchivalURI(), clusterHistoryArchivalConfig.GetDomainDefaultURI())
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
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.VisibilityArchivalStatus, cfg.GetVisibilityArchivalURI(), clusterVisibilityArchivalConfig.GetDomainDefaultURI())
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
			info.Data = d.mergeDomainData(info.Data, updatedInfo.Data)
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

			if err := d.domainAttrValidator.validateDomainReplicationConfigClustersDoesNotRemove(
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

	if err := d.domainAttrValidator.validateDomainConfig(config); err != nil {
		return nil, err
	}
	if isGlobalDomain {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForGlobalDomain(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	} else {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForLocalDomain(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	if configurationChanged && activeClusterChanged && isGlobalDomain {
		return nil, errCannotDoDomainFailoverAndUpdate
	} else if configurationChanged || activeClusterChanged {
		if configurationChanged && isGlobalDomain && !d.clusterMetadata.IsMasterCluster() {
			return nil, errNotMasterCluster
		}

		// set the versions
		if configurationChanged {
			configVersion++
		}
		if activeClusterChanged && isGlobalDomain {
			failoverVersion = d.clusterMetadata.GetNextFailoverVersion(
				replicationConfig.ActiveClusterName,
				failoverVersion,
			)
			failoverNotificationVersion = notificationVersion
		}

		updateReq := &persistence.UpdateDomainRequest{
			Info:                        info,
			Config:                      config,
			ReplicationConfig:           replicationConfig,
			ConfigVersion:               configVersion,
			FailoverVersion:             failoverVersion,
			FailoverNotificationVersion: failoverNotificationVersion,
			NotificationVersion:         notificationVersion,
		}
		err = d.metadataMgr.UpdateDomain(updateReq)
		if err != nil {
			return nil, err
		}
	} else if isGlobalDomain && !d.clusterMetadata.IsMasterCluster() {
		// although there is no attr updated, just prevent customer to use the non master cluster
		// for update domain, ever (except if customer want to do a domain failover)
		return nil, errNotMasterCluster
	}

	if isGlobalDomain {
		err = d.domainReplicator.HandleTransmissionTask(enums.DomainOperationUpdate,
			info, config, replicationConfig, configVersion, failoverVersion, isGlobalDomain)
		if err != nil {
			return nil, err
		}
	}

	response := &workflowservice.UpdateDomainResponse{
		IsGlobalDomain:  isGlobalDomain,
		FailoverVersion: failoverVersion,
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(ctx, info, config, replicationConfig)

	d.logger.Info("Update domain succeeded",
		tag.WorkflowDomainName(info.Name),
		tag.WorkflowDomainID(info.ID),
	)
	return response, nil
}

// DeprecateDomain deprecates a domain
func (d *HandlerImpl) DeprecateDomain(
	ctx context.Context,
	deprecateRequest *workflowservice.DeprecateDomainRequest,
) (*workflowservice.DeprecateDomainResponse, error) {

	clusterMetadata := d.clusterMetadata
	// TODO remove the IsGlobalDomainEnabled check once cross DC is public
	if clusterMetadata.IsGlobalDomainEnabled() && !clusterMetadata.IsMasterCluster() {
		return nil, errNotMasterCluster
	}

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 domain table
	// and since we do not know which table will return the domain afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata()
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: deprecateRequest.GetName()})
	if err != nil {
		return nil, err
	}

	getResponse.ConfigVersion = getResponse.ConfigVersion + 1
	getResponse.Info.Status = persistence.DomainStatusDeprecated
	updateReq := &persistence.UpdateDomainRequest{
		Info:                        getResponse.Info,
		Config:                      getResponse.Config,
		ReplicationConfig:           getResponse.ReplicationConfig,
		ConfigVersion:               getResponse.ConfigVersion,
		FailoverVersion:             getResponse.FailoverVersion,
		FailoverNotificationVersion: getResponse.FailoverNotificationVersion,
		NotificationVersion:         notificationVersion,
	}
	err = d.metadataMgr.UpdateDomain(updateReq)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *HandlerImpl) createResponse(
	ctx context.Context,
	info *persistence.DomainInfo,
	config *persistence.DomainConfig,
	replicationConfig *persistence.DomainReplicationConfig,
) (*commonproto.DomainInfo, *commonproto.DomainConfiguration, *commonproto.DomainReplicationConfiguration) {

	infoResult := &commonproto.DomainInfo{
		Name:        info.Name,
		Status:      getDomainStatus(info),
		Description: info.Description,
		OwnerEmail:  info.OwnerEmail,
		Data:        info.Data,
		Uuid:        info.ID,
	}

	configResult := &commonproto.DomainConfiguration{
		EmitMetric:                             &types.BoolValue{Value: config.EmitMetric},
		WorkflowExecutionRetentionPeriodInDays: config.Retention,
		HistoryArchivalStatus:                  config.HistoryArchivalStatus,
		HistoryArchivalURI:                     config.HistoryArchivalURI,
		VisibilityArchivalStatus:               config.VisibilityArchivalStatus,
		VisibilityArchivalURI:                  config.VisibilityArchivalURI,
		BadBinaries:                            &config.BadBinaries,
	}

	var clusters []*commonproto.ClusterReplicationConfiguration
	for _, cluster := range replicationConfig.Clusters {
		clusters = append(clusters, &commonproto.ClusterReplicationConfiguration{
			ClusterName: cluster.ClusterName,
		})
	}

	replicationConfigResult := &commonproto.DomainReplicationConfiguration{
		ActiveClusterName: replicationConfig.ActiveClusterName,
		Clusters:          clusters,
	}

	return infoResult, configResult, replicationConfigResult
}

func (d *HandlerImpl) mergeBadBinaries(
	old map[string]*commonproto.BadBinaryInfo,
	new map[string]*commonproto.BadBinaryInfo,
	createTimeNano int64,
) commonproto.BadBinaries {

	if old == nil {
		old = map[string]*commonproto.BadBinaryInfo{}
	}
	for k, v := range new {
		v.CreatedTimeNano = createTimeNano
		old[k] = v
	}
	return commonproto.BadBinaries{
		Binaries: old,
	}
}

func (d *HandlerImpl) mergeDomainData(
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
	status enums.ArchivalStatus,
	URI string,
	defaultStatus enums.ArchivalStatus,
	defaultURI string,
) (*ArchivalEvent, error) {

	event := &ArchivalEvent{
		status:     status,
		URI:        URI,
		defaultURI: defaultURI,
	}
	if event.status == enums.ArchivalStatusDefault {
		event.status = defaultStatus
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *HandlerImpl) toArchivalUpdateEvent(
	status enums.ArchivalStatus,
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

func getDomainStatus(info *persistence.DomainInfo) enums.DomainStatus {
	switch info.Status {
	case persistence.DomainStatusRegistered:
		return enums.DomainStatusRegistered
	case persistence.DomainStatusDeprecated:
		return enums.DomainStatusDeprecated
	case persistence.DomainStatusDeleted:
		return enums.DomainStatusDeleted
	}

	// TODO: panic, log, ...?
	return enums.DomainStatusRegistered
}
