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

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// Handler is the domain operation handler
	Handler interface {
		DeprecateDomain(
			ctx context.Context,
			deprecateRequest *shared.DeprecateDomainRequest,
		) error
		DescribeDomain(
			ctx context.Context,
			describeRequest *shared.DescribeDomainRequest,
		) (*shared.DescribeDomainResponse, error)
		ListDomains(
			ctx context.Context,
			listRequest *shared.ListDomainsRequest,
		) (*shared.ListDomainsResponse, error)
		RegisterDomain(
			ctx context.Context,
			registerRequest *shared.RegisterDomainRequest,
		) error
		UpdateDomain(
			ctx context.Context,
			updateRequest *shared.UpdateDomainRequest,
		) (*shared.UpdateDomainResponse, error)
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
	ctx context.Context,
	registerRequest *shared.RegisterDomainRequest,
) error {

	if !d.clusterMetadata.IsGlobalDomainEnabled() {
		if registerRequest.GetIsGlobalDomain() {
			return &shared.BadRequestError{Message: "Cannot register global domain when not enabled"}
		}

		registerRequest.IsGlobalDomain = common.BoolPtr(false)
	} else {
		// cluster global domain enabled
		if registerRequest.IsGlobalDomain == nil {
			return &shared.BadRequestError{Message: "Must specify whether domain is a global domain"}
		}
		if !d.clusterMetadata.IsMasterCluster() && registerRequest.GetIsGlobalDomain() {
			return errNotMasterCluster
		}
	}

	// first check if the name is already registered as the local domain
	_, err := d.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: registerRequest.GetName()})
	switch err.(type) {
	case nil:
		// domain already exists, cannot proceed
		return &shared.DomainAlreadyExistsError{Message: "Domain already exists."}
	case *shared.EntityNotExistsError:
		// domain does not exists, proceeds
	default:
		// other err
		return err
	}

	activeClusterName := d.clusterMetadata.GetCurrentClusterName()
	// input validation on cluster names
	if registerRequest.ActiveClusterName != nil {
		activeClusterName = registerRequest.GetActiveClusterName()
	}
	clusters := []*persistence.ClusterReplicationConfig{}
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
			return err
		}

		nextHistoryArchivalState, _, err = currentHistoryArchivalState.getNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return err
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
			return err
		}

		nextVisibilityArchivalState, _, err = currentVisibilityArchivalState.getNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return err
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
		BadBinaries:              shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
	}
	replicationConfig := &persistence.DomainReplicationConfig{
		ActiveClusterName: activeClusterName,
		Clusters:          clusters,
	}
	isGlobalDomain := registerRequest.GetIsGlobalDomain()

	if err := d.domainAttrValidator.validateDomainConfig(config); err != nil {
		return err
	}
	if isGlobalDomain {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForGlobalDomain(
			replicationConfig,
		); err != nil {
			return err
		}
	} else {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForLocalDomain(
			replicationConfig,
		); err != nil {
			return err
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
		return err
	}

	if domainRequest.IsGlobalDomain {
		err = d.domainReplicator.HandleTransmissionTask(
			replicator.DomainOperationCreate,
			domainRequest.Info,
			domainRequest.Config,
			domainRequest.ReplicationConfig,
			domainRequest.ConfigVersion,
			domainRequest.FailoverVersion,
			domainRequest.IsGlobalDomain,
		)
		if err != nil {
			return err
		}
	}

	d.logger.Info("Register domain succeeded",
		tag.WorkflowDomainName(registerRequest.GetName()),
		tag.WorkflowDomainID(domainResponse.ID),
	)

	return nil
}

// ListDomains list all domains
func (d *HandlerImpl) ListDomains(
	ctx context.Context,
	listRequest *shared.ListDomainsRequest,
) (*shared.ListDomainsResponse, error) {

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

	domains := []*shared.DescribeDomainResponse{}
	for _, domain := range resp.Domains {
		desc := &shared.DescribeDomainResponse{
			IsGlobalDomain:  common.BoolPtr(domain.IsGlobalDomain),
			FailoverVersion: common.Int64Ptr(domain.FailoverVersion),
		}
		desc.DomainInfo, desc.Configuration, desc.ReplicationConfiguration = d.createResponse(domain.Info, domain.Config, domain.ReplicationConfig)
		domains = append(domains, desc)
	}

	response := &shared.ListDomainsResponse{
		Domains:       domains,
		NextPageToken: resp.NextPageToken,
	}

	return response, nil
}

// DescribeDomain describe the domain
func (d *HandlerImpl) DescribeDomain(
	ctx context.Context,
	describeRequest *shared.DescribeDomainRequest,
) (*shared.DescribeDomainResponse, error) {

	// TODO, we should migrate the non global domain to new table, see #773
	req := &persistence.GetDomainRequest{
		Name: describeRequest.GetName(),
		ID:   describeRequest.GetUUID(),
	}
	resp, err := d.metadataMgr.GetDomain(req)
	if err != nil {
		return nil, err
	}

	response := &shared.DescribeDomainResponse{
		IsGlobalDomain:  common.BoolPtr(resp.IsGlobalDomain),
		FailoverVersion: common.Int64Ptr(resp.FailoverVersion),
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(resp.Info, resp.Config, resp.ReplicationConfig)
	return response, nil
}

// UpdateDomain update the domain
func (d *HandlerImpl) UpdateDomain(
	ctx context.Context,
	updateRequest *shared.UpdateDomainRequest,
) (*shared.UpdateDomainResponse, error) {

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
	gracefulFailoverEndTime := getResponse.FailoverEndTime
	currentActiveCluster := replicationConfig.ActiveClusterName

	// whether history archival config changed
	historyArchivalConfigChanged := false
	// whether visibility archival config changed
	visibilityArchivalConfigChanged := false
	// whether active cluster is changed
	activeClusterChanged := false
	// whether anything other than active cluster is changed
	configurationChanged := false

	// Update history archival state
	historyArchivalState, historyArchivalConfigChanged, err := d.getHistoryArchivalState(
		config,
		updateRequest.Configuration,
	)
	if err != nil {
		return nil, err
	}
	if historyArchivalConfigChanged {
		config.HistoryArchivalStatus = historyArchivalState.Status
		config.HistoryArchivalURI = historyArchivalState.URI
	}

	// Update visibility archival state
	visibilityArchivalState, visibilityArchivalConfigChanged, err := d.getVisibilityArchivalState(
		config,
		updateRequest.Configuration,
	)
	if err != nil {
		return nil, err
	}
	if visibilityArchivalConfigChanged {
		config.VisibilityArchivalStatus = visibilityArchivalState.Status
		config.VisibilityArchivalURI = visibilityArchivalState.URI
	}

	// Update domain info
	info, domainInfoChanged := d.updateDomainInfo(
		updateRequest.UpdatedInfo,
		info,
	)
	// Update domain config
	config, domainConfigChanged, err := d.updateDomainConfiguration(
		updateRequest.GetName(),
		config,
		updateRequest.Configuration,
	)
	if err != nil {
		return nil, err
	}

	// Update domain bad binary
	config, deleteBinaryChanged, err := d.updateDeleteBadBinary(
		config,
		updateRequest.DeleteBadBinary,
	)
	if err != nil {
		return nil, err
	}

	//Update replication config
	replicationConfig, replicationConfigChanged, activeClusterChanged, err := d.updateReplicationConfig(
		replicationConfig,
		updateRequest.ReplicationConfiguration,
	)
	if err != nil {
		return nil, err
	}

	// Handle graceful failover request
	if updateRequest.IsSetFailoverTimeoutInSeconds() {
		// must update active cluster on a global domain
		if !activeClusterChanged || !isGlobalDomain {
			return nil, errInvalidGracefulFailover
		}
		// must start with the passive -> active cluster
		if replicationConfig.ActiveClusterName != d.clusterMetadata.GetCurrentClusterName() {
			return nil, errCannotDoGracefulFailoverFromCluster
		}
		if replicationConfig.ActiveClusterName == currentActiveCluster {
			return nil, errGracefulFailoverInActiveCluster
		}
		// cannot have concurrent failover
		if gracefulFailoverEndTime != nil {
			return nil, errOngoingGracefulFailover
		}
		endTime := time.Now().UTC().Add(time.Duration(updateRequest.GetFailoverTimeoutInSeconds()) * time.Second).UnixNano()
		gracefulFailoverEndTime = &endTime
	}

	configurationChanged = historyArchivalConfigChanged || visibilityArchivalConfigChanged || domainInfoChanged || domainConfigChanged || deleteBinaryChanged || replicationConfigChanged

	if err := d.domainAttrValidator.validateDomainConfig(config); err != nil {
		return nil, err
	}
	if isGlobalDomain {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForGlobalDomain(
			replicationConfig,
		); err != nil {
			return nil, err
		}

		if configurationChanged && activeClusterChanged {
			return nil, errCannotDoDomainFailoverAndUpdate
		}

		if !activeClusterChanged && !d.clusterMetadata.IsMasterCluster() {
			return nil, errNotMasterCluster
		}
	} else {
		if err := d.domainAttrValidator.validateDomainReplicationConfigForLocalDomain(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	if configurationChanged || activeClusterChanged {
		// set the versions
		if configurationChanged {
			configVersion++
		}

		if activeClusterChanged && isGlobalDomain {
			// Force failover cleans graceful failover state
			if !updateRequest.IsSetFailoverTimeoutInSeconds() {
				// force failover cleanup graceful failover state
				gracefulFailoverEndTime = nil
			}
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
			FailoverEndTime:             gracefulFailoverEndTime,
			NotificationVersion:         notificationVersion,
		}
		err = d.metadataMgr.UpdateDomain(updateReq)
		if err != nil {
			return nil, err
		}
	}

	if isGlobalDomain {
		// TODO: add failover endtime to replication task
		err = d.domainReplicator.HandleTransmissionTask(replicator.DomainOperationUpdate,
			info, config, replicationConfig, configVersion, failoverVersion, isGlobalDomain)
		if err != nil {
			return nil, err
		}
	}

	response := &shared.UpdateDomainResponse{
		IsGlobalDomain:  common.BoolPtr(isGlobalDomain),
		FailoverVersion: common.Int64Ptr(failoverVersion),
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(info, config, replicationConfig)

	d.logger.Info("Update domain succeeded",
		tag.WorkflowDomainName(info.Name),
		tag.WorkflowDomainID(info.ID),
	)
	return response, nil
}

// DeprecateDomain deprecates a domain
func (d *HandlerImpl) DeprecateDomain(
	ctx context.Context,
	deprecateRequest *shared.DeprecateDomainRequest,
) error {

	clusterMetadata := d.clusterMetadata
	// TODO remove the IsGlobalDomainEnabled check once cross DC is public
	if clusterMetadata.IsGlobalDomainEnabled() && !clusterMetadata.IsMasterCluster() {
		return errNotMasterCluster
	}

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 domain table
	// and since we do not know which table will return the domain afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata()
	if err != nil {
		return err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: deprecateRequest.GetName()})
	if err != nil {
		return err
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
		return err
	}
	return nil
}

func (d *HandlerImpl) createResponse(
	info *persistence.DomainInfo,
	config *persistence.DomainConfig,
	replicationConfig *persistence.DomainReplicationConfig,
) (*shared.DomainInfo, *shared.DomainConfiguration, *shared.DomainReplicationConfiguration) {

	infoResult := &shared.DomainInfo{
		Name:        common.StringPtr(info.Name),
		Status:      getDomainStatus(info),
		Description: common.StringPtr(info.Description),
		OwnerEmail:  common.StringPtr(info.OwnerEmail),
		Data:        info.Data,
		UUID:        common.StringPtr(info.ID),
	}

	configResult := &shared.DomainConfiguration{
		EmitMetric:                             common.BoolPtr(config.EmitMetric),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(config.Retention),
		HistoryArchivalStatus:                  common.ArchivalStatusPtr(config.HistoryArchivalStatus),
		HistoryArchivalURI:                     common.StringPtr(config.HistoryArchivalURI),
		VisibilityArchivalStatus:               common.ArchivalStatusPtr(config.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  common.StringPtr(config.VisibilityArchivalURI),
		BadBinaries:                            &config.BadBinaries,
	}

	clusters := []*shared.ClusterReplicationConfiguration{}
	for _, cluster := range replicationConfig.Clusters {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(cluster.ClusterName),
		})
	}

	replicationConfigResult := &shared.DomainReplicationConfiguration{
		ActiveClusterName: common.StringPtr(replicationConfig.ActiveClusterName),
		Clusters:          clusters,
	}

	return infoResult, configResult, replicationConfigResult
}

func (d *HandlerImpl) mergeBadBinaries(
	old map[string]*shared.BadBinaryInfo,
	new map[string]*shared.BadBinaryInfo,
	createTimeNano int64,
) shared.BadBinaries {

	if old == nil {
		old = map[string]*shared.BadBinaryInfo{}
	}
	for k, v := range new {
		v.CreatedTimeNano = common.Int64Ptr(createTimeNano)
		old[k] = v
	}
	return shared.BadBinaries{
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
	status *shared.ArchivalStatus,
	URI string,
	defaultStatus shared.ArchivalStatus,
	defaultURI string,
) (*ArchivalEvent, error) {

	event := &ArchivalEvent{
		status:     status,
		URI:        URI,
		defaultURI: defaultURI,
	}
	if event.status == nil {
		event.status = defaultStatus.Ptr()
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *HandlerImpl) toArchivalUpdateEvent(
	status *shared.ArchivalStatus,
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

func (d *HandlerImpl) getHistoryArchivalState(
	config *persistence.DomainConfig,
	requestedConfig *shared.DomainConfiguration,
) (*ArchivalState, bool, error) {

	currentHistoryArchivalState := &ArchivalState{
		Status: config.HistoryArchivalStatus,
		URI:    config.HistoryArchivalURI,
	}
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()

	if requestedConfig != nil && clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalUpdateEvent(
			requestedConfig.HistoryArchivalStatus,
			requestedConfig.GetHistoryArchivalURI(),
			clusterHistoryArchivalConfig.GetDomainDefaultURI(),
		)
		if err != nil {
			return currentHistoryArchivalState, false, err
		}
		return currentHistoryArchivalState.getNextState(archivalEvent, d.validateHistoryArchivalURI)
	}
	return currentHistoryArchivalState, false, nil
}

func (d *HandlerImpl) getVisibilityArchivalState(
	config *persistence.DomainConfig,
	requestedConfig *shared.DomainConfiguration,
) (*ArchivalState, bool, error) {
	currentVisibilityArchivalState := &ArchivalState{
		Status: config.VisibilityArchivalStatus,
		URI:    config.VisibilityArchivalURI,
	}
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if requestedConfig != nil && clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalUpdateEvent(
			requestedConfig.VisibilityArchivalStatus,
			requestedConfig.GetVisibilityArchivalURI(),
			clusterVisibilityArchivalConfig.GetDomainDefaultURI(),
		)
		if err != nil {
			return currentVisibilityArchivalState, false, err
		}
		return currentVisibilityArchivalState.getNextState(archivalEvent, d.validateVisibilityArchivalURI)
	}
	return currentVisibilityArchivalState, false, nil
}

func (d *HandlerImpl) updateDomainInfo(
	updatedDomainInfo *shared.UpdateDomainInfo,
	currentDomainInfo *persistence.DomainInfo,
) (*persistence.DomainInfo, bool) {

	isDomainUpdated := false
	if updatedDomainInfo != nil {
		if updatedDomainInfo.Description != nil {
			isDomainUpdated = true
			currentDomainInfo.Description = updatedDomainInfo.GetDescription()
		}
		if updatedDomainInfo.OwnerEmail != nil {
			isDomainUpdated = true
			currentDomainInfo.OwnerEmail = updatedDomainInfo.GetOwnerEmail()
		}
		if updatedDomainInfo.Data != nil {
			isDomainUpdated = true
			// only do merging
			currentDomainInfo.Data = d.mergeDomainData(currentDomainInfo.Data, updatedDomainInfo.Data)
		}
	}
	return currentDomainInfo, isDomainUpdated
}

func (d *HandlerImpl) updateDomainConfiguration(
	domainName string,
	config *persistence.DomainConfig,
	domainConfig *shared.DomainConfiguration,
) (*persistence.DomainConfig, bool, error) {

	isConfigChanged := false
	if domainConfig != nil {
		if domainConfig.EmitMetric != nil {
			isConfigChanged = true
			config.EmitMetric = domainConfig.GetEmitMetric()
		}
		if domainConfig.WorkflowExecutionRetentionPeriodInDays != nil {
			isConfigChanged = true
			config.Retention = domainConfig.GetWorkflowExecutionRetentionPeriodInDays()
		}
		if domainConfig.BadBinaries != nil {
			maxLength := d.maxBadBinaryCount(domainName)
			// only do merging
			config.BadBinaries = d.mergeBadBinaries(config.BadBinaries.Binaries, domainConfig.BadBinaries.Binaries, time.Now().UnixNano())
			if len(config.BadBinaries.Binaries) > maxLength {
				return config, isConfigChanged, &shared.BadRequestError{
					Message: fmt.Sprintf("Total resetBinaries cannot exceed the max limit: %v", maxLength),
				}
			}
		}
	}
	return config, isConfigChanged, nil
}

func (d *HandlerImpl) updateDeleteBadBinary(
	config *persistence.DomainConfig,
	deleteBadBinary *string,
) (*persistence.DomainConfig, bool, error) {

	if deleteBadBinary != nil {
		_, ok := config.BadBinaries.Binaries[*deleteBadBinary]
		if !ok {
			return config, false, &shared.BadRequestError{
				Message: fmt.Sprintf("Bad binary checksum %v doesn't exists.", *deleteBadBinary),
			}
		}
		delete(config.BadBinaries.Binaries, *deleteBadBinary)
		return config, true, nil
	}
	return config, false, nil
}

func (d *HandlerImpl) updateReplicationConfig(
	config *persistence.DomainReplicationConfig,
	replicationConfig *shared.DomainReplicationConfiguration,
) (*persistence.DomainReplicationConfig, bool, bool, error) {

	clusterUpdated := false
	activeClusterUpdated := false
	if replicationConfig != nil {
		if len(replicationConfig.GetClusters()) != 0 {
			clusterUpdated = true
			clustersNew := []*persistence.ClusterReplicationConfig{}
			for _, clusterConfig := range replicationConfig.Clusters {
				clustersNew = append(clustersNew, &persistence.ClusterReplicationConfig{
					ClusterName: clusterConfig.GetClusterName(),
				})
			}

			if err := d.domainAttrValidator.validateDomainReplicationConfigClustersDoesNotRemove(
				config.Clusters,
				clustersNew,
			); err != nil {
				return config, clusterUpdated, activeClusterUpdated, err
			}
			config.Clusters = clustersNew
		}

		if replicationConfig.IsSetActiveClusterName() {
			activeClusterUpdated = true
			config.ActiveClusterName = replicationConfig.GetActiveClusterName()
		}
	}
	return config, clusterUpdated, activeClusterUpdated, nil
}

func getDomainStatus(info *persistence.DomainInfo) *shared.DomainStatus {
	switch info.Status {
	case persistence.DomainStatusRegistered:
		v := shared.DomainStatusRegistered
		return &v
	case persistence.DomainStatusDeprecated:
		v := shared.DomainStatusDeprecated
		return &v
	case persistence.DomainStatusDeleted:
		v := shared.DomainStatusDeleted
		return &v
	}

	return nil
}
