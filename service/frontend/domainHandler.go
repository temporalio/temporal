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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	domainHandlerImpl struct {
		config           *Config
		logger           log.Logger
		metadataMgr      persistence.MetadataManager
		clusterMetadata  cluster.Metadata
		blobstoreClient  blobstore.Client
		domainReplicator DomainReplicator
	}
)

// newDomainHandler create a new domain handler
func newDomainHandler(config *Config,
	logger log.Logger,
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	blobstoreClient blobstore.Client,
	domainReplicator DomainReplicator) *domainHandlerImpl {

	return &domainHandlerImpl{
		config:           config,
		logger:           logger,
		metadataMgr:      metadataMgr,
		clusterMetadata:  clusterMetadata,
		blobstoreClient:  blobstoreClient,
		domainReplicator: domainReplicator,
	}
}

func (d *domainHandlerImpl) registerDomain(ctx context.Context,
	registerRequest *shared.RegisterDomainRequest) (retError error) {

	if registerRequest == nil {
		return errRequestNotSet
	}

	if err := d.checkPermission(registerRequest.SecurityToken); err != nil {
		return err
	}

	if registerRequest.GetName() == "" {
		return errDomainNotSet
	}

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
	if !registerRequest.GetIsGlobalDomain() {
		registerRequest.ActiveClusterName = nil
		registerRequest.Clusters = nil
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
		if err := d.validateClusterName(activeClusterName); err != nil {
			return err
		}
	}
	clusters := []*persistence.ClusterReplicationConfig{}
	for _, cluster := range registerRequest.Clusters {
		clusterName := cluster.GetClusterName()
		if err := d.validateClusterName(clusterName); err != nil {
			return err
		}
		clusters = append(clusters, &persistence.ClusterReplicationConfig{ClusterName: clusterName})
	}
	clusters = persistence.GetOrUseDefaultClusters(activeClusterName, clusters)

	// validate active cluster is also specified in all clusters
	activeClusterInClusters := false
	for _, cluster := range clusters {
		if cluster.ClusterName == activeClusterName {
			activeClusterInClusters = true
			break
		}
	}
	if !activeClusterInClusters {
		return errActiveClusterNotInClusters
	}

	currentArchivalState := neverEnabledState()
	nextArchivalState := currentArchivalState
	archivalClusterConfig := d.clusterMetadata.ArchivalConfig()
	if archivalClusterConfig.ConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(registerRequest, archivalClusterConfig.GetDefaultBucket())
		if err != nil {
			return err
		}
		nextArchivalState, _, err = currentArchivalState.getNextState(ctx, d.blobstoreClient, archivalEvent)
		if err != nil {
			return err
		}
	}

	if err := d.validateRetentionPeriod(registerRequest.GetWorkflowExecutionRetentionPeriodInDays()); err != nil {
		return err
	}

	failoverVersion := common.EmptyVersion
	if registerRequest.GetIsGlobalDomain() {
		failoverVersion = d.clusterMetadata.GetNextFailoverVersion(activeClusterName, 0)
	}

	domainRequest := &persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        registerRequest.GetName(),
			Status:      persistence.DomainStatusRegistered,
			OwnerEmail:  registerRequest.GetOwnerEmail(),
			Description: registerRequest.GetDescription(),
			Data:        registerRequest.Data,
		},
		Config: &persistence.DomainConfig{
			Retention:      registerRequest.GetWorkflowExecutionRetentionPeriodInDays(),
			EmitMetric:     registerRequest.GetEmitMetric(),
			ArchivalBucket: nextArchivalState.bucket,
			ArchivalStatus: nextArchivalState.status,
			BadBinaries:    shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters:          clusters,
		},
		IsGlobalDomain:  registerRequest.GetIsGlobalDomain(),
		FailoverVersion: failoverVersion,
	}

	domainResponse, err := d.metadataMgr.CreateDomain(domainRequest)
	if err != nil {
		return err
	}

	if domainRequest.IsGlobalDomain {
		err = d.domainReplicator.HandleTransmissionTask(replicator.DomainOperationCreate,
			domainRequest.Info, domainRequest.Config, domainRequest.ReplicationConfig, 0,
			domainRequest.FailoverVersion, domainRequest.IsGlobalDomain)
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

func (d *domainHandlerImpl) listDomains(ctx context.Context,
	listRequest *shared.ListDomainsRequest) (response *shared.ListDomainsResponse, retError error) {

	if listRequest == nil {
		return nil, errRequestNotSet
	}

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
		desc.DomainInfo, desc.Configuration, desc.ReplicationConfiguration = d.createResponse(ctx, domain.Info, domain.Config, domain.ReplicationConfig)
		domains = append(domains, desc)
	}

	response = &shared.ListDomainsResponse{
		Domains:       domains,
		NextPageToken: resp.NextPageToken,
	}

	return response, nil
}

func (d *domainHandlerImpl) describeDomain(ctx context.Context,
	describeRequest *shared.DescribeDomainRequest) (response *shared.DescribeDomainResponse, retError error) {

	if describeRequest == nil {
		return nil, errRequestNotSet
	}

	if describeRequest.GetName() == "" && describeRequest.GetUUID() == "" {
		return nil, errDomainNotSet
	}

	// TODO, we should migrate the non global domain to new table, see #773
	req := &persistence.GetDomainRequest{
		Name: describeRequest.GetName(),
		ID:   describeRequest.GetUUID(),
	}
	resp, err := d.metadataMgr.GetDomain(req)
	if err != nil {
		return nil, err
	}

	response = &shared.DescribeDomainResponse{
		IsGlobalDomain:  common.BoolPtr(resp.IsGlobalDomain),
		FailoverVersion: common.Int64Ptr(resp.FailoverVersion),
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(ctx, resp.Info, resp.Config, resp.ReplicationConfig)
	return response, nil
}

func (d *domainHandlerImpl) updateDomain(ctx context.Context,
	updateRequest *shared.UpdateDomainRequest) (resp *shared.UpdateDomainResponse, retError error) {

	if updateRequest == nil {
		return nil, errRequestNotSet
	}

	if updateRequest.GetName() == "" {
		return nil, errDomainNotSet
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
	getResponse, err := d.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: updateRequest.GetName()})
	if err != nil {
		return nil, err
	}
	if !getResponse.IsGlobalDomain {
		updateRequest.ReplicationConfiguration = nil
	}

	// don't require permission for failover request
	if !isFailoverRequest(updateRequest) {
		if err := d.checkPermission(updateRequest.SecurityToken); err != nil {
			return nil, err
		}
	}

	info := getResponse.Info
	config := getResponse.Config
	replicationConfig := getResponse.ReplicationConfig
	configVersion := getResponse.ConfigVersion
	failoverVersion := getResponse.FailoverVersion
	failoverNotificationVersion := getResponse.FailoverNotificationVersion

	currentArchivalState := &archivalState{
		bucket: config.ArchivalBucket,
		status: config.ArchivalStatus,
	}
	nextArchivalState := currentArchivalState
	archivalConfigChanged := false
	archivalClusterConfig := d.clusterMetadata.ArchivalConfig()
	if archivalClusterConfig.ConfiguredForArchival() {
		archivalEvent, err := d.toArchivalUpdateEvent(updateRequest, archivalClusterConfig.GetDefaultBucket())
		if err != nil {
			return nil, err
		}
		nextArchivalState, archivalConfigChanged, err = currentArchivalState.getNextState(ctx, d.blobstoreClient, archivalEvent)
		if err != nil {
			return nil, err
		}
	}

	// whether active cluster is changed
	activeClusterChanged := false
	// whether anything other than active cluster is changed
	configurationChanged := false

	validateReplicationConfig := func(existingDomain *persistence.GetDomainResponse,
		updatedActiveClusterName *string, updatedClusters []*shared.ClusterReplicationConfiguration) error {

		if len(updatedClusters) != 0 {
			configurationChanged = true
			clusters := []*persistence.ClusterReplicationConfig{}
			// this is used to prove that target cluster names is a superset of existing cluster names
			targetClustersNames := make(map[string]bool)
			for _, cluster := range updatedClusters {
				clusterName := cluster.GetClusterName()
				if err := d.validateClusterName(clusterName); err != nil {
					return err
				}
				clusters = append(clusters, &persistence.ClusterReplicationConfig{ClusterName: clusterName})
				targetClustersNames[clusterName] = true
			}

			// NOTE: this is to validate that target cluster cannot change
			// For future adding new cluster and backfill workflow remove this logic
			// -- START
			existingClustersNames := make(map[string]bool)
			for _, cluster := range existingDomain.ReplicationConfig.Clusters {
				existingClustersNames[cluster.ClusterName] = true
			}
			if len(existingClustersNames) != len(targetClustersNames) {
				return errCannotModifyClustersFromDomain
			}
			for clusterName := range existingClustersNames {
				if _, ok := targetClustersNames[clusterName]; !ok {
					return errCannotModifyClustersFromDomain
				}
			}
			// -- END

			// validate that updated clusters is a superset of existing clusters
			for _, cluster := range replicationConfig.Clusters {
				if _, ok := targetClustersNames[cluster.ClusterName]; !ok {
					return errCannotModifyClustersFromDomain
				}
			}
			replicationConfig.Clusters = clusters
			// for local domain, the clusters should be 1 and only 1, being the current cluster
			if len(replicationConfig.Clusters) > 1 && !existingDomain.IsGlobalDomain {
				return errCannotAddClusterToLocalDomain
			}
		}

		if updatedActiveClusterName != nil {
			activeClusterChanged = true
			replicationConfig.ActiveClusterName = *updatedActiveClusterName
		}

		// validate active cluster is also specified in all clusters
		activeClusterInClusters := false
	CheckActiveClusterNameInClusters:
		for _, cluster := range replicationConfig.Clusters {
			if cluster.ClusterName == replicationConfig.ActiveClusterName {
				activeClusterInClusters = true
				break CheckActiveClusterNameInClusters
			}
		}
		if !activeClusterInClusters {
			return errActiveClusterNotInClusters
		}

		return nil
	}

	if updateRequest.UpdatedInfo != nil {
		updatedInfo := updateRequest.UpdatedInfo
		if updatedInfo.Description != nil {
			configurationChanged = true
			info.Description = updatedInfo.GetDescription()
		}
		if updatedInfo.OwnerEmail != nil {
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
			config.EmitMetric = updatedConfig.GetEmitMetric()
		}
		if updatedConfig.WorkflowExecutionRetentionPeriodInDays != nil {
			configurationChanged = true
			config.Retention = updatedConfig.GetWorkflowExecutionRetentionPeriodInDays()
			if err := d.validateRetentionPeriod(config.Retention); err != nil {
				return nil, err
			}
		}
		if archivalConfigChanged {
			configurationChanged = true
			config.ArchivalBucket = nextArchivalState.bucket
			config.ArchivalStatus = nextArchivalState.status
		}
		if updatedConfig.BadBinaries != nil {
			maxLength := d.config.MaxBadBinaries(updateRequest.GetName())
			// only do merging
			config.BadBinaries = d.mergeBadBinaries(config.BadBinaries.Binaries, updatedConfig.BadBinaries.Binaries, time.Now().UnixNano())
			if len(config.BadBinaries.Binaries) > maxLength {
				return nil, &shared.BadRequestError{
					Message: fmt.Sprintf("Total resetBinaries cannot exceed the max limit: %v", maxLength),
				}
			}
		}
	}

	if updateRequest.DeleteBadBinary != nil {
		binChecksum := updateRequest.GetDeleteBadBinary()
		_, ok := config.BadBinaries.Binaries[binChecksum]
		if !ok {
			return nil, &shared.BadRequestError{
				Message: fmt.Sprintf("Bad binary checksum %v doesn't exists.", binChecksum),
			}
		}
		configurationChanged = true
		delete(config.BadBinaries.Binaries, binChecksum)
	}

	if updateRequest.ReplicationConfiguration != nil {
		updateReplicationConfig := updateRequest.ReplicationConfiguration
		if err := validateReplicationConfig(getResponse,
			updateReplicationConfig.ActiveClusterName, updateReplicationConfig.Clusters); err != nil {
			return nil, err
		}
	}

	if configurationChanged && activeClusterChanged {
		return nil, errCannotDoDomainFailoverAndUpdate
	} else if configurationChanged || activeClusterChanged {
		if configurationChanged && getResponse.IsGlobalDomain && !d.clusterMetadata.IsMasterCluster() {
			return nil, errNotMasterCluster
		}

		// set the versions
		if configurationChanged {
			configVersion++
		}
		if activeClusterChanged {
			failoverVersion = d.clusterMetadata.GetNextFailoverVersion(replicationConfig.ActiveClusterName, failoverVersion)
			failoverNotificationVersion = notificationVersion
		}

		updateReq := &persistence.UpdateDomainRequest{
			Info:                        info,
			Config:                      config,
			ReplicationConfig:           replicationConfig,
			ConfigVersion:               configVersion,
			FailoverVersion:             failoverVersion,
			FailoverNotificationVersion: failoverNotificationVersion,
		}

		switch getResponse.TableVersion {
		case persistence.DomainTableVersionV1:
			updateReq.NotificationVersion = getResponse.NotificationVersion
			updateReq.TableVersion = persistence.DomainTableVersionV1
		case persistence.DomainTableVersionV2:
			updateReq.NotificationVersion = notificationVersion
			updateReq.TableVersion = persistence.DomainTableVersionV2
		default:
			return nil, errors.New("domain table version is not set")
		}
		err = d.metadataMgr.UpdateDomain(updateReq)
		if err != nil {
			return nil, err
		}
	} else if getResponse.IsGlobalDomain && !d.clusterMetadata.IsMasterCluster() {
		// although there is no attr updated, just prevent customer to use the non master cluster
		// for update domain, ever (except if customer want to do a domain failover)
		return nil, errNotMasterCluster
	}

	if getResponse.IsGlobalDomain {
		err = d.domainReplicator.HandleTransmissionTask(replicator.DomainOperationUpdate,
			info, config, replicationConfig, configVersion, failoverVersion, getResponse.IsGlobalDomain)
		if err != nil {
			return nil, err
		}
	}

	response := &shared.UpdateDomainResponse{
		IsGlobalDomain:  common.BoolPtr(getResponse.IsGlobalDomain),
		FailoverVersion: common.Int64Ptr(failoverVersion),
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = d.createResponse(ctx, info, config, replicationConfig)

	d.logger.Info("Update domain succeeded",
		tag.WorkflowDomainName(info.Name),
		tag.WorkflowDomainID(info.ID),
	)
	return response, nil
}

func (d *domainHandlerImpl) deprecateDomain(ctx context.Context,
	deprecateRequest *shared.DeprecateDomainRequest) (retError error) {

	if deprecateRequest == nil {
		return errRequestNotSet
	}

	if err := d.checkPermission(deprecateRequest.SecurityToken); err != nil {
		return err
	}

	clusterMetadata := d.clusterMetadata
	// TODO remove the IsGlobalDomainEnabled check once cross DC is public
	if clusterMetadata.IsGlobalDomainEnabled() && !clusterMetadata.IsMasterCluster() {
		return errNotMasterCluster
	}

	if deprecateRequest.GetName() == "" {
		return errDomainNotSet
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
		Info:              getResponse.Info,
		Config:            getResponse.Config,
		ReplicationConfig: getResponse.ReplicationConfig,
		ConfigVersion:     getResponse.ConfigVersion,
		FailoverVersion:   getResponse.FailoverVersion,
	}

	switch getResponse.TableVersion {
	case persistence.DomainTableVersionV1:
		updateReq.NotificationVersion = getResponse.NotificationVersion
		updateReq.TableVersion = persistence.DomainTableVersionV1
	case persistence.DomainTableVersionV2:
		updateReq.FailoverNotificationVersion = getResponse.FailoverNotificationVersion
		updateReq.NotificationVersion = notificationVersion
		updateReq.TableVersion = persistence.DomainTableVersionV2
	default:
		return errors.New("domain table version is not set")
	}
	err = d.metadataMgr.UpdateDomain(updateReq)
	if err != nil {
		return err
	}

	if err != nil {
		return errDomainNotSet
	}
	return nil
}

func (d *domainHandlerImpl) createResponse(
	ctx context.Context,
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
		ArchivalStatus:                         common.ArchivalStatusPtr(config.ArchivalStatus),
		ArchivalBucketName:                     common.StringPtr(config.ArchivalBucket),
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

func (d *domainHandlerImpl) mergeBadBinaries(old map[string]*shared.BadBinaryInfo, new map[string]*shared.BadBinaryInfo, createTimeNano int64) shared.BadBinaries {
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

func (d *domainHandlerImpl) mergeDomainData(old map[string]string, new map[string]string) map[string]string {
	if old == nil {
		old = map[string]string{}
	}
	for k, v := range new {
		old[k] = v
	}
	return old
}

func (d *domainHandlerImpl) validateClusterName(clusterName string) error {
	if info, ok := d.clusterMetadata.GetAllClusterInfo()[clusterName]; !ok || !info.Enabled {
		errMsg := "Invalid cluster name: %s"
		return &shared.BadRequestError{Message: fmt.Sprintf(errMsg, clusterName)}
	}
	return nil
}

func (d *domainHandlerImpl) validateRetentionPeriod(retentionDays int32) error {
	if retentionDays < int32(d.config.MinRetentionDays()) {
		return errInvalidRetentionPeriod
	}
	return nil
}

func (d *domainHandlerImpl) checkPermission(securityToken *string) error {
	if d.config.EnableAdminProtection() {
		if securityToken == nil {
			return errNoPermission
		}
		requiredToken := d.config.AdminOperationToken()
		if *securityToken != requiredToken {
			return errNoPermission
		}
	}
	return nil
}

func (d *domainHandlerImpl) toArchivalRegisterEvent(request *shared.RegisterDomainRequest, defaultBucket string) (*archivalEvent, error) {
	event := &archivalEvent{
		defaultBucket: defaultBucket,
		bucket:        request.GetArchivalBucketName(),
		status:        request.ArchivalStatus,
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *domainHandlerImpl) toArchivalUpdateEvent(request *shared.UpdateDomainRequest, defaultBucket string) (*archivalEvent, error) {
	event := &archivalEvent{
		defaultBucket: defaultBucket,
	}
	if request.Configuration != nil {
		cfg := request.GetConfiguration()
		event.bucket = cfg.GetArchivalBucketName()
		event.status = cfg.ArchivalStatus
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}
