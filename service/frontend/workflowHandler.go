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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/uber/cadence/common/blobstore"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/replicator"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"go.uber.org/yarpc/yarpcerrors"
)

var _ workflowserviceserver.Interface = (*WorkflowHandler)(nil)

type (
	// WorkflowHandler - Thrift handler inteface for workflow service
	WorkflowHandler struct {
		domainCache       cache.DomainCache
		metadataMgr       persistence.MetadataManager
		historyMgr        persistence.HistoryManager
		historyV2Mgr      persistence.HistoryV2Manager
		visibitiltyMgr    persistence.VisibilityManager
		history           history.Client
		matching          matching.Client
		matchingRawClient matching.Client
		tokenSerializer   common.TaskTokenSerializer
		metricsClient     metrics.Client
		startWG           sync.WaitGroup
		rateLimiter       common.TokenBucket
		config            *Config
		domainReplicator  DomainReplicator
		blobstoreClient   blobstore.Client
		service.Service
	}

	getHistoryContinuationToken struct {
		RunID             string
		FirstEventID      int64
		NextEventID       int64
		IsWorkflowRunning bool
		PersistenceToken  []byte
		TransientDecision *gen.TransientDecisionInfo
		EventStoreVersion int32
		BranchToken       []byte
	}
)

var (
	errDomainNotSet                               = &gen.BadRequestError{Message: "Domain not set on request."}
	errTaskTokenNotSet                            = &gen.BadRequestError{Message: "Task token not set on request."}
	errInvalidTaskToken                           = &gen.BadRequestError{Message: "Invalid TaskToken."}
	errInvalidRequestType                         = &gen.BadRequestError{Message: "Invalid request type."}
	errTaskListNotSet                             = &gen.BadRequestError{Message: "TaskList is not set on request."}
	errTaskListTypeNotSet                         = &gen.BadRequestError{Message: "TaskListType is not set on request."}
	errExecutionNotSet                            = &gen.BadRequestError{Message: "Execution is not set on request."}
	errWorkflowIDNotSet                           = &gen.BadRequestError{Message: "WorkflowId is not set on request."}
	errRunIDNotSet                                = &gen.BadRequestError{Message: "RunId is not set on request."}
	errActivityIDNotSet                           = &gen.BadRequestError{Message: "ActivityID is not set on request."}
	errInvalidRunID                               = &gen.BadRequestError{Message: "Invalid RunId."}
	errInvalidNextPageToken                       = &gen.BadRequestError{Message: "Invalid NextPageToken."}
	errNextPageTokenRunIDMismatch                 = &gen.BadRequestError{Message: "RunID in the request does not match the NextPageToken."}
	errQueryNotSet                                = &gen.BadRequestError{Message: "WorkflowQuery is not set on request."}
	errQueryTypeNotSet                            = &gen.BadRequestError{Message: "QueryType is not set on request."}
	errRequestNotSet                              = &gen.BadRequestError{Message: "Request is nil."}
	errNoPermission                               = &gen.BadRequestError{Message: "No permission to do this operation."}
	errRequestIDNotSet                            = &gen.BadRequestError{Message: "RequestId is not set on request."}
	errWorkflowTypeNotSet                         = &gen.BadRequestError{Message: "WorkflowType is not set on request."}
	errInvalidExecutionStartToCloseTimeoutSeconds = &gen.BadRequestError{Message: "A valid ExecutionStartToCloseTimeoutSeconds is not set on request."}
	errInvalidTaskStartToCloseTimeoutSeconds      = &gen.BadRequestError{Message: "A valid TaskStartToCloseTimeoutSeconds is not set on request."}

	// err indicating that this cluster is not the master, so cannot do domain registration or update
	errNotMasterCluster                = &gen.BadRequestError{Message: "Cluster is not master cluster, cannot do domain registration or domain update."}
	errCannotAddClusterToLocalDomain   = &gen.BadRequestError{Message: "Cannot add more replicated cluster to local domain."}
	errCannotModifyClustersFromDomain  = &gen.BadRequestError{Message: "Cannot modify existing replicated clusters from a domain."}
	errActiveClusterNotInClusters      = &gen.BadRequestError{Message: "Active cluster is not contained in all clusters."}
	errCannotDoDomainFailoverAndUpdate = &gen.BadRequestError{Message: "Cannot set active cluster to current cluster when other parameters are set."}

	frontendServiceRetryPolicy = common.CreateFrontendServiceRetryPolicy()
)

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(sVice service.Service, config *Config, metadataMgr persistence.MetadataManager,
	historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager, visibilityMgr persistence.VisibilityManager,
	kafkaProducer messaging.Producer, blobstoreClient blobstore.Client) *WorkflowHandler {
	handler := &WorkflowHandler{
		Service:          sVice,
		config:           config,
		metadataMgr:      metadataMgr,
		historyMgr:       historyMgr,
		historyV2Mgr:     historyV2Mgr,
		visibitiltyMgr:   visibilityMgr,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
		domainCache:      cache.NewDomainCache(metadataMgr, sVice.GetClusterMetadata(), sVice.GetMetricsClient(), sVice.GetLogger()),
		rateLimiter:      common.NewTokenBucket(config.RPS(), common.NewRealTimeSource()),
		domainReplicator: NewDomainReplicator(kafkaProducer, sVice.GetLogger()),
		blobstoreClient:  blobstoreClient,
	}
	// prevent us from trying to serve requests before handler's Start() is complete
	handler.startWG.Add(1)
	return handler
}

// Start starts the handler
func (wh *WorkflowHandler) Start() error {
	wh.Service.GetDispatcher().Register(workflowserviceserver.New(wh))
	wh.Service.GetDispatcher().Register(metaserver.New(wh))
	wh.Service.Start()
	wh.domainCache.Start()
	var err error
	wh.history, err = wh.Service.GetClientFactory().NewHistoryClient()
	if err != nil {
		return err
	}
	wh.matchingRawClient, err = wh.Service.GetClientFactory().NewMatchingClient()
	if err != nil {
		return err
	}
	wh.matching = matching.NewRetryableClient(wh.matchingRawClient, common.CreateMatchingRetryPolicy(),
		common.IsWhitelistServiceTransientError)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	return nil
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	wh.domainCache.Stop()
	wh.metadataMgr.Close()
	wh.visibitiltyMgr.Close()
	wh.historyMgr.Close()
	wh.Service.Stop()
}

// Health is for health check
func (wh *WorkflowHandler) Health(ctx context.Context) (*health.HealthStatus, error) {
	wh.startWG.Wait()
	wh.GetLogger().Debug("Frontend health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("frontend good")}
	return hs, nil
}

func (wh *WorkflowHandler) checkPermission(securityToken *string, scope int) error {
	if wh.config.EnableAdminProtection() {
		if securityToken == nil {
			return wh.error(errNoPermission, scope)
		}
		requiredToken := wh.config.AdminOperationToken()
		if *securityToken != requiredToken {
			return wh.error(errNoPermission, scope)
		}
	}
	return nil
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandler) RegisterDomain(ctx context.Context, registerRequest *gen.RegisterDomainRequest) error {
	scope := metrics.FrontendRegisterDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if registerRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if err := wh.checkPermission(registerRequest.SecurityToken, scope); err != nil {
		return err
	}

	clusterMetadata := wh.GetClusterMetadata()
	// TODO remove the IsGlobalDomainEnabled check once cross DC is public
	if clusterMetadata.IsGlobalDomainEnabled() && !clusterMetadata.IsMasterCluster() {
		return wh.error(errNotMasterCluster, scope)
	}
	if !clusterMetadata.IsGlobalDomainEnabled() {
		registerRequest.ActiveClusterName = nil
		registerRequest.Clusters = nil
	}

	if registerRequest.GetName() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	// first check if the name is already resigered as the local domain
	_, err := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: registerRequest.GetName()})
	if err != nil {
		if _, ok := err.(*gen.EntityNotExistsError); !ok {
			return wh.error(err, scope)
		}
		// extity not exists, we can proceed to create the domain
	} else {
		// domain already exists, cannot proceed
		return wh.error(&gen.DomainAlreadyExistsError{Message: "Domain already exists."}, scope)
	}

	activeClusterName := clusterMetadata.GetCurrentClusterName()
	// input validation on cluster names
	if registerRequest.ActiveClusterName != nil {
		activeClusterName = registerRequest.GetActiveClusterName()
		if err := wh.validateClusterName(activeClusterName); err != nil {
			return wh.error(err, scope)
		}
	}
	clusters := []*persistence.ClusterReplicationConfig{}
	for _, cluster := range registerRequest.Clusters {
		clusterName := cluster.GetClusterName()
		if err := wh.validateClusterName(clusterName); err != nil {
			return wh.error(err, scope)
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
		return wh.error(errActiveClusterNotInClusters, scope)
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
			Retention:  registerRequest.GetWorkflowExecutionRetentionPeriodInDays(),
			EmitMetric: registerRequest.GetEmitMetric(),
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters:          clusters,
		},
		IsGlobalDomain:  clusterMetadata.IsGlobalDomainEnabled(),
		FailoverVersion: clusterMetadata.GetNextFailoverVersion(activeClusterName, 0),
	}

	domainResponse, err := wh.metadataMgr.CreateDomain(domainRequest)
	if err != nil {
		return wh.error(err, scope)
	}

	if domainRequest.IsGlobalDomain {
		err = wh.domainReplicator.HandleTransmissionTask(replicator.DomainOperationCreate,
			domainRequest.Info, domainRequest.Config, domainRequest.ReplicationConfig, 0,
			domainRequest.FailoverVersion, domainRequest.IsGlobalDomain)
		if err != nil {
			return wh.error(err, scope)
		}
	}

	// TODO: Log through logging framework.  We need to have good auditing of domain CRUD
	wh.GetLogger().Debugf("Register domain succeeded for name: %v, Id: %v", registerRequest.GetName(), domainResponse.ID)

	return nil
}

// ListDomains returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) ListDomains(ctx context.Context,
	listRequest *gen.ListDomainsRequest) (*gen.ListDomainsResponse, error) {
	scope := metrics.FrontendListDomainsScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	pageSize := 100
	if listRequest.GetPageSize() != 0 {
		pageSize = int(listRequest.GetPageSize())
	}

	resp, err := wh.metadataMgr.ListDomains(&persistence.ListDomainsRequest{
		PageSize:      pageSize,
		NextPageToken: listRequest.NextPageToken,
	})

	if err != nil {
		return nil, wh.error(err, scope)
	}

	domains := []*gen.DescribeDomainResponse{}
	for _, d := range resp.Domains {
		desc := &gen.DescribeDomainResponse{
			IsGlobalDomain:  common.BoolPtr(d.IsGlobalDomain),
			FailoverVersion: common.Int64Ptr(d.FailoverVersion),
		}
		desc.DomainInfo, desc.Configuration, desc.ReplicationConfiguration = createDomainResponse(d.Info, d.Config, d.ReplicationConfig)
		domains = append(domains, desc)
	}

	response := &gen.ListDomainsResponse{
		Domains:       domains,
		NextPageToken: resp.NextPageToken,
	}

	return response, nil
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) DescribeDomain(ctx context.Context,
	describeRequest *gen.DescribeDomainRequest) (*gen.DescribeDomainResponse, error) {

	scope := metrics.FrontendDescribeDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if describeRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if describeRequest.GetName() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	// TODO, we should migrate the non global domain to new table, see #773
	resp, err := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: describeRequest.GetName()})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	response := &gen.DescribeDomainResponse{
		IsGlobalDomain:  common.BoolPtr(resp.IsGlobalDomain),
		FailoverVersion: common.Int64Ptr(resp.FailoverVersion),
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = createDomainResponse(
		resp.Info, resp.Config, resp.ReplicationConfig)

	return response, nil
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandler) UpdateDomain(ctx context.Context,
	updateRequest *gen.UpdateDomainRequest) (*gen.UpdateDomainResponse, error) {

	scope := metrics.FrontendUpdateDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if updateRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// don't require permission for failover request
	if !isFailoverRequest(updateRequest) {
		if err := wh.checkPermission(updateRequest.SecurityToken, scope); err != nil {
			return nil, err
		}
	}

	clusterMetadata := wh.GetClusterMetadata()
	// TODO remove the IsGlobalDomainEnabled check once cross DC is public
	if !clusterMetadata.IsGlobalDomainEnabled() {
		updateRequest.ReplicationConfiguration = nil
	}

	if updateRequest.GetName() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 domain table
	// and since we do not know which table will return the domain afterwards
	// this call has to be made
	metadata, err := wh.metadataMgr.GetMetadata()
	if err != nil {
		return nil, wh.error(err, scope)
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: updateRequest.GetName()})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	info := getResponse.Info
	config := getResponse.Config
	replicationConfig := getResponse.ReplicationConfig
	configVersion := getResponse.ConfigVersion
	failoverVersion := getResponse.FailoverVersion
	failoverNotificationVersion := getResponse.FailoverNotificationVersion

	// whether active cluster is changed
	activeClusterChanged := false
	// whether anything other than active cluster is changed
	configurationChanged := false

	validateReplicationConfig := func(existingDomain *persistence.GetDomainResponse,
		updatedActiveClusterName *string, updatedClusters []*gen.ClusterReplicationConfiguration) error {

		if len(updatedClusters) != 0 {
			configurationChanged = true
			clusters := []*persistence.ClusterReplicationConfig{}
			// this is used to prove that target cluster names is a superset of existing cluster names
			targetClustersNames := make(map[string]bool)
			for _, cluster := range updatedClusters {
				clusterName := cluster.GetClusterName()
				if err := wh.validateClusterName(clusterName); err != nil {
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
			info.Data = wh.mergeDomainData(info.Data, updatedInfo.Data)
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
		}
	}
	if updateRequest.ReplicationConfiguration != nil {
		updateReplicationConfig := updateRequest.ReplicationConfiguration
		if err := validateReplicationConfig(getResponse,
			updateReplicationConfig.ActiveClusterName, updateReplicationConfig.Clusters); err != nil {
			return nil, wh.error(err, scope)
		}
	}

	if configurationChanged && activeClusterChanged {
		return nil, wh.error(errCannotDoDomainFailoverAndUpdate, scope)
	} else if configurationChanged || activeClusterChanged {
		if configurationChanged && getResponse.IsGlobalDomain && !clusterMetadata.IsMasterCluster() {
			return nil, wh.error(errNotMasterCluster, scope)
		}

		// set the versions
		if configurationChanged {
			configVersion++
		}
		if activeClusterChanged {
			failoverVersion = clusterMetadata.GetNextFailoverVersion(replicationConfig.ActiveClusterName, failoverVersion)
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
			return nil, wh.error(errors.New("domain table version is not set"), scope)
		}
		err = wh.metadataMgr.UpdateDomain(updateReq)
		if err != nil {
			return nil, wh.error(err, scope)
		}

		if getResponse.IsGlobalDomain {
			err = wh.domainReplicator.HandleTransmissionTask(replicator.DomainOperationUpdate,
				info, config, replicationConfig, configVersion, failoverVersion, getResponse.IsGlobalDomain)
			if err != nil {
				return nil, wh.error(err, scope)
			}
		}
	} else if clusterMetadata.IsGlobalDomainEnabled() && !clusterMetadata.IsMasterCluster() {
		// although there is no attr updated, just prevent customer to use the non master cluster
		// for update domain, ever (except if customer want to do a domain failover)
		return nil, wh.error(errNotMasterCluster, scope)
	}

	response := &gen.UpdateDomainResponse{
		IsGlobalDomain:  common.BoolPtr(getResponse.IsGlobalDomain),
		FailoverVersion: common.Int64Ptr(failoverVersion),
	}
	response.DomainInfo, response.Configuration, response.ReplicationConfiguration = createDomainResponse(
		info, config, replicationConfig)
	return response, nil
}

func (wh *WorkflowHandler) mergeDomainData(old map[string]string, new map[string]string) map[string]string {
	if old == nil {
		old = map[string]string{}
	}
	for k, v := range new {
		old[k] = v
	}
	return old
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED.  Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandler) DeprecateDomain(ctx context.Context, deprecateRequest *gen.DeprecateDomainRequest) error {

	scope := metrics.FrontendDeprecateDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if deprecateRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if err := wh.checkPermission(deprecateRequest.SecurityToken, scope); err != nil {
		return err
	}

	clusterMetadata := wh.GetClusterMetadata()
	// TODO remove the IsGlobalDomainEnabled check once cross DC is public
	if clusterMetadata.IsGlobalDomainEnabled() && !clusterMetadata.IsMasterCluster() {
		return wh.error(errNotMasterCluster, scope)
	}

	if deprecateRequest.GetName() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 domain table
	// and since we do not know which table will return the domain afterwards
	// this call has to be made
	metadata, err := wh.metadataMgr.GetMetadata()
	if err != nil {
		return wh.error(err, scope)
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: deprecateRequest.GetName()})
	if err != nil {
		return wh.error(err, scope)
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
		return wh.error(errors.New("domain table version is not set"), scope)
	}
	err = wh.metadataMgr.UpdateDomain(updateReq)
	if err != nil {
		return wh.error(err, scope)
	}

	if err != nil {
		return wh.error(errDomainNotSet, scope)
	}
	return nil
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx context.Context,
	pollRequest *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {

	scope := metrics.FrontendPollForActivityTaskScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if pollRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	wh.Service.GetLogger().Debug("Received PollForActivityTask")
	if pollRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.domainCache.GetDomainID(pollRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	pollerID := uuid.New()
	var resp *gen.PollForActivityTaskResponse
	op := func() error {
		var err error
		resp, err = wh.matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{
			DomainUUID:  common.StringPtr(domainID),
			PollerID:    common.StringPtr(pollerID),
			PollRequest: pollRequest,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeActivity, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			wh.Service.GetLogger().Errorf(
				"PollForActivityTask failed. TaskList: %v, Error: %v", pollRequest.TaskList.GetName(), err)
			return nil, wh.error(err, scope)
		}
	}
	return resp, nil
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx context.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {

	scope := metrics.FrontendPollForDecisionTaskScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if pollRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	if pollRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope); err != nil {
		return nil, err
	}

	domainName := pollRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debugf("Poll for decision. DomainName: %v, DomainID: %v", domainName, domainID)

	pollerID := uuid.New()
	var matchingResp *m.PollForDecisionTaskResponse
	op := func() error {
		var err error
		matchingResp, err = wh.matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{
			DomainUUID:  common.StringPtr(domainID),
			PollerID:    common.StringPtr(pollerID),
			PollRequest: pollRequest,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeDecision, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			wh.Service.GetLogger().Errorf(
				"PollForDecisionTask failed. TaskList: %v, Error: %v", pollRequest.TaskList.GetName(), err)
			return nil, wh.error(err, scope)
		}

		// Must be cancellation error.  Does'nt matter what we return here.  Client already went away.
		return nil, nil
	}

	eventStoreVersion := matchingResp.GetEventStoreVersion()
	resp, err := wh.createPollForDecisionTaskResponse(ctx, scope, domainID, matchingResp, eventStoreVersion, matchingResp.GetBranchToken())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

func (wh *WorkflowHandler) cancelOutstandingPoll(ctx context.Context, err error, domainID string, taskListType int32,
	taskList *gen.TaskList, pollerID string) error {
	// First check if this err is due to context cancellation.  This means client connection to frontend is closed.
	if ctx.Err() == context.Canceled {
		// Our rpc stack does not propagates context cancellation to the other service.  Lets make an explicit
		// call to matching to notify this poller is gone to prevent any tasks being dispatched to zombie pollers.
		err = wh.matching.CancelOutstandingPoll(context.Background(), &m.CancelOutstandingPollRequest{
			DomainUUID:   common.StringPtr(domainID),
			TaskListType: common.Int32Ptr(taskListType),
			TaskList:     taskList,
			PollerID:     common.StringPtr(pollerID),
		})
		// We can not do much if this call fails.  Just log the error and move on
		if err != nil {
			wh.Service.GetLogger().Warnf("Failed to cancel outstanding poller.  Tasklist: %v, Error: %v,",
				taskList.GetName(), err)
		}

		// Clear error as we don't want to report context cancellation error to count against our SLA
		return nil
	}

	return err
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (resp *gen.RecordActivityTaskHeartbeatResponse, err error) {

	scope := metrics.FrontendRecordActivityTaskHeartbeatScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if heartbeatRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	if heartbeatRequest.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(heartbeatRequest.Details), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: heartbeatRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonHeartbeatExceedsLimit),
			Details:   heartbeatRequest.Details[0:sizeLimitError],
			Identity:  heartbeatRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
		resp = &gen.RecordActivityTaskHeartbeatResponse{CancelRequested: common.BoolPtr(true)}
	} else {
		resp, err = wh.history.RecordActivityTaskHeartbeat(ctx, &h.RecordActivityTaskHeartbeatRequest{
			DomainUUID:       common.StringPtr(taskToken.DomainID),
			HeartbeatRequest: heartbeatRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return resp, nil
}

// RecordActivityTaskHeartbeatByID - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatByIDRequest) (resp *gen.RecordActivityTaskHeartbeatResponse, err error) {

	scope := metrics.FrontendRecordActivityTaskHeartbeatByIDScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if heartbeatRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeatByID")
	domainID, err := wh.domainCache.GetDomainID(heartbeatRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	workflowID := heartbeatRequest.GetWorkflowID()
	runID := heartbeatRequest.GetRunID() // runID is optional so can be empty
	activityID := heartbeatRequest.GetActivityID()

	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return nil, wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(heartbeatRequest.Details), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonHeartbeatExceedsLimit),
			Details:   heartbeatRequest.Details[0:sizeLimitError],
			Identity:  heartbeatRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
		resp = &gen.RecordActivityTaskHeartbeatResponse{CancelRequested: common.BoolPtr(true)}
	} else {
		req := &gen.RecordActivityTaskHeartbeatRequest{
			TaskToken: token,
			Details:   heartbeatRequest.Details,
			Identity:  heartbeatRequest.Identity,
		}

		resp, err = wh.history.RecordActivityTaskHeartbeat(ctx, &h.RecordActivityTaskHeartbeatRequest{
			DomainUUID:       common.StringPtr(taskToken.DomainID),
			HeartbeatRequest: req,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return resp, nil
}

// RespondActivityTaskCompleted - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {

	scope := metrics.FrontendRespondActivityTaskCompletedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if completeRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if completeRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(completeRequest.Result), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: completeRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonCompleteResultExceedsLimit),
			Details:   completeRequest.Result[0:sizeLimitError],
			Identity:  completeRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
		err = wh.history.RespondActivityTaskCompleted(ctx, &h.RespondActivityTaskCompletedRequest{
			DomainUUID:      common.StringPtr(taskToken.DomainID),
			CompleteRequest: completeRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	}

	return nil
}

// RespondActivityTaskCompletedByID - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompletedByID(
	ctx context.Context,
	completeRequest *gen.RespondActivityTaskCompletedByIDRequest) error {

	scope := metrics.FrontendRespondActivityTaskCompletedByIDScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if completeRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	domainID, err := wh.domainCache.GetDomainID(completeRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}
	workflowID := completeRequest.GetWorkflowID()
	runID := completeRequest.GetRunID() // runID is optional so can be empty
	activityID := completeRequest.GetActivityID()

	if domainID == "" {
		return wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return wh.error(err, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(completeRequest.Result), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonCompleteResultExceedsLimit),
			Details:   completeRequest.Result[0:sizeLimitError],
			Identity:  completeRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
		req := &gen.RespondActivityTaskCompletedRequest{
			TaskToken: token,
			Result:    completeRequest.Result,
			Identity:  completeRequest.Identity,
		}

		err = wh.history.RespondActivityTaskCompleted(ctx, &h.RespondActivityTaskCompletedRequest{
			DomainUUID:      common.StringPtr(taskToken.DomainID),
			CompleteRequest: req,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	}

	return nil
}

// RespondActivityTaskFailed - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx context.Context,
	failedRequest *gen.RespondActivityTaskFailedRequest) error {

	scope := metrics.FrontendRespondActivityTaskFailedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if failedRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if failedRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(failedRequest.Details), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		failedRequest.Reason = common.StringPtr(common.FailureReasonFailureDetailsExceedsLimit)
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
	}

	err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		FailedRequest: failedRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondActivityTaskFailedByID - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailedByID(
	ctx context.Context,
	failedRequest *gen.RespondActivityTaskFailedByIDRequest) error {

	scope := metrics.FrontendRespondActivityTaskFailedByIDScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if failedRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	domainID, err := wh.domainCache.GetDomainID(failedRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}
	workflowID := failedRequest.GetWorkflowID()
	runID := failedRequest.GetRunID() // runID is optional so can be empty
	activityID := failedRequest.GetActivityID()

	if domainID == "" {
		return wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return wh.error(err, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(failedRequest.Details), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		failedRequest.Reason = common.StringPtr(common.FailureReasonFailureDetailsExceedsLimit)
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
	}

	req := &gen.RespondActivityTaskFailedRequest{
		TaskToken: token,
		Reason:    failedRequest.Reason,
		Details:   failedRequest.Details,
		Identity:  failedRequest.Identity,
	}

	err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		FailedRequest: req,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondActivityTaskCanceled - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceled(
	ctx context.Context,
	cancelRequest *gen.RespondActivityTaskCanceledRequest) error {

	scope := metrics.FrontendRespondActivityTaskCanceledScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if cancelRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if cancelRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(cancelRequest.Details), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: cancelRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonCancelDetailsExceedsLimit),
			Details:   cancelRequest.Details[0:sizeLimitError],
			Identity:  cancelRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
		err = wh.history.RespondActivityTaskCanceled(ctx, &h.RespondActivityTaskCanceledRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			CancelRequest: cancelRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	}

	return nil
}

// RespondActivityTaskCanceledByID - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceledByID(
	ctx context.Context,
	cancelRequest *gen.RespondActivityTaskCanceledByIDRequest) error {

	scope := metrics.FrontendRespondActivityTaskCanceledScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if cancelRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	domainID, err := wh.domainCache.GetDomainID(cancelRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}
	workflowID := cancelRequest.GetWorkflowID()
	runID := cancelRequest.GetRunID() // runID is optional so can be empty
	activityID := cancelRequest.GetActivityID()

	if domainID == "" {
		return wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return wh.error(err, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(cancelRequest.Details), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonCancelDetailsExceedsLimit),
			Details:   cancelRequest.Details[0:sizeLimitError],
			Identity:  cancelRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
		req := &gen.RespondActivityTaskCanceledRequest{
			TaskToken: token,
			Details:   cancelRequest.Details,
			Identity:  cancelRequest.Identity,
		}

		err = wh.history.RespondActivityTaskCanceled(ctx, &h.RespondActivityTaskCanceledRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			CancelRequest: req,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	}

	return nil
}

// RespondDecisionTaskCompleted - response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) (*gen.RespondDecisionTaskCompletedResponse, error) {

	scope := metrics.FrontendRespondDecisionTaskCompletedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if completeRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if completeRequest.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	histResp, err := wh.history.RespondDecisionTaskCompleted(ctx, &h.RespondDecisionTaskCompletedRequest{
		DomainUUID:      common.StringPtr(taskToken.DomainID),
		CompleteRequest: completeRequest},
	)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	completedResp := &gen.RespondDecisionTaskCompletedResponse{}
	if completeRequest.GetReturnNewDecisionTask() && histResp != nil && histResp.StartedResponse != nil {
		taskToken := &common.TaskToken{
			DomainID:        taskToken.DomainID,
			WorkflowID:      taskToken.WorkflowID,
			RunID:           taskToken.RunID,
			ScheduleID:      histResp.StartedResponse.GetScheduledEventId(),
			ScheduleAttempt: histResp.StartedResponse.GetAttempt(),
		}
		token, _ := wh.tokenSerializer.Serialize(taskToken)
		workflowExecution := &gen.WorkflowExecution{
			WorkflowId: common.StringPtr(taskToken.WorkflowID),
			RunId:      common.StringPtr(taskToken.RunID),
		}
		matchingResp := common.CreateMatchingPollForDecisionTaskResponse(histResp.StartedResponse, workflowExecution, token)

		eventStoreVersion := matchingResp.GetEventStoreVersion()
		newDecisionTask, err := wh.createPollForDecisionTaskResponse(ctx, scope, taskToken.DomainID, matchingResp, eventStoreVersion, matchingResp.GetBranchToken())
		if err != nil {
			return nil, wh.error(err, scope)
		}
		completedResp.DecisionTask = newDecisionTask
	}

	return completedResp, nil
}

// RespondDecisionTaskFailed - failed response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskFailed(
	ctx context.Context,
	failedRequest *gen.RespondDecisionTaskFailedRequest) error {

	scope := metrics.FrontendRespondDecisionTaskFailedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if failedRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if failedRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(len(failedRequest.Details), sizeLimitWarn, sizeLimitError,
		taskToken.DomainID, taskToken.WorkflowID, taskToken.RunID,
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		// details exceed, we would just truncate the size for decision task failed as the details is not used anywhere by client code
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
	}

	err = wh.history.RespondDecisionTaskFailed(ctx, &h.RespondDecisionTaskFailedRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		FailedRequest: failedRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondQueryTaskCompleted - response to a query task
func (wh *WorkflowHandler) RespondQueryTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondQueryTaskCompletedRequest) error {

	scope := metrics.FrontendRespondQueryTaskCompletedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if completeRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if completeRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	queryTaskToken, err := wh.tokenSerializer.DeserializeQueryTaskToken(completeRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if queryTaskToken.DomainID == "" || queryTaskToken.TaskList == "" || queryTaskToken.TaskID == "" {
		return wh.error(errInvalidTaskToken, scope)
	}

	matchingRequest := &m.RespondQueryTaskCompletedRequest{
		DomainUUID:       common.StringPtr(queryTaskToken.DomainID),
		TaskList:         &gen.TaskList{Name: common.StringPtr(queryTaskToken.TaskList)},
		TaskID:           common.StringPtr(queryTaskToken.TaskID),
		CompletedRequest: completeRequest,
	}

	err = wh.matching.RespondQueryTaskCompleted(ctx, matchingRequest)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// StartWorkflowExecution - Creates a new workflow execution
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx context.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {

	scope := metrics.FrontendStartWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if startRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if startRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if startRequest.GetWorkflowId() == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}

	if err := common.ValidateRetryPolicy(startRequest.RetryPolicy); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := common.ValidateCronSchedule(startRequest.GetCronSchedule()); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debugf(
		"Received StartWorkflowExecution. WorkflowID: %v",
		startRequest.GetWorkflowId())

	if startRequest.WorkflowType == nil || startRequest.WorkflowType.GetName() == "" {
		return nil, wh.error(errWorkflowTypeNotSet, scope)
	}

	if err := wh.validateTaskList(startRequest.TaskList, scope); err != nil {
		return nil, err
	}

	if startRequest.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidExecutionStartToCloseTimeoutSeconds, scope)
	}

	if startRequest.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidTaskStartToCloseTimeoutSeconds, scope)
	}

	if startRequest.GetRequestId() == "" {
		return nil, wh.error(errRequestIDNotSet, scope)
	}

	maxDecisionTimeout := int32(wh.config.MaxDecisionStartToCloseTimeout(startRequest.GetDomain()))
	// TODO: remove this assignment and logging in future, so that frontend will just return bad request for large decision timeout
	if startRequest.GetTaskStartToCloseTimeoutSeconds() > startRequest.GetExecutionStartToCloseTimeoutSeconds() {
		logging.LogDecisionTimeoutLargerThanWorkflowTimeout(wh.Service.GetLogger(),
			startRequest.GetTaskStartToCloseTimeoutSeconds(),
			startRequest.GetDomain(),
			startRequest.GetWorkflowId(),
			startRequest.WorkflowType.GetName(),
		)
		startRequest.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(startRequest.GetExecutionStartToCloseTimeoutSeconds())
	}
	if startRequest.GetTaskStartToCloseTimeoutSeconds() > maxDecisionTimeout {
		logging.LogDecisionTimeoutTooLarge(wh.Service.GetLogger(),
			startRequest.GetTaskStartToCloseTimeoutSeconds(),
			startRequest.GetDomain(),
			startRequest.GetWorkflowId(),
			startRequest.WorkflowType.GetName(),
		)
		startRequest.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(maxDecisionTimeout)
	}
	if startRequest.GetTaskStartToCloseTimeoutSeconds() > startRequest.GetExecutionStartToCloseTimeoutSeconds() ||
		startRequest.GetTaskStartToCloseTimeoutSeconds() > maxDecisionTimeout {
		return nil, wh.error(&gen.BadRequestError{
			Message: fmt.Sprintf("TaskStartToCloseTimeoutSeconds is larger than ExecutionStartToCloseTimeout or MaxDecisionStartToCloseTimeout (%ds).", maxDecisionTimeout)}, scope)
	}

	domainName := startRequest.GetDomain()
	wh.Service.GetLogger().Debugf("Start workflow execution request domain: %v", domainName)
	domainID, err := wh.domainCache.GetDomainID(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(startRequest.GetDomain())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(startRequest.GetDomain())
	if err := common.CheckEventBlobSizeLimit(len(startRequest.Input), sizeLimitWarn, sizeLimitError, domainID,
		startRequest.GetWorkflowId(), "", wh.metricsClient, scope, wh.GetLogger()); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debugf("Start workflow execution request domainID: %v", domainID)

	resp, err := wh.history.StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(domainID, startRequest))

	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

// GetWorkflowExecutionHistory - retrieves the history of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx context.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {

	scope := metrics.FrontendGetWorkflowExecutionHistoryScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if getRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if getRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(getRequest.Execution, scope); err != nil {
		return nil, err
	}

	if getRequest.GetMaximumPageSize() <= 0 {
		getRequest.MaximumPageSize = common.Int32Ptr(int32(wh.config.HistoryMaxPageSize(getRequest.GetDomain())))
	}

	domainID, err := wh.domainCache.GetDomainID(getRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// force limit page size if exceed
	if getRequest.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.GetLogger().WithFields(bark.Fields{
			logging.TagWorkflowExecutionID: getRequest.Execution.GetWorkflowId(),
			logging.TagWorkflowRunID:       getRequest.Execution.GetRunId(),
			logging.TagDomainID:            domainID,
			logging.TagSize:                getRequest.GetMaximumPageSize(),
		}).Warn("GetHistory page size is larger than threshold")

		getRequest.MaximumPageSize = common.Int32Ptr(common.GetHistoryMaxPageSize)
	}

	// this function return the following 5 things,
	// 1. the workflow run ID
	// 2. the last first event ID (the event ID of the last batch of events in the history)
	// 3. the next event ID
	// 4. whether the workflow is closed
	// 5. error if any
	queryHistory := func(domainUUID string, execution *gen.WorkflowExecution, expectedNextEventID int64) (int32, []byte, string, int64, int64, bool, error) {
		response, err := wh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
			DomainUUID:          common.StringPtr(domainUUID),
			Execution:           execution,
			ExpectedNextEventId: common.Int64Ptr(expectedNextEventID),
		})

		if err != nil {
			return 0, nil, "", 0, 0, false, err
		}

		return response.GetEventStoreVersion(), response.BranchToken, response.Execution.GetRunId(), response.GetLastFirstEventId(), response.GetNextEventId(), response.GetIsWorkflowRunning(), nil
	}

	isLongPoll := getRequest.GetWaitForNewEvent()
	isCloseEventOnly := getRequest.GetHistoryEventFilterType() == gen.HistoryEventFilterTypeCloseEvent
	execution := getRequest.Execution
	token := &getHistoryContinuationToken{}

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if getRequest.NextPageToken != nil {
		token, err = deserializeHistoryToken(getRequest.NextPageToken)
		if err != nil {
			return nil, wh.error(errInvalidNextPageToken, scope)
		}
		if execution.RunId != nil && execution.GetRunId() != token.RunID {
			return nil, wh.error(errNextPageTokenRunIDMismatch, scope)
		}

		execution.RunId = common.StringPtr(token.RunID)

		// we need to update the current next event ID and whether workflow is running
		if len(token.PersistenceToken) == 0 && isLongPoll && token.IsWorkflowRunning {
			if !isCloseEventOnly {
				queryNextEventID = token.NextEventID
			}
			token.EventStoreVersion, token.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, err = queryHistory(domainID, execution, queryNextEventID)
			if err != nil {
				return nil, wh.error(err, scope)
			}

			token.FirstEventID = token.NextEventID
			token.NextEventID = nextEventID
			token.IsWorkflowRunning = isWorkflowRunning
		}
	} else {
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		token.EventStoreVersion, token.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, err = queryHistory(domainID, execution, queryNextEventID)
		if err != nil {
			return nil, wh.error(err, scope)
		}

		execution.RunId = &runID

		token.RunID = runID
		token.FirstEventID = common.FirstEventID
		token.NextEventID = nextEventID
		token.IsWorkflowRunning = isWorkflowRunning
		token.PersistenceToken = nil
	}

	history := &gen.History{}
	history.Events = []*gen.HistoryEvent{}
	if isCloseEventOnly {
		if !isWorkflowRunning {
			history, _, err = wh.getHistory(scope, domainID, *execution, lastFirstEventID, nextEventID,
				getRequest.GetMaximumPageSize(), nil, token.TransientDecision, token.EventStoreVersion, token.BranchToken)
			if err != nil {
				return nil, wh.error(err, scope)
			}
			// since getHistory func will not return empty history, so the below is safe
			history.Events = history.Events[len(history.Events)-1 : len(history.Events)]
			token = nil
		} else if isLongPoll {
			// set the persistence token to be nil so next time we will query history for updates
			token.PersistenceToken = nil
		} else {
			token = nil
		}
	} else {
		// return all events
		if token.FirstEventID >= token.NextEventID {
			// currently there is no new event
			history.Events = []*gen.HistoryEvent{}
			if !isWorkflowRunning {
				token = nil
			}
		} else {
			history, token.PersistenceToken, err =
				wh.getHistory(scope, domainID, *execution, token.FirstEventID, token.NextEventID,
					getRequest.GetMaximumPageSize(), token.PersistenceToken, token.TransientDecision, token.EventStoreVersion, token.BranchToken)
			if err != nil {
				return nil, wh.error(err, scope)
			}

			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(token.PersistenceToken) == 0 && (!token.IsWorkflowRunning || !isLongPoll) {
				// meaning, there is no more history to be returned
				token = nil
			}
		}
	}

	nextToken, err := serializeHistoryToken(token)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return createGetWorkflowExecutionHistoryResponse(history, nextToken), nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(ctx context.Context,
	signalRequest *gen.SignalWorkflowExecutionRequest) error {

	scope := metrics.FrontendSignalWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if signalRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if signalRequest.GetDomain() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(signalRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	if signalRequest.GetSignalName() == "" {
		return wh.error(&gen.BadRequestError{Message: "SignalName is not set on request."}, scope)
	}

	domainID, err := wh.domainCache.GetDomainID(signalRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(signalRequest.GetDomain())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(signalRequest.GetDomain())
	if err := common.CheckEventBlobSizeLimit(len(signalRequest.Input), sizeLimitWarn, sizeLimitError, domainID,
		signalRequest.GetWorkflowExecution().GetWorkflowId(), signalRequest.GetWorkflowExecution().GetWorkflowId(),
		wh.metricsClient, scope, wh.GetLogger()); err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.SignalWorkflowExecution(ctx, &h.SignalWorkflowExecutionRequest{
		DomainUUID:    common.StringPtr(domainID),
		SignalRequest: signalRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a decision task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a decision task being created for the execution
func (wh *WorkflowHandler) SignalWithStartWorkflowExecution(ctx context.Context,
	signalWithStartRequest *gen.SignalWithStartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {

	scope := metrics.FrontendSignalWithStartWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if signalWithStartRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if signalWithStartRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if signalWithStartRequest.GetWorkflowId() == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "WorkflowId is not set on request."}, scope)
	}

	if signalWithStartRequest.GetSignalName() == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "SignalName is not set on request."}, scope)
	}

	if signalWithStartRequest.WorkflowType == nil || signalWithStartRequest.WorkflowType.GetName() == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "WorkflowType is not set on request."}, scope)
	}

	if err := wh.validateTaskList(signalWithStartRequest.TaskList, scope); err != nil {
		return nil, err
	}

	if signalWithStartRequest.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "A valid ExecutionStartToCloseTimeoutSeconds is not set on request."}, scope)
	}

	if signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "A valid TaskStartToCloseTimeoutSeconds is not set on request."}, scope)
	}

	if err := common.ValidateRetryPolicy(signalWithStartRequest.RetryPolicy); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := common.ValidateCronSchedule(signalWithStartRequest.GetCronSchedule()); err != nil {
		return nil, wh.error(err, scope)
	}

	maxDecisionTimeout := int32(wh.config.MaxDecisionStartToCloseTimeout(signalWithStartRequest.GetDomain()))
	// TODO: remove this assignment and logging in future, so that frontend will just return bad request for large decision timeout
	if signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds() > signalWithStartRequest.GetExecutionStartToCloseTimeoutSeconds() {
		logging.LogDecisionTimeoutLargerThanWorkflowTimeout(wh.Service.GetLogger(),
			signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds(),
			signalWithStartRequest.GetDomain(),
			signalWithStartRequest.GetWorkflowId(),
			signalWithStartRequest.WorkflowType.GetName(),
		)
		signalWithStartRequest.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(signalWithStartRequest.GetExecutionStartToCloseTimeoutSeconds())
	}
	if signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds() > maxDecisionTimeout {
		logging.LogDecisionTimeoutTooLarge(wh.Service.GetLogger(),
			signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds(),
			signalWithStartRequest.GetDomain(),
			signalWithStartRequest.GetWorkflowId(),
			signalWithStartRequest.WorkflowType.GetName(),
		)
		signalWithStartRequest.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(maxDecisionTimeout)
	}
	if signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds() > signalWithStartRequest.GetExecutionStartToCloseTimeoutSeconds() ||
		signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds() > maxDecisionTimeout {
		return nil, wh.error(&gen.BadRequestError{
			Message: fmt.Sprintf("TaskStartToCloseTimeoutSeconds is larger than ExecutionStartToCloseTimeout or MaxDecisionStartToCloseTimeout (%ds).", maxDecisionTimeout)}, scope)
	}

	domainID, err := wh.domainCache.GetDomainID(signalWithStartRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(signalWithStartRequest.GetDomain())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(signalWithStartRequest.GetDomain())
	if err := common.CheckEventBlobSizeLimit(len(signalWithStartRequest.SignalInput), sizeLimitWarn, sizeLimitError, domainID,
		signalWithStartRequest.GetWorkflowId(), "", wh.metricsClient, scope, wh.GetLogger()); err != nil {
		return nil, wh.error(err, scope)
	}
	if err := common.CheckEventBlobSizeLimit(len(signalWithStartRequest.Input), sizeLimitWarn, sizeLimitError, domainID,
		signalWithStartRequest.GetWorkflowId(), "", wh.metricsClient, scope, wh.GetLogger()); err != nil {
		return nil, wh.error(err, scope)
	}

	var resp *gen.StartWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = wh.history.SignalWithStartWorkflowExecution(ctx, &h.SignalWithStartWorkflowExecutionRequest{
			DomainUUID:             common.StringPtr(domainID),
			SignalWithStartRequest: signalWithStartRequest,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return resp, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandler) TerminateWorkflowExecution(ctx context.Context,
	terminateRequest *gen.TerminateWorkflowExecutionRequest) error {

	scope := metrics.FrontendTerminateWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if terminateRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if terminateRequest.GetDomain() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(terminateRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	domainID, err := wh.domainCache.GetDomainID(terminateRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.TerminateWorkflowExecution(ctx, &h.TerminateWorkflowExecutionRequest{
		DomainUUID:       common.StringPtr(domainID),
		TerminateRequest: terminateRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// RequestCancelWorkflowExecution - requests to cancel a workflow execution
func (wh *WorkflowHandler) RequestCancelWorkflowExecution(
	ctx context.Context,
	cancelRequest *gen.RequestCancelWorkflowExecutionRequest) error {

	scope := metrics.FrontendRequestCancelWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if cancelRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if cancelRequest.GetDomain() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(cancelRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	domainID, err := wh.domainCache.GetDomainID(cancelRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.RequestCancelWorkflowExecution(ctx, &h.RequestCancelWorkflowExecutionRequest{
		DomainUUID:    common.StringPtr(domainID),
		CancelRequest: cancelRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// ListOpenWorkflowExecutions - retrieves info for open workflow executions in a domain
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(ctx context.Context,
	listRequest *gen.ListOpenWorkflowExecutionsRequest) (*gen.ListOpenWorkflowExecutionsResponse, error) {

	scope := metrics.FrontendListOpenWorkflowExecutionsScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if listRequest.StartTimeFilter == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.EarliestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "EarliestTime in StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.LatestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "LatestTime in StartTimeFilter is required"}, scope)
	}

	if listRequest.ExecutionFilter != nil && listRequest.TypeFilter != nil {
		return nil, wh.error(&gen.BadRequestError{
			Message: "Only one of ExecutionFilter or TypeFilter is allowed"}, scope)
	}

	if listRequest.GetMaximumPageSize() <= 0 {
		listRequest.MaximumPageSize = common.Int32Ptr(int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain())))
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainID,
		Domain:            domain,
		PageSize:          int(listRequest.GetMaximumPageSize()),
		NextPageToken:     listRequest.NextPageToken,
		EarliestStartTime: listRequest.StartTimeFilter.GetEarliestTime(),
		LatestStartTime:   listRequest.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutionsByWorkflowID(
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowId(),
				})
		}
		logging.LogListOpenWorkflowByFilter(wh.GetLogger(), listRequest.GetDomain(), logging.ListWorkflowFilterByID)
	} else if listRequest.TypeFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              listRequest.TypeFilter.GetName(),
			})
		}
		logging.LogListOpenWorkflowByFilter(wh.GetLogger(), listRequest.GetDomain(), logging.ListWorkflowFilterByType)
	} else {
		persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp := &gen.ListOpenWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListClosedWorkflowExecutions - retrieves info for closed workflow executions in a domain
func (wh *WorkflowHandler) ListClosedWorkflowExecutions(ctx context.Context,
	listRequest *gen.ListClosedWorkflowExecutionsRequest) (*gen.ListClosedWorkflowExecutionsResponse, error) {

	scope := metrics.FrontendListClosedWorkflowExecutionsScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if listRequest.StartTimeFilter == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.EarliestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "EarliestTime in StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.LatestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "LatestTime in StartTimeFilter is required"}, scope)
	}

	filterCount := 0
	if listRequest.ExecutionFilter != nil {
		filterCount++
	}
	if listRequest.TypeFilter != nil {
		filterCount++
	}
	if listRequest.StatusFilter != nil {
		filterCount++
	}

	if filterCount > 1 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed"}, scope)
	}

	if listRequest.GetMaximumPageSize() <= 0 {
		listRequest.MaximumPageSize = common.Int32Ptr(int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain())))
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainID,
		Domain:            domain,
		PageSize:          int(listRequest.GetMaximumPageSize()),
		NextPageToken:     listRequest.NextPageToken,
		EarliestStartTime: listRequest.StartTimeFilter.GetEarliestTime(),
		LatestStartTime:   listRequest.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByWorkflowID(
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowId(),
				})
		}
		logging.LogListClosedWorkflowByFilter(wh.GetLogger(), listRequest.GetDomain(), logging.ListWorkflowFilterByID)
	} else if listRequest.TypeFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              listRequest.TypeFilter.GetName(),
			})
		}
		logging.LogListClosedWorkflowByFilter(wh.GetLogger(), listRequest.GetDomain(), logging.ListWorkflowFilterByType)
	} else if listRequest.StatusFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByStatus(&persistence.ListClosedWorkflowExecutionsByStatusRequest{
				ListWorkflowExecutionsRequest: baseReq,
				Status:                        listRequest.GetStatusFilter(),
			})
		}
		logging.LogListClosedWorkflowByFilter(wh.GetLogger(), listRequest.GetDomain(), logging.ListWorkflowFilterByStatus)
	} else {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp := &gen.ListClosedWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ResetStickyTaskList reset the volatile information in mutable state of a given workflow.
func (wh *WorkflowHandler) ResetStickyTaskList(ctx context.Context, resetRequest *gen.ResetStickyTaskListRequest) (*gen.ResetStickyTaskListResponse, error) {
	scope := metrics.FrontendResetStickyTaskListScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if resetRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if resetRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(resetRequest.Execution, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.domainCache.GetDomainID(resetRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	_, err = wh.history.ResetStickyTaskList(ctx, &h.ResetStickyTaskListRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  resetRequest.Execution,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &gen.ResetStickyTaskListResponse{}, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandler) QueryWorkflow(ctx context.Context,
	queryRequest *gen.QueryWorkflowRequest) (*gen.QueryWorkflowResponse, error) {

	scope := metrics.FrontendQueryWorkflowScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if queryRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if queryRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(queryRequest.Execution, scope); err != nil {
		return nil, err
	}

	if queryRequest.Query == nil {
		return nil, wh.error(errQueryNotSet, scope)
	}

	if queryRequest.Query.GetQueryType() == "" {
		return nil, wh.error(errQueryTypeNotSet, scope)
	}

	domainID, err := wh.domainCache.GetDomainID(queryRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	matchingRequest := &m.QueryWorkflowRequest{
		DomainUUID:   common.StringPtr(domainID),
		QueryRequest: queryRequest,
	}

	// we should always use the mutable state, since it contains the sticky tasklist information
	response, err := wh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  queryRequest.Execution,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	clientFeature := client.NewFeatureImpl(
		response.GetClientLibraryVersion(),
		response.GetClientFeatureVersion(),
		response.GetClientImpl(),
	)

	queryRequest.Execution.RunId = response.Execution.RunId
	if len(response.StickyTaskList.GetName()) != 0 && clientFeature.SupportStickyQuery() {
		matchingRequest.TaskList = response.StickyTaskList
		stickyDecisionTimeout := response.GetStickyTaskListScheduleToStartTimeout()
		// using a clean new context in case customer provide a context which has
		// a really short deadline, causing we clear the stickyness
		stickyContext, cancel := context.WithTimeout(context.Background(), time.Duration(stickyDecisionTimeout)*time.Second)
		matchingResp, err := wh.matchingRawClient.QueryWorkflow(stickyContext, matchingRequest)
		cancel()
		if err == nil {
			return matchingResp, nil
		}
		if yarpcError, ok := err.(*yarpcerrors.Status); !ok || yarpcError.Code() != yarpcerrors.CodeDeadlineExceeded {
			// this means query failure
			logging.LogQueryTaskFailedEvent(wh.GetLogger(),
				queryRequest.GetDomain(),
				queryRequest.Execution.GetWorkflowId(),
				queryRequest.Execution.GetRunId(),
				queryRequest.Query.GetQueryType())
			return nil, wh.error(err, scope)
		}
		// this means sticky timeout, should try using the normal tasklist
		// we should clear the stickyness of this workflow
		resetContext, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = wh.history.ResetStickyTaskList(resetContext, &h.ResetStickyTaskListRequest{
			DomainUUID: common.StringPtr(domainID),
			Execution:  queryRequest.Execution,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	matchingRequest.TaskList = response.TaskList
	matchingResp, err := wh.matching.QueryWorkflow(ctx, matchingRequest)
	if err != nil {
		logging.LogQueryTaskFailedEvent(wh.GetLogger(),
			queryRequest.GetDomain(),
			queryRequest.Execution.GetWorkflowId(),
			queryRequest.Execution.GetRunId(),
			queryRequest.Query.GetQueryType())
		return nil, wh.error(err, scope)
	}

	return matchingResp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandler) DescribeWorkflowExecution(ctx context.Context, request *gen.DescribeWorkflowExecutionRequest) (*gen.DescribeWorkflowExecutionResponse, error) {

	scope := metrics.FrontendDescribeWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	domainID, err := wh.domainCache.GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	response, err := wh.history.DescribeWorkflowExecution(ctx, &h.DescribeWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		Request:    request,
	})

	if err != nil {
		return nil, wh.error(err, scope)
	}

	return response, nil
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes.
func (wh *WorkflowHandler) DescribeTaskList(ctx context.Context, request *gen.DescribeTaskListRequest) (*gen.DescribeTaskListResponse, error) {

	scope := metrics.FrontendDescribeTaskListScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	domainID, err := wh.domainCache.GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}

	if err := wh.validateTaskListType(request.TaskListType, scope); err != nil {
		return nil, err
	}

	var response *gen.DescribeTaskListResponse
	op := func() error {
		var err error
		response, err = wh.matching.DescribeTaskList(ctx, &m.DescribeTaskListRequest{
			DomainUUID:  common.StringPtr(domainID),
			DescRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return response, nil
}

func (wh *WorkflowHandler) getHistory(scope int, domainID string, execution gen.WorkflowExecution,
	firstEventID, nextEventID int64, pageSize int32, nextPageToken []byte,
	transientDecision *gen.TransientDecisionInfo, eventStoreVersion int32, branchToken []byte) (*gen.History, []byte, error) {

	historyEvents := []*gen.HistoryEvent{}
	var size int
	if eventStoreVersion == persistence.EventStoreVersionV2 {
		response, err := wh.historyV2Mgr.ReadHistoryBranch(&persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      int(pageSize),
			NextPageToken: nextPageToken,
		})

		if err != nil {
			return nil, nil, err
		}
		historyEvents = append(historyEvents, response.History...)
		nextPageToken = response.NextPageToken
		size = response.Size
	} else {
		response, err := wh.historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
			DomainID:      domainID,
			Execution:     execution,
			FirstEventID:  firstEventID,
			NextEventID:   nextEventID,
			PageSize:      int(pageSize),
			NextPageToken: nextPageToken,
		})

		if err != nil {
			return nil, nil, err
		}
		historyEvents = append(historyEvents, response.History.Events...)
		nextPageToken = response.NextPageToken
		size = response.Size
	}

	if len(historyEvents) > 0 {
		wh.metricsClient.RecordTimer(scope, metrics.HistorySize, time.Duration(size))

		if size > common.GetHistoryWarnSizeLimit {
			wh.GetLogger().WithFields(bark.Fields{
				logging.TagWorkflowExecutionID: execution.GetWorkflowId(),
				logging.TagWorkflowRunID:       execution.GetRunId(),
				logging.TagDomainID:            domainID,
				logging.TagSize:                size,
			}).Warn("GetHistory size threshold breached")
		}
	}

	if len(nextPageToken) == 0 && transientDecision != nil {
		// Append the transient decision events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientDecision.ScheduledEvent, transientDecision.StartedEvent)
	}

	executionHistory := &gen.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nextPageToken, nil
}

func (wh *WorkflowHandler) getLoggerForTask(taskToken []byte) bark.Logger {
	logger := wh.Service.GetLogger()
	task, err := wh.tokenSerializer.Deserialize(taskToken)
	if err == nil {
		logger = logger.WithFields(bark.Fields{
			"WorkflowID": task.WorkflowID,
			"RunID":      task.RunID,
			"ScheduleID": task.ScheduleID,
		})
	}
	return logger
}

// startRequestProfile initiates recording of request metrics
func (wh *WorkflowHandler) startRequestProfile(scope int) tally.Stopwatch {
	wh.startWG.Wait()
	sw := wh.metricsClient.StartTimer(scope, metrics.CadenceLatency)
	wh.metricsClient.IncCounter(scope, metrics.CadenceRequests)
	return sw
}

func (wh *WorkflowHandler) error(err error, scope int) error {
	switch err := err.(type) {
	case *gen.InternalServiceError:
		logging.LogInternalServiceError(wh.Service.GetLogger(), err)
		wh.metricsClient.IncCounter(scope, metrics.CadenceFailures)
		// NOTE: For internal error, we won't return thrift error from cadence-frontend.
		// Because in uber internal metrics, thrift errors are counted as user errors
		return fmt.Errorf("Cadence internal error, msg: %v", err.Message)
	case *gen.BadRequestError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.DomainNotActiveError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.ServiceBusyError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrServiceBusyCounter)
		return err
	case *gen.EntityNotExistsError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
		return err
	case *gen.WorkflowExecutionAlreadyStartedError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
		return err
	case *gen.DomainAlreadyExistsError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrDomainAlreadyExistsCounter)
		return err
	case *gen.CancellationAlreadyRequestedError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrCancellationAlreadyRequestedCounter)
		return err
	case *gen.QueryFailedError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrQueryFailedCounter)
		return err
	case *gen.LimitExceededError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrLimitExceededCounter)
		return err
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			wh.metricsClient.IncCounter(scope, metrics.CadenceErrContextTimeoutCounter)
			return err
		}
	}

	logging.LogUncategorizedError(wh.Service.GetLogger(), err)
	wh.metricsClient.IncCounter(scope, metrics.CadenceFailures)
	return fmt.Errorf("Cadence internal uncategorized error, msg: %v", err.Error())
}

func (wh *WorkflowHandler) validateTaskListType(t *gen.TaskListType, scope int) error {
	if t == nil {
		return wh.error(errTaskListTypeNotSet, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateTaskList(t *gen.TaskList, scope int) error {
	if t == nil || t.Name == nil || t.GetName() == "" {
		return wh.error(errTaskListNotSet, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateExecutionAndEmitMetrics(w *gen.WorkflowExecution, scope int) error {
	err := validateExecution(w)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

func validateExecution(w *gen.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.WorkflowId == nil || w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}

func getDomainStatus(info *persistence.DomainInfo) *gen.DomainStatus {
	switch info.Status {
	case persistence.DomainStatusRegistered:
		v := gen.DomainStatusRegistered
		return &v
	case persistence.DomainStatusDeprecated:
		v := gen.DomainStatusDeprecated
		return &v
	case persistence.DomainStatusDeleted:
		v := gen.DomainStatusDeleted
		return &v
	}

	return nil
}

func createDomainResponse(info *persistence.DomainInfo, config *persistence.DomainConfig,
	replicationConfig *persistence.DomainReplicationConfig) (*gen.DomainInfo,
	*gen.DomainConfiguration, *gen.DomainReplicationConfiguration) {

	infoResult := &gen.DomainInfo{
		Name:        common.StringPtr(info.Name),
		Status:      getDomainStatus(info),
		Description: common.StringPtr(info.Description),
		OwnerEmail:  common.StringPtr(info.OwnerEmail),
		Data:        info.Data,
	}

	configResult := &gen.DomainConfiguration{
		EmitMetric:                             common.BoolPtr(config.EmitMetric),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(config.Retention),
	}

	clusters := []*gen.ClusterReplicationConfiguration{}
	for _, cluster := range replicationConfig.Clusters {
		clusters = append(clusters, &gen.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(cluster.ClusterName),
		})
	}

	replicationConfigResult := &gen.DomainReplicationConfiguration{
		ActiveClusterName: common.StringPtr(replicationConfig.ActiveClusterName),
		Clusters:          clusters,
	}

	return infoResult, configResult, replicationConfigResult
}

func (wh *WorkflowHandler) createPollForDecisionTaskResponse(ctx context.Context, scope int, domainID string,
	matchingResp *m.PollForDecisionTaskResponse, eventStoreVersion int32, branchToken []byte) (*gen.PollForDecisionTaskResponse, error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no decision task to be send to worker / caller
		return &gen.PollForDecisionTaskResponse{}, nil
	}

	var history *gen.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &gen.History{
			Events: []*gen.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventId()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventId() + 1
		}
		domain, dErr := wh.domainCache.GetDomainByID(domainID)
		if dErr != nil {
			return nil, dErr
		}
		history, persistenceToken, err = wh.getHistory(
			scope,
			domainID,
			*matchingResp.WorkflowExecution,
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(domain.GetInfo().Name)),
			nil,
			matchingResp.DecisionInfo, eventStoreVersion, branchToken)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = serializeHistoryToken(&getHistoryContinuationToken{
				RunID:             matchingResp.WorkflowExecution.GetRunId(),
				FirstEventID:      firstEventID,
				NextEventID:       nextEventID,
				PersistenceToken:  persistenceToken,
				TransientDecision: matchingResp.DecisionInfo,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &gen.PollForDecisionTaskResponse{
		TaskToken:                 matchingResp.TaskToken,
		WorkflowExecution:         matchingResp.WorkflowExecution,
		WorkflowType:              matchingResp.WorkflowType,
		PreviousStartedEventId:    matchingResp.PreviousStartedEventId,
		StartedEventId:            matchingResp.StartedEventId,
		Query:                     matchingResp.Query,
		BacklogCountHint:          matchingResp.BacklogCountHint,
		Attempt:                   matchingResp.Attempt,
		History:                   history,
		NextPageToken:             continuation,
		WorkflowExecutionTaskList: matchingResp.WorkflowExecutionTaskList,
	}

	return resp, nil
}

func createGetWorkflowExecutionHistoryResponse(
	history *gen.History, nextPageToken []byte) *gen.GetWorkflowExecutionHistoryResponse {
	resp := &gen.GetWorkflowExecutionHistoryResponse{}
	resp.History = history
	resp.NextPageToken = nextPageToken
	return resp
}

func deserializeHistoryToken(bytes []byte) (*getHistoryContinuationToken, error) {
	token := &getHistoryContinuationToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func serializeHistoryToken(token *getHistoryContinuationToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(token)
	return bytes, err
}

func createServiceBusyError() *gen.ServiceBusyError {
	err := &gen.ServiceBusyError{}
	err.Message = "Too many outstanding requests to the cadence service"
	return err
}

func isFailoverRequest(updateRequest *gen.UpdateDomainRequest) bool {
	return updateRequest.ReplicationConfiguration != nil && updateRequest.ReplicationConfiguration.ActiveClusterName != nil
}

func (wh *WorkflowHandler) validateClusterName(clusterName string) error {
	clusterMetadata := wh.GetClusterMetadata()
	if _, ok := clusterMetadata.GetAllClusterFailoverVersions()[clusterName]; !ok {
		errMsg := "Invalid cluster name: %s"
		return &gen.BadRequestError{Message: fmt.Sprintf(errMsg, clusterName)}
	}
	return nil
}
