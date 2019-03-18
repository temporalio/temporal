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
	"fmt"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/elasticsearch"
	persistencefactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Config represents configuration for cadence-frontend service
type Config struct {
	PersistenceMaxQPS               dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize           dynamicconfig.IntPropertyFnWithDomainFilter
	EnableVisibilitySampling        dynamicconfig.BoolPropertyFn
	EnableReadFromClosedExecutionV2 dynamicconfig.BoolPropertyFn
	VisibilityListMaxQPS            dynamicconfig.IntPropertyFnWithDomainFilter
	EnableVisibilityToKafka         dynamicconfig.BoolPropertyFn
	EnableReadVisibilityFromES      dynamicconfig.BoolPropertyFnWithDomainFilter
	ESVisibilityListMaxQPS          dynamicconfig.IntPropertyFnWithDomainFilter
	HistoryMaxPageSize              dynamicconfig.IntPropertyFnWithDomainFilter
	RPS                             dynamicconfig.IntPropertyFn
	MaxIDLengthLimit                dynamicconfig.IntPropertyFn
	EnableReadHistoryFromArchival   dynamicconfig.BoolPropertyFnWithDomainFilter

	// Persistence settings
	HistoryMgrNumConns dynamicconfig.IntPropertyFn

	MaxDecisionStartToCloseTimeout dynamicconfig.IntPropertyFnWithDomainFilter

	// security protection settings
	EnableAdminProtection         dynamicconfig.BoolPropertyFn
	AdminOperationToken           dynamicconfig.StringPropertyFn
	DisableListVisibilityByFilter dynamicconfig.BoolPropertyFnWithDomainFilter

	// size limit system protection
	BlobSizeLimitError dynamicconfig.IntPropertyFnWithDomainFilter
	BlobSizeLimitWarn  dynamicconfig.IntPropertyFnWithDomainFilter
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, enableVisibilityToKafka bool) *Config {
	return &Config{
		PersistenceMaxQPS:               dc.GetIntProperty(dynamicconfig.FrontendPersistenceMaxQPS, 2000),
		VisibilityMaxPageSize:           dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendVisibilityMaxPageSize, 1000),
		EnableVisibilitySampling:        dc.GetBoolProperty(dynamicconfig.EnableVisibilitySampling, true),
		EnableReadFromClosedExecutionV2: dc.GetBoolProperty(dynamicconfig.EnableReadFromClosedExecutionV2, false),
		VisibilityListMaxQPS:            dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendVisibilityListMaxQPS, 1),
		EnableVisibilityToKafka:         dc.GetBoolProperty(dynamicconfig.EnableVisibilityToKafka, enableVisibilityToKafka),
		EnableReadVisibilityFromES:      dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableReadVisibilityFromES, false),
		ESVisibilityListMaxQPS:          dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendESVisibilityListMaxQPS, 3),
		HistoryMaxPageSize:              dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendHistoryMaxPageSize, common.GetHistoryMaxPageSize),
		RPS:                             dc.GetIntProperty(dynamicconfig.FrontendRPS, 1200),
		MaxIDLengthLimit:                dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		EnableReadHistoryFromArchival:   dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableReadHistoryFromArchival, false),
		HistoryMgrNumConns:              dc.GetIntProperty(dynamicconfig.FrontendHistoryMgrNumConns, 10),
		MaxDecisionStartToCloseTimeout:  dc.GetIntPropertyFilteredByDomain(dynamicconfig.MaxDecisionStartToCloseTimeout, 600),
		EnableAdminProtection:           dc.GetBoolProperty(dynamicconfig.EnableAdminProtection, false),
		AdminOperationToken:             dc.GetStringProperty(dynamicconfig.AdminOperationToken, "CadenceTeamONLY"),
		DisableListVisibilityByFilter:   dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.DisableListVisibilityByFilter, false),
		BlobSizeLimitError:              dc.GetIntPropertyFilteredByDomain(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:               dc.GetIntPropertyFilteredByDomain(dynamicconfig.BlobSizeLimitWarn, 256*1204),
	}
}

// Service represents the cadence-frontend service
type Service struct {
	stopC  chan struct{}
	config *Config
	params *service.BootstrapParams
}

// NewService builds a new cadence-frontend service
func NewService(params *service.BootstrapParams) common.Daemon {
	params.UpdateLoggerWithServiceName(common.FrontendServiceName)
	return &Service{
		params: params,
		config: NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger), params.ESConfig.Enable),
		stopC:  make(chan struct{}),
	}
}

// Start starts the service
func (s *Service) Start() {

	var params = s.params
	var log = params.Logger

	log.Infof("%v starting", common.FrontendServiceName)

	base := service.New(params)

	pConfig := params.PersistenceConfig
	pConfig.HistoryMaxConns = s.config.HistoryMgrNumConns()
	pConfig.SetMaxQPS(pConfig.DefaultStore, s.config.PersistenceMaxQPS())
	pConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityListMaxQPS:            s.config.VisibilityListMaxQPS,
		EnableSampling:                  s.config.EnableVisibilitySampling,
		EnableReadFromClosedExecutionV2: s.config.EnableReadFromClosedExecutionV2,
	}
	pFactory := persistencefactory.New(&pConfig, params.ClusterMetadata.GetCurrentClusterName(), base.GetMetricsClient(), log)

	metadata, err := pFactory.NewMetadataManager(persistencefactory.MetadataV1V2)
	if err != nil {
		log.Fatalf("failed to create metadata manager: %v", err)
	}

	visibilityFromDB, err := pFactory.NewVisibilityManager()
	if err != nil {
		log.Fatalf("failed to create visibility manager: %v", err)
	}
	var visibilityFromES persistence.VisibilityManager
	if s.config.EnableVisibilityToKafka() {
		visibilityIndexName := params.ESConfig.Indices[common.VisibilityAppName]
		visibilityFromES = elasticsearch.NewElasticSearchVisibilityManager(params.ESClient, visibilityIndexName, log)
		// wrap with rate limiter
		esRateLimiter := common.NewTokenBucket(s.config.PersistenceMaxQPS(), common.NewRealTimeSource())
		visibilityFromES = persistence.NewVisibilityPersistenceRateLimitedClient(visibilityFromES, esRateLimiter, log)
		// wrap with advanced rate limit for list
		visibilityConfigForES := &config.VisibilityConfig{
			VisibilityListMaxQPS: s.config.ESVisibilityListMaxQPS,
		}
		visibilityFromES = persistence.NewVisibilitySamplingClient(visibilityFromES, visibilityConfigForES, base.GetMetricsClient(), log)
		// wrap with metrics
		visibilityFromES = elasticsearch.NewVisibilityMetricsClient(visibilityFromES, base.GetMetricsClient(), log)
	}
	visibility := persistence.NewVisibilityManagerWrapper(visibilityFromDB, visibilityFromES, s.config.EnableReadVisibilityFromES)

	history, err := pFactory.NewHistoryManager()
	if err != nil {
		log.Fatalf("Creating Cassandra history manager persistence failed: %v", err)
	}
	historyV2, err := pFactory.NewHistoryV2Manager()
	if err != nil {
		log.Warnf("Creating Cassandra historyV2 manager persistence failed: %v", err)
	}

	// TODO when global domain is enabled, uncomment the line below and remove the line after
	var kafkaProducer messaging.Producer
	if base.GetClusterMetadata().IsGlobalDomainEnabled() {
		kafkaProducer, err = base.GetMessagingClient().NewProducerWithClusterName(base.GetClusterMetadata().GetCurrentClusterName())
		if err != nil {
			log.Fatalf("Creating kafka producer failed: %v", err)
		}
	} else {
		kafkaProducer = &mocks.KafkaProducer{}
	}

	wfHandler := NewWorkflowHandler(base, s.config, metadata, history, historyV2, visibility, kafkaProducer, params.BlobstoreClient)
	wfHandler.Start()
	switch params.DCRedirectionPolicy.Policy {
	case DCRedirectionPolicyDefault:
		base.GetDispatcher().Register(workflowserviceserver.New(wfHandler))
	case DCRedirectionPolicyNoop:
		base.GetDispatcher().Register(workflowserviceserver.New(wfHandler))
	case DCRedirectionPolicyForwarding:
		dcRedirectionPolicy := RedirectionPolicyGenerator(
			base.GetClusterMetadata(),
			wfHandler.domainCache,
			params.DCRedirectionPolicy,
		)
		currentClusteName := base.GetClusterMetadata().GetCurrentClusterName()
		dcRediectionHandle := NewDCRedirectionHandler(
			currentClusteName, dcRedirectionPolicy, base, wfHandler,
		)
		base.GetDispatcher().Register(workflowserviceserver.New(dcRediectionHandle))
	default:
		panic(fmt.Sprintf("Unknown DC redirection policy %v", params.DCRedirectionPolicy.Policy))
	}

	adminHandler := NewAdminHandler(base, pConfig.NumHistoryShards, metadata, history, historyV2)
	adminHandler.Start()

	log.Infof("%v started", common.FrontendServiceName)

	<-s.stopC

	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Infof("%v stopped", common.FrontendServiceName)
}
