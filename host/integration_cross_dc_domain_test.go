// Copyright (c) 2016 Uber Technologies, Inc.
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

package host

import (
	"flag"
	"os"
	"testing"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	"strconv"
	"strings"

	"fmt"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	integrationCrossDCSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		suite.Suite
		IntegrationBase
	}
)

func TestIntegrationCrossDCSuite(t *testing.T) {
	flag.Parse()
	if *integration {
		s := new(integrationCrossDCSuite)
		suite.Run(t, s)
	} else {
		t.Skip()
	}
}

func (s *integrationCrossDCSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	logger := log.New()
	formatter := &log.TextFormatter{}
	formatter.FullTimestamp = true
	logger.Formatter = formatter
	//logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)
}

func (s *integrationCrossDCSuite) TearDownSuite() {
}

func (s *integrationCrossDCSuite) SetupTest() {
	s.setupTest(false, false)
}

func (s *integrationCrossDCSuite) TearDownTest() {
	s.host.Stop()
	s.host = nil
	s.TearDownWorkflowStore()
}

func (s *integrationCrossDCSuite) setupTest(enableGlobalDomain bool, isMasterCluster bool) {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	options := persistence.TestBaseOptions{}
	options.ClusterHost = "127.0.0.1"
	options.DropKeySpace = true
	options.SchemaDir = ".."
	options.EnableGlobalDomain = enableGlobalDomain
	options.IsMasterCluster = isMasterCluster
	s.SetupWorkflowStoreWithOptions(options, nil)

	s.setupShards()

	// TODO: Use mock messaging client until we support kafka setup onebox to write end-to-end integration test
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)

	s.host = NewCadence(s.ClusterMetadata, s.mockMessagingClient, s.MetadataProxy, s.MetadataManagerV2, s.ShardMgr, s.HistoryMgr, s.ExecutionMgrFactory, s.TaskMgr,
		s.VisibilityMgr, testNumberOfHistoryShards, testNumberOfHistoryHosts, s.logger, 0, false)

	s.host.Start()

	s.engine = s.host.GetFrontendClient()
}

// Note: if the global domain is not enabled, active clusters and clusters
// will be ignored on the server side
func (s *integrationCrossDCSuite) TestIntegrationRegisterGetDomain_GlobalDomainDisabled_AllDefault() {
	testFn := func(isMasterCluster bool) {
		// re-initialize to enable global domain
		s.TearDownTest()
		s.setupTest(false, isMasterCluster)

		domainName := "some random domain name"
		clusters := []*workflow.ClusterReplicationConfiguration{}
		for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
			clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(replicationConfig.ClusterName),
			})
		}

		err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)

		resp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)
		s.Equal(domainName, resp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *resp.DomainInfo.Status)
		s.Empty(resp.DomainInfo.GetDescription())
		s.Empty(resp.DomainInfo.GetOwnerEmail())
		s.Equal(int32(0), resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(false, resp.Configuration.GetEmitMetric())
		s.Equal(s.ClusterMetadata.GetCurrentClusterName(), resp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(clusters, resp.ReplicationConfiguration.Clusters)
	}

	testFn(false)
	testFn(true)
}

func (s *integrationCrossDCSuite) TestIntegrationRegisterGetDomain_GlobalDomainEnabled_NotMaster_AllDefault() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, false)

	domainName := "some random domain name"
	err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.NotNil(err)
}

func (s *integrationCrossDCSuite) TestIntegrationRegisterGetDomain_GlobalDomainEnabled_IsMaster_AllDefault() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, true)

	domainName := "some random domain name"
	clusters := []*workflow.ClusterReplicationConfiguration{}
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(replicationConfig.ClusterName),
		})
	}

	err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)

	resp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	s.Equal(domainName, resp.DomainInfo.GetName())
	s.Equal(workflow.DomainStatusRegistered, *resp.DomainInfo.Status)
	s.Empty(resp.DomainInfo.GetDescription())
	s.Empty(resp.DomainInfo.GetOwnerEmail())
	s.Equal(int32(0), resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
	s.Equal(false, resp.Configuration.GetEmitMetric())
	s.Equal(s.ClusterMetadata.GetCurrentClusterName(), resp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(clusters, resp.ReplicationConfiguration.Clusters)
}

// Note: if the global domain is not enabled, active clusters and clusters
// will be ignored on the server side
func (s *integrationCrossDCSuite) TestIntegrationRegisterGetDomain_GlobalDomainDisabled_NoDefault() {
	testFn := func(isMasterCluster bool) {
		// re-initialize to enable global domain
		s.TearDownTest()
		s.setupTest(false, isMasterCluster)

		domainName := "some random domain name"
		description := "some random description"
		email := "some random email"
		retention := int32(7)
		emitMetric := true
		activeClusterName := ""
		currentClusterName := s.ClusterMetadata.GetCurrentClusterName()
		clusters := []*workflow.ClusterReplicationConfiguration{}
		for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
			clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterName),
			})
			if clusterName != currentClusterName {
				activeClusterName = clusterName
			}
		}

		err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
			Name:                                   common.StringPtr(domainName),
			Description:                            common.StringPtr(description),
			OwnerEmail:                             common.StringPtr(email),
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
			Clusters:                               clusters,
			ActiveClusterName:                      common.StringPtr(activeClusterName),
		})
		s.Nil(err)

		resp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)
		s.Equal(domainName, resp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *resp.DomainInfo.Status)
		s.Equal(description, resp.DomainInfo.GetDescription())
		s.Equal(email, resp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, resp.Configuration.GetEmitMetric())
		s.Equal(currentClusterName, resp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(1, len(resp.ReplicationConfiguration.Clusters))
		s.Equal(currentClusterName, resp.ReplicationConfiguration.Clusters[0].GetClusterName())
	}

	testFn(false)
	testFn(true)
}

func (s *integrationCrossDCSuite) TestIntegrationRegisterGetDomain_GlobalDomainEnabled_NotMaster_NoDefault() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, false)

	domainName := "some random domain name"
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := ""
	clusters := []*workflow.ClusterReplicationConfiguration{}
	for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
		clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
	}

	err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(email),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(activeClusterName),
	})
	s.NotNil(err)
}

func (s *integrationCrossDCSuite) TestIntegrationRegisterListDomains() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, true)

	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := ""
	clusters := []*workflow.ClusterReplicationConfiguration{}
	for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
		clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
	}

	total := 10
	pageSize := int32(6)
	domainNamePrefix := "some random domain name"
	for i := 0; i < total; i++ {
		err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
			Name:                                   common.StringPtr(fmt.Sprintf("%v-%v", domainNamePrefix, i)),
			Description:                            common.StringPtr(description),
			OwnerEmail:                             common.StringPtr(email),
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
			Clusters:                               clusters,
			ActiveClusterName:                      common.StringPtr(activeClusterName),
		})
		s.Nil(err)
	}

	resp1, err := s.engine.ListDomains(createContext(), &workflow.ListDomainsRequest{
		PageSize: common.Int32Ptr(pageSize),
	})
	s.Nil(err)
	s.True(len(resp1.NextPageToken) > 0)
	resp2, err := s.engine.ListDomains(createContext(), &workflow.ListDomainsRequest{
		PageSize:      common.Int32Ptr(pageSize),
		NextPageToken: resp1.NextPageToken,
	})
	s.Nil(err)

	s.Equal(0, len(resp2.NextPageToken))
	domains := append(resp1.Domains, resp2.Domains...)

	for _, resp := range domains {
		s.True(strings.HasPrefix(resp.DomainInfo.GetName(), domainNamePrefix))
		ss := strings.Split(*resp.DomainInfo.Name, "-")
		s.Equal(2, len(ss))
		id, err := strconv.Atoi(ss[1])
		s.Nil(err)
		s.True(id >= 0)
		s.True(id < total)

		s.Equal(workflow.DomainStatusRegistered, *resp.DomainInfo.Status)
		s.Equal(description, resp.DomainInfo.GetDescription())
		s.Equal(email, resp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, resp.Configuration.GetEmitMetric())
		s.Equal(activeClusterName, resp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(clusters, resp.ReplicationConfiguration.Clusters)
	}
}

func (s *integrationCrossDCSuite) TestIntegrationRegisterGetDomain_GlobalDomainEnabled_IsMaster_NoDefault() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, true)

	domainName := "some random domain name"
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := ""
	clusters := []*workflow.ClusterReplicationConfiguration{}
	for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
		clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
	}

	err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(email),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(activeClusterName),
	})
	s.Nil(err)

	resp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	s.Equal(domainName, resp.DomainInfo.GetName())
	s.Equal(workflow.DomainStatusRegistered, *resp.DomainInfo.Status)
	s.Equal(description, resp.DomainInfo.GetDescription())
	s.Equal(email, resp.DomainInfo.GetOwnerEmail())
	s.Equal(retention, resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
	s.Equal(emitMetric, resp.Configuration.GetEmitMetric())
	s.Equal(activeClusterName, resp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(clusters, resp.ReplicationConfiguration.Clusters)
}

// Note: if the global domain is not enabled, active clusters and clusters
// will be ignored on the server side
func (s *integrationCrossDCSuite) TestIntegrationUpdateGetDomain_GlobalDomainDisabled_AllSet() {
	testFn := func(isMasterCluster bool) {
		// re-initialize to enable global domain
		s.TearDownTest()
		s.setupTest(false, isMasterCluster)

		domainName := "some random domain name"
		err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)

		description := "some random description"
		email := "some random email"
		retention := int32(7)
		emitMetric := true
		currentClusterName := s.ClusterMetadata.GetCurrentClusterName()
		clusters := []*workflow.ClusterReplicationConfiguration{}
		for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
			clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterName),
			})
		}

		updateResp, err := s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
			Name: common.StringPtr(domainName),
			UpdatedInfo: &workflow.UpdateDomainInfo{
				Description: common.StringPtr(description),
				OwnerEmail:  common.StringPtr(email),
			},
			Configuration: &workflow.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
				EmitMetric:                             common.BoolPtr(emitMetric),
			},
			ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
				Clusters: clusters,
			},
		})
		s.Nil(err)
		s.Equal(domainName, updateResp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *updateResp.DomainInfo.Status)
		s.Equal(description, updateResp.DomainInfo.GetDescription())
		s.Equal(email, updateResp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, updateResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, updateResp.Configuration.GetEmitMetric())
		s.Equal(currentClusterName, updateResp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(1, len(updateResp.ReplicationConfiguration.Clusters))
		s.Equal(currentClusterName, updateResp.ReplicationConfiguration.Clusters[0].GetClusterName())

		describeResp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)
		s.Equal(domainName, describeResp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *describeResp.DomainInfo.Status)
		s.Equal(description, describeResp.DomainInfo.GetDescription())
		s.Equal(email, describeResp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, describeResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, describeResp.Configuration.GetEmitMetric())
		s.Equal(currentClusterName, describeResp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(1, len(describeResp.ReplicationConfiguration.Clusters))
		s.Equal(currentClusterName, describeResp.ReplicationConfiguration.Clusters[0].GetClusterName())
	}

	testFn(false)
	testFn(true)
}

func (s *integrationCrossDCSuite) TestIntegrationUpdateGetDomain_GlobalDomainEnabled_NotMaster_AllSet() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, false)

	domainName := "some random domain name"
	// bypass to create a domain, since this cluster is not the master
	// set all attr to default
	_, err := s.MetadataManagerV2.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "",
			OwnerEmail:  "",
		},
		Config: &persistence.DomainConfig{
			Retention:  0,
			EmitMetric: false,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: s.ClusterMetadata.GetCurrentClusterName()},
			},
		},
		FailoverVersion: 0,
	})
	s.Nil(err)

	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	clusters := []*workflow.ClusterReplicationConfiguration{}
	for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
		clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
	}

	_, err = s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		UpdatedInfo: &workflow.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(email),
		},
		Configuration: &workflow.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
		},
		ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
			Clusters: clusters,
		},
	})
	s.NotNil(err)
}

func (s *integrationCrossDCSuite) TestIntegrationUpdateGetDomain_GlobalDomainEnabled_IsMaster_AllSet() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, true)

	domainName := "some random domain name"
	err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)

	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	clusters := []*workflow.ClusterReplicationConfiguration{}
	for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
		clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
	}

	updateResp, err := s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		UpdatedInfo: &workflow.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(email),
		},
		Configuration: &workflow.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
		},
		ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
			Clusters: clusters,
		},
	})
	s.Nil(err)
	s.Equal(domainName, updateResp.DomainInfo.GetName())
	s.Equal(workflow.DomainStatusRegistered, *updateResp.DomainInfo.Status)
	s.Equal(description, updateResp.DomainInfo.GetDescription())
	s.Equal(email, updateResp.DomainInfo.GetOwnerEmail())
	s.Equal(retention, updateResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
	s.Equal(emitMetric, updateResp.Configuration.GetEmitMetric())
	s.Equal(s.ClusterMetadata.GetCurrentClusterName(), updateResp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(clusters, updateResp.ReplicationConfiguration.Clusters)

	describeResp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	s.Equal(domainName, describeResp.DomainInfo.GetName())
	s.Equal(workflow.DomainStatusRegistered, *describeResp.DomainInfo.Status)
	s.Equal(description, describeResp.DomainInfo.GetDescription())
	s.Equal(email, describeResp.DomainInfo.GetOwnerEmail())
	s.Equal(retention, describeResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
	s.Equal(emitMetric, describeResp.Configuration.GetEmitMetric())
	s.Equal(s.ClusterMetadata.GetCurrentClusterName(), describeResp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(clusters, describeResp.ReplicationConfiguration.Clusters)

	// update domain with less replicated regions is not allowed
	_, err = s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
			Clusters: []*workflow.ClusterReplicationConfiguration{
				&workflow.ClusterReplicationConfiguration{
					ClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
				},
			},
		},
	})
	s.NotNil(err)
}

// Note: if the global domain is not enabled, active clusters and clusters
// will be ignored on the server side
func (s *integrationCrossDCSuite) TestIntegrationUpdateGetDomain_GlobalDomainDisabled_NoSet() {
	testFn := func(isMasterCluster bool) {
		// re-initialize to enable global domain
		s.TearDownTest()
		s.setupTest(false, isMasterCluster)

		domainName := "some random domain name"
		description := "some random description"
		email := "some random email"
		retention := int32(7)
		emitMetric := true
		currentClusterName := s.ClusterMetadata.GetCurrentClusterName()

		err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
			Name:                                   common.StringPtr(domainName),
			Description:                            common.StringPtr(description),
			OwnerEmail:                             common.StringPtr(email),
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
		})
		s.Nil(err)

		updateResp, err := s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)
		s.Equal(domainName, updateResp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *updateResp.DomainInfo.Status)
		s.Equal(description, updateResp.DomainInfo.GetDescription())
		s.Equal(email, updateResp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, updateResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, updateResp.Configuration.GetEmitMetric())
		s.Equal(currentClusterName, updateResp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(1, len(updateResp.ReplicationConfiguration.Clusters))
		s.Equal(currentClusterName, updateResp.ReplicationConfiguration.Clusters[0].GetClusterName())

		describeResp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)
		s.Equal(domainName, describeResp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *describeResp.DomainInfo.Status)
		s.Equal(description, describeResp.DomainInfo.GetDescription())
		s.Equal(email, describeResp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, describeResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, describeResp.Configuration.GetEmitMetric())
		s.Equal(currentClusterName, describeResp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(1, len(describeResp.ReplicationConfiguration.Clusters))
		s.Equal(currentClusterName, describeResp.ReplicationConfiguration.Clusters[0].GetClusterName())
	}

	testFn(false)
	testFn(true)
}

func (s *integrationCrossDCSuite) TestIntegrationUpdateGetDomain_GlobalDomainEnabled_NotMaster_NoSet() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, false)

	domainName := "some random domain name"
	// bypass to create a domain, since this cluster is not the master
	// set all attr to default
	_, err := s.MetadataManagerV2.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "",
			OwnerEmail:  "",
		},
		Config: &persistence.DomainConfig{
			Retention:  0,
			EmitMetric: false,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: s.ClusterMetadata.GetCurrentClusterName()},
			},
		},
		FailoverVersion: 0,
	})
	s.Nil(err)

	_, err = s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.NotNil(err)
}

func (s *integrationCrossDCSuite) TestIntegrationUpdateGetDomain_GlobalDomainEnabled_IsMaster_NoSet() {
	// re-initialize to enable global domain
	s.TearDownTest()
	s.setupTest(true, true)

	domainName := "some random domain name"
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	clusters := []*workflow.ClusterReplicationConfiguration{}
	for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
		clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
	}

	err := s.engine.RegisterDomain(createContext(), &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(email),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
	})
	s.Nil(err)

	updateResp, err := s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	s.Equal(domainName, updateResp.DomainInfo.GetName())
	s.Equal(workflow.DomainStatusRegistered, *updateResp.DomainInfo.Status)
	s.Equal(description, updateResp.DomainInfo.GetDescription())
	s.Equal(email, updateResp.DomainInfo.GetOwnerEmail())
	s.Equal(retention, updateResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
	s.Equal(emitMetric, updateResp.Configuration.GetEmitMetric())
	s.Equal(s.ClusterMetadata.GetCurrentClusterName(), updateResp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(clusters, updateResp.ReplicationConfiguration.Clusters)

	describeResp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	s.Equal(domainName, describeResp.DomainInfo.GetName())
	s.Equal(workflow.DomainStatusRegistered, *describeResp.DomainInfo.Status)
	s.Equal(description, describeResp.DomainInfo.GetDescription())
	s.Equal(email, describeResp.DomainInfo.GetOwnerEmail())
	s.Equal(retention, describeResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
	s.Equal(emitMetric, describeResp.Configuration.GetEmitMetric())
	s.Equal(s.ClusterMetadata.GetCurrentClusterName(), describeResp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(clusters, describeResp.ReplicationConfiguration.Clusters)
}

func (s *integrationCrossDCSuite) TestIntegrationUpdateGetDomain_GlobalDomainEnabled_Failover() {
	testFn := func(isMasterCluster bool) {
		// re-initialize to enable global domain
		s.TearDownTest()
		s.setupTest(true, isMasterCluster)

		domainName := "some random domain name"
		description := "some random description"
		email := "some random email"
		retention := int32(7)
		emitMetric := true
		clusters := []*workflow.ClusterReplicationConfiguration{}

		activeClusterName := ""
		failoverVersion := int64(59)
		persistenceClusters := []*persistence.ClusterReplicationConfig{}
		for clusterName := range s.ClusterMetadata.GetAllClusterFailoverVersions() {
			clusters = append(clusters, &workflow.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterName),
			})

			persistenceClusters = append(persistenceClusters, &persistence.ClusterReplicationConfig{
				ClusterName: clusterName,
			})
			if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
				activeClusterName = clusterName
			}
		}

		// create a domain which is not currently active
		s.MetadataManagerV2.CreateDomain(&persistence.CreateDomainRequest{
			Info: &persistence.DomainInfo{
				ID:          uuid.New(),
				Name:        domainName,
				Status:      persistence.DomainStatusRegistered,
				Description: description,
				OwnerEmail:  email,
			},
			Config: &persistence.DomainConfig{
				Retention:  retention,
				EmitMetric: emitMetric,
			},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: activeClusterName,
				Clusters:          persistenceClusters,
			},
			FailoverVersion: failoverVersion,
		})

		// when doing the failover, the only thing can be updated is the active cluster
		updateResp, err := s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
			Name: common.StringPtr(domainName),
			UpdatedInfo: &workflow.UpdateDomainInfo{
				Description: common.StringPtr(description),
				OwnerEmail:  common.StringPtr(email),
			},
			Configuration: &workflow.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
				EmitMetric:                             common.BoolPtr(emitMetric),
			},
			ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
				Clusters:          clusters,
			},
		})
		s.Nil(updateResp)
		s.NotNil(err)

		updateResp, err = s.engine.UpdateDomain(createContext(), &workflow.UpdateDomainRequest{
			Name: common.StringPtr(domainName),
			ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
			},
		})
		s.Nil(err)
		s.Equal(domainName, updateResp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *updateResp.DomainInfo.Status)
		s.Equal(description, updateResp.DomainInfo.GetDescription())
		s.Equal(email, updateResp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, updateResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, updateResp.Configuration.GetEmitMetric())
		s.Equal(s.ClusterMetadata.GetCurrentClusterName(), updateResp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(clusters, updateResp.ReplicationConfiguration.Clusters)

		describeResp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
			Name: common.StringPtr(domainName),
		})
		s.Nil(err)
		s.Equal(domainName, describeResp.DomainInfo.GetName())
		s.Equal(workflow.DomainStatusRegistered, *describeResp.DomainInfo.Status)
		s.Equal(description, describeResp.DomainInfo.GetDescription())
		s.Equal(email, describeResp.DomainInfo.GetOwnerEmail())
		s.Equal(retention, describeResp.Configuration.GetWorkflowExecutionRetentionPeriodInDays())
		s.Equal(emitMetric, describeResp.Configuration.GetEmitMetric())
		s.Equal(s.ClusterMetadata.GetCurrentClusterName(), describeResp.ReplicationConfiguration.GetActiveClusterName())
		s.Equal(clusters, describeResp.ReplicationConfiguration.Clusters)
	}

	testFn(true)
	testFn(false)
}
