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

package domain

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	persistencetests "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/service/config"
	dc "github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	domainHandlerGlobalDomainEnabledNotMasterClusterSuite struct {
		suite.Suite
		persistencetests.TestBase

		minRetentionDays     int
		maxBadBinaryCount    int
		metadataMgr          persistence.MetadataManager
		mockProducer         *mocks.KafkaProducer
		mockDomainReplicator Replicator
		archivalMetadata     archiver.ArchivalMetadata
		mockArchiverProvider *provider.MockArchiverProvider

		handler *HandlerImpl
	}
)

func TestDomainHandlerGlobalDomainEnabledNotMasterClusterSuite(t *testing.T) {
	s := new(domainHandlerGlobalDomainEnabledNotMasterClusterSuite)
	suite.Run(t, s)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{
		ClusterMetadata: cluster.GetTestClusterMetadata(true, false),
	})
	s.TestBase.Setup()
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) SetupTest() {
	logger := loggerimpl.NewNopLogger()
	dcCollection := dc.NewCollection(dc.NewNopClient(), logger)
	s.minRetentionDays = 1
	s.maxBadBinaryCount = 10
	s.metadataMgr = s.TestBase.MetadataManager
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockDomainReplicator = NewDomainReplicator(s.mockProducer, logger)
	s.archivalMetadata = archiver.NewArchivalMetadata(
		dcCollection,
		"",
		false,
		"",
		false,
		&config.ArchivalDomainDefaults{},
	)
	s.mockArchiverProvider = &provider.MockArchiverProvider{}
	s.handler = NewHandler(
		s.minRetentionDays,
		dc.GetIntPropertyFilteredByDomain(s.maxBadBinaryCount),
		logger,
		s.metadataMgr,
		s.ClusterMetadata,
		s.mockDomainReplicator,
		s.archivalMetadata,
		s.mockArchiverProvider,
	)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TearDownTest() {
	s.mockProducer.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestRegisterGetDomain_LocalDomain_AllDefault() {
	domainName := s.getRandomDomainName()
	isGlobalDomain := false
	var clusters []*shared.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(replicationConfig.ClusterName),
		})
	}

	retention := int32(1)
	err := s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
	})
	s.Nil(err)

	resp, err := s.handler.DescribeDomain(context.Background(), &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)

	s.NotEmpty(resp.DomainInfo.GetUUID())
	resp.DomainInfo.UUID = common.StringPtr("")
	s.Equal(&shared.DomainInfo{
		Name:        common.StringPtr(domainName),
		Status:      shared.DomainStatusRegistered.Ptr(),
		Description: common.StringPtr(""),
		OwnerEmail:  common.StringPtr(""),
		Data:        map[string]string{},
		UUID:        common.StringPtr(""),
	}, resp.DomainInfo)
	s.Equal(&shared.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(true),
		HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
		HistoryArchivalURI:                     common.StringPtr(""),
		VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
		VisibilityArchivalURI:                  common.StringPtr(""),
		BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
	}, resp.Configuration)
	s.Equal(&shared.DomainReplicationConfiguration{
		ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
		Clusters:          clusters,
	}, resp.ReplicationConfiguration)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.Equal(isGlobalDomain, resp.GetIsGlobalDomain())
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestRegisterGetDomain_LocalDomain_NoDefault() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*shared.ClusterReplicationConfiguration{
		&shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(activeClusterName),
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := false

	var expectedClusters []*shared.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		expectedClusters = append(expectedClusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(replicationConfig.ClusterName),
		})
	}

	err := s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(email),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(activeClusterName),
		Data:                                   data,
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain),
	})
	s.Nil(err)

	resp, err := s.handler.DescribeDomain(context.Background(), &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)

	s.NotEmpty(resp.DomainInfo.GetUUID())
	resp.DomainInfo.UUID = common.StringPtr("")
	s.Equal(&shared.DomainInfo{
		Name:        common.StringPtr(domainName),
		Status:      shared.DomainStatusRegistered.Ptr(),
		Description: common.StringPtr(description),
		OwnerEmail:  common.StringPtr(email),
		Data:        data,
		UUID:        common.StringPtr(""),
	}, resp.DomainInfo)
	s.Equal(&shared.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(emitMetric),
		HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
		HistoryArchivalURI:                     common.StringPtr(""),
		VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
		VisibilityArchivalURI:                  common.StringPtr(""),
		BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
	}, resp.Configuration)
	s.Equal(&shared.DomainReplicationConfiguration{
		ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
		Clusters:          expectedClusters,
	}, resp.ReplicationConfiguration)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.Equal(isGlobalDomain, resp.GetIsGlobalDomain())
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestUpdateGetDomain_LocalDomain_NoAttrSet() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	data := map[string]string{"some random key": "some random value"}
	var clusters []*shared.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(replicationConfig.ClusterName),
		})
	}
	isGlobalDomain := false

	err := s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(email),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
		Data:                                   data,
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain),
	})
	s.Nil(err)

	fnTest := func(info *shared.DomainInfo, config *shared.DomainConfiguration,
		replicationConfig *shared.DomainReplicationConfiguration, isGlobalDomain bool, failoverVersion int64) {
		s.NotEmpty(info.GetUUID())
		info.UUID = common.StringPtr("")
		s.Equal(&shared.DomainInfo{
			Name:        common.StringPtr(domainName),
			Status:      shared.DomainStatusRegistered.Ptr(),
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(email),
			Data:        data,
			UUID:        common.StringPtr(""),
		}, info)
		s.Equal(&shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
			HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
			HistoryArchivalURI:                     common.StringPtr(""),
			VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
			VisibilityArchivalURI:                  common.StringPtr(""),
			BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
		}, config)
		s.Equal(&shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.Equal(isGlobalDomain, isGlobalDomain)
	}

	updateResp, err := s.handler.UpdateDomain(context.Background(), &shared.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	fnTest(
		updateResp.DomainInfo,
		updateResp.Configuration,
		updateResp.ReplicationConfiguration,
		updateResp.GetIsGlobalDomain(),
		updateResp.GetFailoverVersion(),
	)

	getResp, err := s.handler.DescribeDomain(context.Background(), &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	fnTest(
		getResp.DomainInfo,
		getResp.Configuration,
		getResp.ReplicationConfiguration,
		getResp.GetIsGlobalDomain(),
		getResp.GetFailoverVersion(),
	)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestUpdateGetDomain_LocalDomain_AllAttrSet() {
	domainName := s.getRandomDomainName()
	isGlobalDomain := false
	err := s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(1),
	})
	s.Nil(err)

	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	data := map[string]string{"some random key": "some random value"}
	var clusters []*shared.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(replicationConfig.ClusterName),
		})
	}

	fnTest := func(info *shared.DomainInfo, config *shared.DomainConfiguration,
		replicationConfig *shared.DomainReplicationConfiguration, isGlobalDomain bool, failoverVersion int64) {
		s.NotEmpty(info.GetUUID())
		info.UUID = common.StringPtr("")
		s.Equal(&shared.DomainInfo{
			Name:        common.StringPtr(domainName),
			Status:      shared.DomainStatusRegistered.Ptr(),
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(email),
			Data:        data,
			UUID:        common.StringPtr(""),
		}, info)
		s.Equal(&shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
			HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
			HistoryArchivalURI:                     common.StringPtr(""),
			VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
			VisibilityArchivalURI:                  common.StringPtr(""),
			BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
		}, config)
		s.Equal(&shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.Equal(isGlobalDomain, isGlobalDomain)
	}

	updateResp, err := s.handler.UpdateDomain(context.Background(), &shared.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		UpdatedInfo: &shared.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(email),
			Data:        data,
		},
		Configuration: &shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
			HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
			HistoryArchivalURI:                     common.StringPtr(""),
			VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
			VisibilityArchivalURI:                  common.StringPtr(""),
			BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
		},
		ReplicationConfiguration: &shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
			Clusters:          clusters,
		},
	})
	s.Nil(err)
	fnTest(
		updateResp.DomainInfo,
		updateResp.Configuration,
		updateResp.ReplicationConfiguration,
		updateResp.GetIsGlobalDomain(),
		updateResp.GetFailoverVersion(),
	)

	getResp, err := s.handler.DescribeDomain(context.Background(), &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	fnTest(
		getResp.DomainInfo,
		getResp.Configuration,
		getResp.ReplicationConfiguration,
		getResp.GetIsGlobalDomain(),
		getResp.GetFailoverVersion(),
	)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestRegisterGetDomain_GlobalDomain_AllDefault() {
	domainName := s.getRandomDomainName()
	isGlobalDomain := true
	var clusters []*shared.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(replicationConfig.ClusterName),
		})
	}
	s.Equal(1, len(clusters))

	err := s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:           common.StringPtr(domainName),
		IsGlobalDomain: common.BoolPtr(isGlobalDomain),
	})
	s.IsType(&shared.BadRequestError{}, err)

	resp, err := s.handler.DescribeDomain(context.Background(), &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.IsType(&shared.EntityNotExistsError{}, err)
	s.Nil(resp)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestRegisterGetDomain_GlobalDomain_NoDefault() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := ""
	clusters := []*shared.ClusterReplicationConfiguration{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
	}
	s.True(len(activeClusterName) > 0)
	s.True(len(clusters) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := true

	err := s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(email),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(activeClusterName),
		Data:                                   data,
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain),
	})
	s.IsType(&shared.BadRequestError{}, err)

	resp, err := s.handler.DescribeDomain(context.Background(), &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.IsType(&shared.EntityNotExistsError{}, err)
	s.Nil(resp)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestUpdateGetDomain_GlobalDomain_NoAttrSet() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := ""
	clusters := []*persistence.ClusterReplicationConfig{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
		clusters = append(clusters, &persistence.ClusterReplicationConfig{
			ClusterName: clusterName,
		})
	}
	s.True(len(activeClusterName) > 0)
	s.True(len(clusters) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := true

	_, err := s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &persistence.DomainConfig{
			Retention:                retention,
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    shared.ArchivalStatusDisabled,
			HistoryArchivalURI:       "",
			VisibilityArchivalStatus: shared.ArchivalStatusDisabled,
			VisibilityArchivalURI:    "",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters:          clusters,
		},
		IsGlobalDomain:  isGlobalDomain,
		ConfigVersion:   0,
		FailoverVersion: s.ClusterMetadata.GetNextFailoverVersion(activeClusterName, 0),
	})
	s.Nil(err)

	resp, err := s.handler.UpdateDomain(context.Background(), &shared.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.IsType(&shared.BadRequestError{}, err)
	s.Nil(resp)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestUpdateGetDomain_GlobalDomain_AllAttrSet() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := ""
	clusters := []*shared.ClusterReplicationConfiguration{}
	clustersDB := []*persistence.ClusterReplicationConfig{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
		clustersDB = append(clustersDB, &persistence.ClusterReplicationConfig{
			ClusterName: clusterName,
		})
	}
	s.True(len(activeClusterName) > 0)
	s.True(len(clusters) > 1)
	s.True(len(clustersDB) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := true

	_, err := s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "",
			OwnerEmail:  "",
			Data:        map[string]string{},
		},
		Config: &persistence.DomainConfig{
			Retention:                0,
			EmitMetric:               false,
			HistoryArchivalStatus:    shared.ArchivalStatusDisabled,
			HistoryArchivalURI:       "",
			VisibilityArchivalStatus: shared.ArchivalStatusDisabled,
			VisibilityArchivalURI:    "",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters:          clustersDB,
		},
		IsGlobalDomain:  isGlobalDomain,
		ConfigVersion:   0,
		FailoverVersion: s.ClusterMetadata.GetNextFailoverVersion(activeClusterName, 0),
	})
	s.Nil(err)

	updateResp, err := s.handler.UpdateDomain(context.Background(), &shared.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		UpdatedInfo: &shared.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(email),
			Data:        data,
		},
		Configuration: &shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
			HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
			HistoryArchivalURI:                     common.StringPtr(""),
			VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
			VisibilityArchivalURI:                  common.StringPtr(""),
			BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
		},
		ReplicationConfiguration: &shared.DomainReplicationConfiguration{
			ActiveClusterName: nil,
			Clusters:          clusters,
		},
	})
	s.IsType(&shared.BadRequestError{}, err)
	s.Nil(updateResp)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) TestUpdateGetDomain_GlobalDomain_Failover() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	prevActiveClusterName := ""
	nextActiveClusterName := s.ClusterMetadata.GetCurrentClusterName()
	clusters := []*shared.ClusterReplicationConfiguration{}
	clustersDB := []*persistence.ClusterReplicationConfig{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			prevActiveClusterName = clusterName
		}
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
		clustersDB = append(clustersDB, &persistence.ClusterReplicationConfig{
			ClusterName: clusterName,
		})
	}
	s.True(len(prevActiveClusterName) > 0)
	s.True(len(clusters) > 1)
	s.True(len(clustersDB) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := true

	_, err := s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &persistence.DomainConfig{
			Retention:                retention,
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    shared.ArchivalStatusDisabled,
			HistoryArchivalURI:       "",
			VisibilityArchivalStatus: shared.ArchivalStatusDisabled,
			VisibilityArchivalURI:    "",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: prevActiveClusterName,
			Clusters:          clustersDB,
		},
		IsGlobalDomain:  isGlobalDomain,
		ConfigVersion:   0,
		FailoverVersion: s.ClusterMetadata.GetNextFailoverVersion(prevActiveClusterName, 0),
	})
	s.Nil(err)

	fnTest := func(info *shared.DomainInfo, config *shared.DomainConfiguration,
		replicationConfig *shared.DomainReplicationConfiguration, isGlobalDomain bool, failoverVersion int64) {
		s.NotEmpty(info.GetUUID())
		info.UUID = common.StringPtr("")
		s.Equal(&shared.DomainInfo{
			Name:        common.StringPtr(domainName),
			Status:      shared.DomainStatusRegistered.Ptr(),
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(email),
			Data:        data,
			UUID:        common.StringPtr(""),
		}, info)
		s.Equal(&shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
			EmitMetric:                             common.BoolPtr(emitMetric),
			HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
			HistoryArchivalURI:                     common.StringPtr(""),
			VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
			VisibilityArchivalURI:                  common.StringPtr(""),
			BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
		}, config)
		s.Equal(&shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(nextActiveClusterName),
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(s.ClusterMetadata.GetNextFailoverVersion(
			nextActiveClusterName,
			s.ClusterMetadata.GetNextFailoverVersion(prevActiveClusterName, 0),
		), failoverVersion)
		s.Equal(isGlobalDomain, isGlobalDomain)
	}

	s.mockProducer.On("Publish", mock.Anything).Return(nil).Once()

	updateResp, err := s.handler.UpdateDomain(context.Background(), &shared.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		ReplicationConfiguration: &shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(s.ClusterMetadata.GetCurrentClusterName()),
		},
	})
	s.Nil(err)
	fnTest(
		updateResp.DomainInfo,
		updateResp.Configuration,
		updateResp.ReplicationConfiguration,
		updateResp.GetIsGlobalDomain(),
		updateResp.GetFailoverVersion(),
	)

	getResp, err := s.handler.DescribeDomain(context.Background(), &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.Nil(err)
	fnTest(
		getResp.DomainInfo,
		getResp.Configuration,
		getResp.ReplicationConfiguration,
		getResp.GetIsGlobalDomain(),
		getResp.GetFailoverVersion(),
	)
}

func (s *domainHandlerGlobalDomainEnabledNotMasterClusterSuite) getRandomDomainName() string {
	return "domain" + uuid.New()
}
