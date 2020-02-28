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

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	persistencetests "github.com/temporalio/temporal/common/persistence/persistence-tests"
	"github.com/temporalio/temporal/common/service/config"
	dc "github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	domainHandlerGlobalDomainDisabledSuite struct {
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

func TestDomainHandlerGlobalDomainDisabledSuite(t *testing.T) {
	s := new(domainHandlerGlobalDomainDisabledSuite)
	suite.Run(t, s)
}

func (s *domainHandlerGlobalDomainDisabledSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{
		ClusterMetadata: cluster.GetTestClusterMetadata(false, false),
	})
	s.TestBase.Setup()
}

func (s *domainHandlerGlobalDomainDisabledSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *domainHandlerGlobalDomainDisabledSuite) SetupTest() {
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

func (s *domainHandlerGlobalDomainDisabledSuite) TearDownTest() {
	s.mockProducer.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
}

func (s *domainHandlerGlobalDomainDisabledSuite) TestRegisterGetDomain_InvalidGlobalDomain() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*commonproto.ClusterReplicationConfiguration{
		{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := true

	resp, err := s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName,
		Description:                            description,
		OwnerEmail:                             email,
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             emitMetric,
		Clusters:                               clusters,
		ActiveClusterName:                      activeClusterName,
		Data:                                   data,
		IsGlobalDomain:                         isGlobalDomain,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(resp)
}

func (s *domainHandlerGlobalDomainDisabledSuite) TestRegisterGetDomain_InvalidCluster() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := cluster.TestAlternativeClusterName
	clusters := []*commonproto.ClusterReplicationConfiguration{
		{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := false

	resp, err := s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName,
		Description:                            description,
		OwnerEmail:                             email,
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             emitMetric,
		Clusters:                               clusters,
		ActiveClusterName:                      activeClusterName,
		Data:                                   data,
		IsGlobalDomain:                         isGlobalDomain,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(resp)
}

func (s *domainHandlerGlobalDomainDisabledSuite) TestRegisterGetDomain_AllDefault() {
	domainName := s.getRandomDomainName()
	var clusters []*commonproto.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &commonproto.ClusterReplicationConfiguration{
			ClusterName: replicationConfig.ClusterName,
		})
	}

	retention := int32(1)
	registerResp, err := s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName,
		WorkflowExecutionRetentionPeriodInDays: retention,
	})
	s.NoError(err)
	s.Nil(registerResp)

	resp, err := s.handler.DescribeDomain(context.Background(), &workflowservice.DescribeDomainRequest{
		Name: domainName,
	})
	s.NoError(err)

	s.NotEmpty(resp.DomainInfo.GetUuid())
	resp.DomainInfo.Uuid = ""
	s.Equal(&commonproto.DomainInfo{
		Name:        domainName,
		Status:      enums.DomainStatusRegistered,
		Description: "",
		OwnerEmail:  "",
		Data:        map[string]string{},
		Uuid:        "",
	}, resp.DomainInfo)
	s.Equal(&commonproto.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             &types.BoolValue{Value: false},
		HistoryArchivalStatus:                  enums.ArchivalStatusDisabled,
		HistoryArchivalURI:                     "",
		VisibilityArchivalStatus:               enums.ArchivalStatusDisabled,
		VisibilityArchivalURI:                  "",
		BadBinaries:                            &commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
	}, resp.Configuration)
	s.Equal(&commonproto.DomainReplicationConfiguration{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters:          clusters,
	}, resp.ReplicationConfiguration)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.False(resp.GetIsGlobalDomain())
}

func (s *domainHandlerGlobalDomainDisabledSuite) TestRegisterGetDomain_NoDefault() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalDomain := false

	var expectedClusters []*commonproto.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		expectedClusters = append(expectedClusters, &commonproto.ClusterReplicationConfiguration{
			ClusterName: replicationConfig.ClusterName,
		})
	}

	registerResp, err := s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName,
		Description:                            description,
		OwnerEmail:                             email,
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             emitMetric,
		Clusters:                               clusters,
		ActiveClusterName:                      activeClusterName,
		Data:                                   data,
		IsGlobalDomain:                         isGlobalDomain,
	})
	s.NoError(err)
	s.Nil(registerResp)

	resp, err := s.handler.DescribeDomain(context.Background(), &workflowservice.DescribeDomainRequest{
		Name: domainName,
	})
	s.NoError(err)

	s.NotEmpty(resp.DomainInfo.GetUuid())
	resp.DomainInfo.Uuid = ""
	s.Equal(&commonproto.DomainInfo{
		Name:        domainName,
		Status:      enums.DomainStatusRegistered,
		Description: description,
		OwnerEmail:  email,
		Data:        data,
		Uuid:        "",
	}, resp.DomainInfo)
	s.Equal(&commonproto.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             &types.BoolValue{Value: emitMetric},
		HistoryArchivalStatus:                  enums.ArchivalStatusDisabled,
		HistoryArchivalURI:                     "",
		VisibilityArchivalStatus:               enums.ArchivalStatusDisabled,
		VisibilityArchivalURI:                  "",
		BadBinaries:                            &commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
	}, resp.Configuration)
	s.Equal(&commonproto.DomainReplicationConfiguration{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters:          expectedClusters,
	}, resp.ReplicationConfiguration)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.Equal(isGlobalDomain, resp.GetIsGlobalDomain())
}

func (s *domainHandlerGlobalDomainDisabledSuite) TestUpdateGetDomain_NoAttrSet() {
	domainName := s.getRandomDomainName()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	data := map[string]string{"some random key": "some random value"}
	var clusters []*commonproto.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &commonproto.ClusterReplicationConfiguration{
			ClusterName: replicationConfig.ClusterName,
		})
	}

	registerResp, err := s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName,
		Description:                            description,
		OwnerEmail:                             email,
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             emitMetric,
		Clusters:                               clusters,
		ActiveClusterName:                      s.ClusterMetadata.GetCurrentClusterName(),
		Data:                                   data,
	})
	s.NoError(err)
	s.Nil(registerResp)

	fnTest := func(info *commonproto.DomainInfo, config *commonproto.DomainConfiguration,
		replicationConfig *commonproto.DomainReplicationConfiguration, isGlobalDomain bool, failoverVersion int64) {
		s.NotEmpty(info.GetUuid())
		info.Uuid = ""
		s.Equal(&commonproto.DomainInfo{
			Name:        domainName,
			Status:      enums.DomainStatusRegistered,
			Description: description,
			OwnerEmail:  email,
			Data:        data,
			Uuid:        "",
		}, info)
		s.Equal(&commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  enums.ArchivalStatusDisabled,
			HistoryArchivalURI:                     "",
			VisibilityArchivalStatus:               enums.ArchivalStatusDisabled,
			VisibilityArchivalURI:                  "",
			BadBinaries:                            &commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
		}, config)
		s.Equal(&commonproto.DomainReplicationConfiguration{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.False(isGlobalDomain)
	}

	updateResp, err := s.handler.UpdateDomain(context.Background(), &workflowservice.UpdateDomainRequest{
		Name: domainName,
	})
	s.NoError(err)
	fnTest(
		updateResp.DomainInfo,
		updateResp.Configuration,
		updateResp.ReplicationConfiguration,
		updateResp.GetIsGlobalDomain(),
		updateResp.GetFailoverVersion(),
	)

	getResp, err := s.handler.DescribeDomain(context.Background(), &workflowservice.DescribeDomainRequest{
		Name: domainName,
	})
	s.NoError(err)
	fnTest(
		getResp.DomainInfo,
		getResp.Configuration,
		getResp.ReplicationConfiguration,
		getResp.GetIsGlobalDomain(),
		getResp.GetFailoverVersion(),
	)
}

func (s *domainHandlerGlobalDomainDisabledSuite) TestUpdateGetDomain_AllAttrSet() {
	domainName := s.getRandomDomainName()
	registerResp, err := s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName,
		WorkflowExecutionRetentionPeriodInDays: 1,
	})
	s.NoError(err)
	s.Nil(registerResp)

	description := "some random description"
	email := "some random email"
	retention := int32(7)
	emitMetric := true
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}

	var expectedClusters []*commonproto.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		expectedClusters = append(expectedClusters, &commonproto.ClusterReplicationConfiguration{
			ClusterName: replicationConfig.ClusterName,
		})
	}

	fnTest := func(info *commonproto.DomainInfo, config *commonproto.DomainConfiguration,
		replicationConfig *commonproto.DomainReplicationConfiguration, isGlobalDomain bool, failoverVersion int64) {
		s.NotEmpty(info.GetUuid())
		info.Uuid = ""
		s.Equal(&commonproto.DomainInfo{
			Name:        domainName,
			Status:      enums.DomainStatusRegistered,
			Description: description,
			OwnerEmail:  email,
			Data:        data,
			Uuid:        "",
		}, info)
		s.Equal(&commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  enums.ArchivalStatusDisabled,
			HistoryArchivalURI:                     "",
			VisibilityArchivalStatus:               enums.ArchivalStatusDisabled,
			VisibilityArchivalURI:                  "",
			BadBinaries:                            &commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
		}, config)
		s.Equal(&commonproto.DomainReplicationConfiguration{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters:          expectedClusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.False(isGlobalDomain)
	}

	updateResp, err := s.handler.UpdateDomain(context.Background(), &workflowservice.UpdateDomainRequest{
		Name: domainName,
		UpdatedInfo: &commonproto.UpdateDomainInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Configuration: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  enums.ArchivalStatusDisabled,
			HistoryArchivalURI:                     "",
			VisibilityArchivalStatus:               enums.ArchivalStatusDisabled,
			VisibilityArchivalURI:                  "",
			BadBinaries:                            &commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
		},
		ReplicationConfiguration: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: activeClusterName,
			Clusters:          clusters,
		},
	})
	s.NoError(err)
	fnTest(updateResp.DomainInfo, updateResp.Configuration, updateResp.ReplicationConfiguration, updateResp.GetIsGlobalDomain(), updateResp.GetFailoverVersion())

	getResp, err := s.handler.DescribeDomain(context.Background(), &workflowservice.DescribeDomainRequest{
		Name: domainName,
	})
	s.NoError(err)
	fnTest(
		getResp.DomainInfo,
		getResp.Configuration,
		getResp.ReplicationConfiguration,
		getResp.GetIsGlobalDomain(),
		getResp.GetFailoverVersion(),
	)
}

func (s *domainHandlerGlobalDomainDisabledSuite) getRandomDomainName() string {
	return "domain" + uuid.New()
}
