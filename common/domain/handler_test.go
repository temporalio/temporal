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
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
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
	domainHandlerCommonSuite struct {
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

var nowInt64 = time.Now().UnixNano()

func TestDomainHandlerCommonSuite(t *testing.T) {
	s := new(domainHandlerCommonSuite)
	suite.Run(t, s)
}

func (s *domainHandlerCommonSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{
		ClusterMetadata: cluster.GetTestClusterMetadata(true, true),
	})
	s.TestBase.Setup()
}

func (s *domainHandlerCommonSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *domainHandlerCommonSuite) SetupTest() {
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

func (s *domainHandlerCommonSuite) TearDownTest() {
	s.mockProducer.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
}

func (s *domainHandlerCommonSuite) TestMergeDomainData_Overriding() {
	out := s.handler.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v2",
	}, out)
}

func (s *domainHandlerCommonSuite) TestMergeDomainData_Adding() {
	out := s.handler.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v0",
		"k1": "v2",
	}, out)
}

func (s *domainHandlerCommonSuite) TestMergeDomainData_Merging() {
	out := s.handler.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

func (s *domainHandlerCommonSuite) TestMergeDomainData_Nil() {
	out := s.handler.mergeDomainData(
		nil,
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

// test merging bad binaries
func (s *domainHandlerCommonSuite) TestMergeBadBinaries_Overriding() {
	out := s.handler.mergeBadBinaries(
		map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason0"},
		},
		map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason2"},
		}, nowInt64,
	)

	assert.True(s.T(), out.Equal(commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason2", CreatedTimeNano: nowInt64},
		},
	}))
}

func (s *domainHandlerCommonSuite) TestMergeBadBinaries_Adding() {
	out := s.handler.mergeBadBinaries(
		map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason0"},
		},
		map[string]*commonproto.BadBinaryInfo{
			"k1": {Reason: "reason2"},
		}, nowInt64,
	)

	expected := commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason0"},
			"k1": {Reason: "reason2", CreatedTimeNano: nowInt64},
		},
	}
	assert.Equal(s.T(), out.String(), expected.String())
}

func (s *domainHandlerCommonSuite) TestMergeBadBinaries_Merging() {
	out := s.handler.mergeBadBinaries(
		map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason0"},
		},
		map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason1"},
			"k1": {Reason: "reason2"},
		}, nowInt64,
	)

	assert.True(s.T(), out.Equal(commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason1", CreatedTimeNano: nowInt64},
			"k1": {Reason: "reason2", CreatedTimeNano: nowInt64},
		},
	}))
}

func (s *domainHandlerCommonSuite) TestMergeBadBinaries_Nil() {
	out := s.handler.mergeBadBinaries(
		nil,
		map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason1"},
			"k1": {Reason: "reason2"},
		}, nowInt64,
	)

	assert.True(s.T(), out.Equal(commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"k0": {Reason: "reason1", CreatedTimeNano: nowInt64},
			"k1": {Reason: "reason2", CreatedTimeNano: nowInt64},
		},
	}))
}

func (s *domainHandlerCommonSuite) TestListDomain() {
	domainName1 := s.getRandomDomainName()
	description1 := "some random description 1"
	email1 := "some random email 1"
	retention1 := int32(1)
	emitMetric1 := true
	data1 := map[string]string{"some random key 1": "some random value 1"}
	isGlobalDomain1 := false
	activeClusterName1 := s.ClusterMetadata.GetCurrentClusterName()
	var cluster1 []*commonproto.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		cluster1 = append(cluster1, &commonproto.ClusterReplicationConfiguration{
			ClusterName: replicationConfig.ClusterName,
		})
	}
	registerResp, err := s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName1,
		Description:                            description1,
		OwnerEmail:                             email1,
		WorkflowExecutionRetentionPeriodInDays: retention1,
		EmitMetric:                             emitMetric1,
		Data:                                   data1,
		IsGlobalDomain:                         isGlobalDomain1,
	})
	s.NoError(err)
	s.Nil(registerResp)

	domainName2 := s.getRandomDomainName()
	description2 := "some random description 2"
	email2 := "some random email 2"
	retention2 := int32(2)
	emitMetric2 := false
	data2 := map[string]string{"some random key 2": "some random value 2"}
	isGlobalDomain2 := true
	activeClusterName2 := ""
	var cluster2 []*commonproto.ClusterReplicationConfiguration
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName2 = clusterName
		}
		cluster2 = append(cluster2, &commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterName,
		})
	}
	s.mockProducer.On("Publish", mock.Anything).Return(nil).Once()
	registerResp, err = s.handler.RegisterDomain(context.Background(), &workflowservice.RegisterDomainRequest{
		Name:                                   domainName2,
		Description:                            description2,
		OwnerEmail:                             email2,
		WorkflowExecutionRetentionPeriodInDays: retention2,
		EmitMetric:                             emitMetric2,
		Clusters:                               cluster2,
		ActiveClusterName:                      activeClusterName2,
		Data:                                   data2,
		IsGlobalDomain:                         isGlobalDomain2,
	})
	s.NoError(err)
	s.Nil(registerResp)

	domains := map[string]*workflowservice.DescribeDomainResponse{}
	pagesize := int32(1)
	var token []byte
	for doPaging := true; doPaging; doPaging = len(token) > 0 {
		resp, err := s.handler.ListDomains(context.Background(), &workflowservice.ListDomainsRequest{
			PageSize:      pagesize,
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		s.True(len(resp.Domains) <= int(pagesize))
		if len(resp.Domains) > 0 {
			s.NotEmpty(resp.Domains[0].DomainInfo.GetUuid())
			resp.Domains[0].DomainInfo.Uuid = ""
			domains[resp.Domains[0].DomainInfo.GetName()] = resp.Domains[0]
		}
	}
	delete(domains, common.SystemLocalDomainName)
	s.Equal(map[string]*workflowservice.DescribeDomainResponse{
		domainName1: &workflowservice.DescribeDomainResponse{
			DomainInfo: &commonproto.DomainInfo{
				Name:        domainName1,
				Status:      enums.DomainStatusRegistered,
				Description: description1,
				OwnerEmail:  email1,
				Data:        data1,
				Uuid:        "",
			},
			Configuration: &commonproto.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: retention1,
				EmitMetric:                             &types.BoolValue{Value: emitMetric1},
				HistoryArchivalStatus:                  enums.ArchivalStatusDisabled,
				HistoryArchivalURI:                     "",
				VisibilityArchivalStatus:               enums.ArchivalStatusDisabled,
				VisibilityArchivalURI:                  "",
				BadBinaries:                            &commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
			},
			ReplicationConfiguration: &commonproto.DomainReplicationConfiguration{
				ActiveClusterName: activeClusterName1,
				Clusters:          cluster1,
			},
			FailoverVersion: common.EmptyVersion,
			IsGlobalDomain:  isGlobalDomain1,
		},
		domainName2: &workflowservice.DescribeDomainResponse{
			DomainInfo: &commonproto.DomainInfo{
				Name:        domainName2,
				Status:      enums.DomainStatusRegistered,
				Description: description2,
				OwnerEmail:  email2,
				Data:        data2,
				Uuid:        "",
			},
			Configuration: &commonproto.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: retention2,
				EmitMetric:                             &types.BoolValue{Value: emitMetric2},
				HistoryArchivalStatus:                  enums.ArchivalStatusDisabled,
				HistoryArchivalURI:                     "",
				VisibilityArchivalStatus:               enums.ArchivalStatusDisabled,
				VisibilityArchivalURI:                  "",
				BadBinaries:                            &commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
			},
			ReplicationConfiguration: &commonproto.DomainReplicationConfiguration{
				ActiveClusterName: activeClusterName2,
				Clusters:          cluster2,
			},
			FailoverVersion: s.ClusterMetadata.GetNextFailoverVersion(activeClusterName2, 0),
			IsGlobalDomain:  isGlobalDomain2,
		},
	}, domains)
}

func (s *domainHandlerCommonSuite) TestRegisterDomain_InvalidRetentionPeriod() {
	registerRequest := &workflowservice.RegisterDomainRequest{
		Name:                                   "random domain name",
		Description:                            "random domain name",
		WorkflowExecutionRetentionPeriodInDays: int32(0),
		IsGlobalDomain:                         false,
	}
	resp, err := s.handler.RegisterDomain(context.Background(), registerRequest)
	s.Equal(errInvalidRetentionPeriod, err)
	s.Nil(resp)
}

func (s *domainHandlerCommonSuite) TestUpdateDomain_InvalidRetentionPeriod() {
	domain := "random domain name"
	registerRequest := &workflowservice.RegisterDomainRequest{
		Name:                                   domain,
		Description:                            domain,
		WorkflowExecutionRetentionPeriodInDays: int32(10),
		IsGlobalDomain:                         false,
	}
	registerResp, err := s.handler.RegisterDomain(context.Background(), registerRequest)
	s.NoError(err)
	s.Nil(registerResp)

	updateRequest := &workflowservice.UpdateDomainRequest{
		Name: domain,
		Configuration: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: int32(-1),
		},
	}
	resp, err := s.handler.UpdateDomain(context.Background(), updateRequest)
	s.Equal(errInvalidRetentionPeriod, err)
	s.Nil(resp)
}

func (s *domainHandlerCommonSuite) getRandomDomainName() string {
	return "domain" + uuid.New()
}
