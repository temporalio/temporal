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

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
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
		map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason0")},
		},
		map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason2")},
		}, nowInt64,
	)

	assert.True(s.T(), out.Equals(&shared.BadBinaries{
		Binaries: map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason2"), CreatedTimeNano: common.Int64Ptr(nowInt64)},
		},
	}))
}

func (s *domainHandlerCommonSuite) TestMergeBadBinaries_Adding() {
	out := s.handler.mergeBadBinaries(
		map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason0")},
		},
		map[string]*shared.BadBinaryInfo{
			"k1": {Reason: common.StringPtr("reason2")},
		}, nowInt64,
	)

	expected := &shared.BadBinaries{
		Binaries: map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason0")},
			"k1": {Reason: common.StringPtr("reason2"), CreatedTimeNano: common.Int64Ptr(nowInt64)},
		},
	}
	assert.Equal(s.T(), out.String(), expected.String())
}

func (s *domainHandlerCommonSuite) TestMergeBadBinaries_Merging() {
	out := s.handler.mergeBadBinaries(
		map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason0")},
		},
		map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason1")},
			"k1": {Reason: common.StringPtr("reason2")},
		}, nowInt64,
	)

	assert.True(s.T(), out.Equals(&shared.BadBinaries{
		Binaries: map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason1"), CreatedTimeNano: common.Int64Ptr(nowInt64)},
			"k1": {Reason: common.StringPtr("reason2"), CreatedTimeNano: common.Int64Ptr(nowInt64)},
		},
	}))
}

func (s *domainHandlerCommonSuite) TestMergeBadBinaries_Nil() {
	out := s.handler.mergeBadBinaries(
		nil,
		map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason1")},
			"k1": {Reason: common.StringPtr("reason2")},
		}, nowInt64,
	)

	assert.True(s.T(), out.Equals(&shared.BadBinaries{
		Binaries: map[string]*shared.BadBinaryInfo{
			"k0": {Reason: common.StringPtr("reason1"), CreatedTimeNano: common.Int64Ptr(nowInt64)},
			"k1": {Reason: common.StringPtr("reason2"), CreatedTimeNano: common.Int64Ptr(nowInt64)},
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
	var cluster1 []*shared.ClusterReplicationConfiguration
	for _, replicationConfig := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		cluster1 = append(cluster1, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(replicationConfig.ClusterName),
		})
	}
	err := s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName1),
		Description:                            common.StringPtr(description1),
		OwnerEmail:                             common.StringPtr(email1),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention1),
		EmitMetric:                             common.BoolPtr(emitMetric1),
		Data:                                   data1,
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain1),
	})
	s.Nil(err)

	domainName2 := s.getRandomDomainName()
	description2 := "some random description 2"
	email2 := "some random email 2"
	retention2 := int32(2)
	emitMetric2 := false
	data2 := map[string]string{"some random key 2": "some random value 2"}
	isGlobalDomain2 := true
	activeClusterName2 := ""
	var cluster2 []*shared.ClusterReplicationConfiguration
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName2 = clusterName
		}
		cluster2 = append(cluster2, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterName),
		})
	}
	s.mockProducer.On("Publish", mock.Anything).Return(nil).Once()
	err = s.handler.RegisterDomain(context.Background(), &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName2),
		Description:                            common.StringPtr(description2),
		OwnerEmail:                             common.StringPtr(email2),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention2),
		EmitMetric:                             common.BoolPtr(emitMetric2),
		Clusters:                               cluster2,
		ActiveClusterName:                      common.StringPtr(activeClusterName2),
		Data:                                   data2,
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain2),
	})
	s.Nil(err)

	domains := map[string]*shared.DescribeDomainResponse{}
	pagesize := int32(1)
	var token []byte
	for doPaging := true; doPaging; doPaging = len(token) > 0 {
		resp, err := s.handler.ListDomains(context.Background(), &shared.ListDomainsRequest{
			PageSize:      common.Int32Ptr(pagesize),
			NextPageToken: token,
		})
		s.Nil(err)
		token = resp.NextPageToken
		s.True(len(resp.Domains) <= int(pagesize))
		if len(resp.Domains) > 0 {
			s.NotEmpty(resp.Domains[0].DomainInfo.GetUUID())
			resp.Domains[0].DomainInfo.UUID = common.StringPtr("")
			domains[resp.Domains[0].DomainInfo.GetName()] = resp.Domains[0]
		}
	}
	delete(domains, common.SystemLocalDomainName)
	s.Equal(map[string]*shared.DescribeDomainResponse{
		domainName1: &shared.DescribeDomainResponse{
			DomainInfo: &shared.DomainInfo{
				Name:        common.StringPtr(domainName1),
				Status:      shared.DomainStatusRegistered.Ptr(),
				Description: common.StringPtr(description1),
				OwnerEmail:  common.StringPtr(email1),
				Data:        data1,
				UUID:        common.StringPtr(""),
			},
			Configuration: &shared.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention1),
				EmitMetric:                             common.BoolPtr(emitMetric1),
				HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
				HistoryArchivalURI:                     common.StringPtr(""),
				VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
				VisibilityArchivalURI:                  common.StringPtr(""),
				BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
			},
			ReplicationConfiguration: &shared.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(activeClusterName1),
				Clusters:          cluster1,
			},
			FailoverVersion: common.Int64Ptr(common.EmptyVersion),
			IsGlobalDomain:  common.BoolPtr(isGlobalDomain1),
		},
		domainName2: &shared.DescribeDomainResponse{
			DomainInfo: &shared.DomainInfo{
				Name:        common.StringPtr(domainName2),
				Status:      shared.DomainStatusRegistered.Ptr(),
				Description: common.StringPtr(description2),
				OwnerEmail:  common.StringPtr(email2),
				Data:        data2,
				UUID:        common.StringPtr(""),
			},
			Configuration: &shared.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention2),
				EmitMetric:                             common.BoolPtr(emitMetric2),
				HistoryArchivalStatus:                  shared.ArchivalStatusDisabled.Ptr(),
				HistoryArchivalURI:                     common.StringPtr(""),
				VisibilityArchivalStatus:               shared.ArchivalStatusDisabled.Ptr(),
				VisibilityArchivalURI:                  common.StringPtr(""),
				BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
			},
			ReplicationConfiguration: &shared.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(activeClusterName2),
				Clusters:          cluster2,
			},
			FailoverVersion: common.Int64Ptr(s.ClusterMetadata.GetNextFailoverVersion(activeClusterName2, 0)),
			IsGlobalDomain:  common.BoolPtr(isGlobalDomain2),
		},
	}, domains)
}

func (s *domainHandlerCommonSuite) TestRegisterDomain_InvalidRetentionPeriod() {
	registerRequest := &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr("random domain name"),
		Description:                            common.StringPtr("random domain name"),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(0)),
		IsGlobalDomain:                         common.BoolPtr(false),
	}
	err := s.handler.RegisterDomain(context.Background(), registerRequest)
	s.Equal(errInvalidRetentionPeriod, err)
}

func (s *domainHandlerCommonSuite) TestUpdateDomain_InvalidRetentionPeriod() {
	domain := "random domain name"
	registerRequest := &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr(domain),
		Description:                            common.StringPtr(domain),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(10)),
		IsGlobalDomain:                         common.BoolPtr(false),
	}
	err := s.handler.RegisterDomain(context.Background(), registerRequest)
	s.NoError(err)

	updateRequest := &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domain),
		Configuration: &workflow.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(-1)),
		},
	}
	_, err = s.handler.UpdateDomain(context.Background(), updateRequest)
	s.Equal(errInvalidRetentionPeriod, err)
}

func (s *domainHandlerCommonSuite) getRandomDomainName() string {
	return "domain" + uuid.New()
}
