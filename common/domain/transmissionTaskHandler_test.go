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
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
)

type (
	transmissionTaskSuite struct {
		suite.Suite
		domainReplicator *domainReplicatorImpl
		kafkaProducer    *mocks.KafkaProducer
	}
)

func TestTransmissionTaskSuite(t *testing.T) {
	s := new(transmissionTaskSuite)
	suite.Run(t, s)
}

func (s *transmissionTaskSuite) SetupSuite() {
}

func (s *transmissionTaskSuite) TearDownSuite() {

}

func (s *transmissionTaskSuite) SetupTest() {
	s.kafkaProducer = &mocks.KafkaProducer{}
	s.domainReplicator = NewDomainReplicator(
		s.kafkaProducer,
		loggerimpl.NewDevelopmentForTest(s.Suite),
	).(*domainReplicatorImpl)
}

func (s *transmissionTaskSuite) TearDownTest() {
	s.kafkaProducer.AssertExpectations(s.T())
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_RegisterDomainTask_IsGlobalDomain() {
	taskType := replicator.ReplicationTaskTypeDomain
	id := uuid.New()
	name := "some random domain test name"
	status := shared.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := shared.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := shared.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	domainOperation := replicator.DomainOperationCreate
	info := &p.DomainInfo{
		ID:          id,
		Name:        name,
		Status:      p.DomainStatusRegistered,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.DomainConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
		BadBinaries:              shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
	}
	replicationConfig := &p.DomainReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalDomain := true

	s.kafkaProducer.On("Publish", &replicator.ReplicationTask{
		TaskType: &taskType,
		DomainTaskAttributes: &replicator.DomainTaskAttributes{
			DomainOperation: &domainOperation,
			ID:              common.StringPtr(id),
			Info: &shared.DomainInfo{
				Name:        common.StringPtr(name),
				Status:      &status,
				Description: common.StringPtr(description),
				OwnerEmail:  common.StringPtr(ownerEmail),
				Data:        data,
			},
			Config: &shared.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
				EmitMetric:                             common.BoolPtr(emitMetric),
				HistoryArchivalStatus:                  common.ArchivalStatusPtr(historyArchivalStatus),
				HistoryArchivalURI:                     common.StringPtr(historyArchivalURI),
				VisibilityArchivalStatus:               common.ArchivalStatusPtr(visibilityArchivalStatus),
				VisibilityArchivalURI:                  common.StringPtr(visibilityArchivalURI),
				BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
			},
			ReplicationConfig: &shared.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(clusterActive),
				Clusters:          s.domainReplicator.convertClusterReplicationConfigToThrift(clusters),
			},
			ConfigVersion:   common.Int64Ptr(configVersion),
			FailoverVersion: common.Int64Ptr(failoverVersion),
		},
	}).Return(nil).Once()

	err := s.domainReplicator.HandleTransmissionTask(domainOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalDomain)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_RegisterDomainTask_NotGlobalDomain() {
	id := uuid.New()
	name := "some random domain test name"
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := shared.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := shared.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	domainOperation := replicator.DomainOperationCreate
	info := &p.DomainInfo{
		ID:          id,
		Name:        name,
		Status:      p.DomainStatusRegistered,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.DomainConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
		BadBinaries:              shared.BadBinaries{},
	}
	replicationConfig := &p.DomainReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalDomain := false

	err := s.domainReplicator.HandleTransmissionTask(domainOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalDomain)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_UpdateDomainTask_IsGlobalDomain() {
	taskType := replicator.ReplicationTaskTypeDomain
	id := uuid.New()
	name := "some random domain test name"
	status := shared.DomainStatusDeprecated
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := shared.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := shared.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	domainOperation := replicator.DomainOperationUpdate
	info := &p.DomainInfo{
		ID:          id,
		Name:        name,
		Status:      p.DomainStatusDeprecated,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.DomainConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
		BadBinaries:              shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
	}
	replicationConfig := &p.DomainReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalDomain := true

	s.kafkaProducer.On("Publish", &replicator.ReplicationTask{
		TaskType: &taskType,
		DomainTaskAttributes: &replicator.DomainTaskAttributes{
			DomainOperation: &domainOperation,
			ID:              common.StringPtr(id),
			Info: &shared.DomainInfo{
				Name:        common.StringPtr(name),
				Status:      &status,
				Description: common.StringPtr(description),
				OwnerEmail:  common.StringPtr(ownerEmail),
				Data:        data,
			},
			Config: &shared.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
				EmitMetric:                             common.BoolPtr(emitMetric),
				HistoryArchivalStatus:                  common.ArchivalStatusPtr(historyArchivalStatus),
				HistoryArchivalURI:                     common.StringPtr(historyArchivalURI),
				VisibilityArchivalStatus:               common.ArchivalStatusPtr(visibilityArchivalStatus),
				VisibilityArchivalURI:                  common.StringPtr(visibilityArchivalURI),
				BadBinaries:                            &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{}},
			},
			ReplicationConfig: &shared.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(clusterActive),
				Clusters:          s.domainReplicator.convertClusterReplicationConfigToThrift(clusters),
			},
			ConfigVersion:   common.Int64Ptr(configVersion),
			FailoverVersion: common.Int64Ptr(failoverVersion),
		},
	}).Return(nil).Once()

	err := s.domainReplicator.HandleTransmissionTask(domainOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalDomain)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_UpdateDomainTask_NotGlobalDomain() {
	id := uuid.New()
	name := "some random domain test name"
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := shared.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := shared.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	domainOperation := replicator.DomainOperationUpdate
	info := &p.DomainInfo{
		ID:          id,
		Name:        name,
		Status:      p.DomainStatusDeprecated,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.DomainConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
	}
	replicationConfig := &p.DomainReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalDomain := false

	err := s.domainReplicator.HandleTransmissionTask(domainOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalDomain)
	s.Nil(err)
}
