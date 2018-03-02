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
	"log"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	domainReplicatorSuite struct {
		suite.Suite
		domainReplicator *domainReplicatorImpl
		kafkaProducer    *mocks.KafkaProducer
	}
)

func TestDomainReplicatorSuite(t *testing.T) {
	s := new(domainReplicatorSuite)
	suite.Run(t, s)
}

func (s *domainReplicatorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *domainReplicatorSuite) TearDownSuite() {

}

func (s *domainReplicatorSuite) SetupTest() {
	s.kafkaProducer = &mocks.KafkaProducer{}
	s.domainReplicator = NewDomainReplicator(
		s.kafkaProducer,
		bark.NewLoggerFromLogrus(logrus.New()),
	).(*domainReplicatorImpl)

}

func (s *domainReplicatorSuite) TearDownTest() {
}

func (s *domainReplicatorSuite) TestHandleTransmissionTask_RegisterDomainTask() {
	taskType := replicator.ReplicationTaskTypeDomain
	id := uuid.New()
	name := "some random domain test name"
	status := shared.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	retention := int32(10)
	emitMetric := true
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*persistence.ClusterReplicationConfig{
		&persistence.ClusterReplicationConfig{
			ClusterName: clusterActive,
		},
		&persistence.ClusterReplicationConfig{
			ClusterName: clusterStandby,
		},
	}

	domainOperation := replicator.DomainOperationCreate
	info := &persistence.DomainInfo{
		ID:          id,
		Name:        name,
		Status:      persistence.DomainStatusRegistered,
		Description: description,
		OwnerEmail:  ownerEmail,
	}
	config := &persistence.DomainConfig{
		Retention:  retention,
		EmitMetric: emitMetric,
	}
	replicationConfig := &persistence.DomainReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}

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
			},
			Config: &shared.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
				EmitMetric:                             common.BoolPtr(emitMetric),
			},
			ReplicationConfig: &shared.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(clusterActive),
				Clusters:          s.domainReplicator.convertClusterReplicationConfigToThrift(clusters),
			},
			ConfigVersion:   common.Int64Ptr(configVersion),
			FailoverVersion: common.Int64Ptr(failoverVersion),
		},
	}).Return(nil).Once()

	err := s.domainReplicator.HandleTransmissionTask(domainOperation, info, config, replicationConfig, configVersion, failoverVersion)
	s.Nil(err)
}

func (s *domainReplicatorSuite) TestHandleTransmissionTask_UpdateDomainTask() {
	taskType := replicator.ReplicationTaskTypeDomain
	id := uuid.New()
	name := "some random domain test name"
	status := shared.DomainStatusDeprecated
	description := "some random test description"
	ownerEmail := "some random test owner"
	retention := int32(10)
	emitMetric := true
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*persistence.ClusterReplicationConfig{
		&persistence.ClusterReplicationConfig{
			ClusterName: clusterActive,
		},
		&persistence.ClusterReplicationConfig{
			ClusterName: clusterStandby,
		},
	}

	domainOperation := replicator.DomainOperationUpdate
	info := &persistence.DomainInfo{
		ID:          id,
		Name:        name,
		Status:      persistence.DomainStatusDeprecated,
		Description: description,
		OwnerEmail:  ownerEmail,
	}
	config := &persistence.DomainConfig{
		Retention:  retention,
		EmitMetric: emitMetric,
	}
	replicationConfig := &persistence.DomainReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}

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
			},
			Config: &shared.DomainConfiguration{
				WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(retention),
				EmitMetric:                             common.BoolPtr(emitMetric),
			},
			ReplicationConfig: &shared.DomainReplicationConfiguration{
				ActiveClusterName: common.StringPtr(clusterActive),
				Clusters:          s.domainReplicator.convertClusterReplicationConfigToThrift(clusters),
			},
			ConfigVersion:   common.Int64Ptr(configVersion),
			FailoverVersion: common.Int64Ptr(failoverVersion),
		},
	}).Return(nil).Once()

	err := s.domainReplicator.HandleTransmissionTask(domainOperation, info, config, replicationConfig, configVersion, failoverVersion)
	s.Nil(err)
}
