// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package namespace

import (
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/primitives"
)

type (
	transmissionTaskSuite struct {
		suite.Suite
		namespaceReplicator *namespaceReplicatorImpl
		kafkaProducer       *mocks.KafkaProducer
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
	s.namespaceReplicator = NewNamespaceReplicator(
		s.kafkaProducer,
		loggerimpl.NewDevelopmentForTest(s.Suite),
	).(*namespaceReplicatorImpl)
}

func (s *transmissionTaskSuite) TearDownTest() {
	s.kafkaProducer.AssertExpectations(s.T())
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_RegisterNamespaceTask_IsGlobalNamespace() {
	taskType := enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK
	id := primitives.NewUUID().String()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []string{clusterActive, clusterStandby}

	namespaceOperation := enumsspb.NAMESPACE_OPERATION_CREATE
	info := &persistenceblobs.NamespaceInfo{
		Id:          id,
		Name:        name,
		State:       enumspb.NAMESPACE_STATE_REGISTERED,
		Description: description,
		Owner:       ownerEmail,
		Data:        data,
	}
	config := &persistenceblobs.NamespaceConfig{
		RetentionDays:           retention,
		EmitMetric:              emitMetric,
		HistoryArchivalState:    historyArchivalState,
		HistoryArchivalUri:      historyArchivalURI,
		VisibilityArchivalState: visibilityArchivalState,
		VisibilityArchivalUri:   visibilityArchivalURI,
		BadBinaries:             &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}
	replicationConfig := &persistenceblobs.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := true

	s.kafkaProducer.On("Publish", &replicationspb.ReplicationTask{
		TaskType: taskType,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
			NamespaceTaskAttributes: &replicationspb.NamespaceTaskAttributes{
				NamespaceOperation: namespaceOperation,
				Id:                 id,
				Info: &namespacepb.NamespaceInfo{
					Name:        name,
					State:       state,
					Description: description,
					OwnerEmail:  ownerEmail,
					Data:        data,
				},
				Config: &namespacepb.NamespaceConfig{
					WorkflowExecutionRetentionPeriodInDays: retention,
					EmitMetric:                             &types.BoolValue{Value: emitMetric},
					HistoryArchivalState:                   historyArchivalState,
					HistoryArchivalUri:                     historyArchivalURI,
					VisibilityArchivalState:                visibilityArchivalState,
					VisibilityArchivalUri:                  visibilityArchivalURI,
					BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
				},
				ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
					ActiveClusterName: clusterActive,
					Clusters:          s.namespaceReplicator.convertClusterReplicationConfigToProto(clusters),
				},
				ConfigVersion:   configVersion,
				FailoverVersion: failoverVersion,
			},
		},
	}).Return(nil).Once()

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_RegisterNamespaceTask_NotGlobalNamespace() {
	id := primitives.NewUUID().String()
	name := "some random namespace test name"
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []string{clusterActive, clusterStandby}

	namespaceOperation := enumsspb.NAMESPACE_OPERATION_CREATE
	info := &persistenceblobs.NamespaceInfo{
		Id:          id,
		Name:        name,
		State:       enumspb.NAMESPACE_STATE_REGISTERED,
		Description: description,
		Owner:       ownerEmail,
		Data:        data,
	}
	config := &persistenceblobs.NamespaceConfig{
		RetentionDays:           retention,
		EmitMetric:              emitMetric,
		HistoryArchivalState:    historyArchivalState,
		HistoryArchivalUri:      historyArchivalURI,
		VisibilityArchivalState: visibilityArchivalState,
		VisibilityArchivalUri:   visibilityArchivalURI,
		BadBinaries:             &namespacepb.BadBinaries{},
	}
	replicationConfig := &persistenceblobs.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := false

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_UpdateNamespaceTask_IsGlobalNamespace() {
	taskType := enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK
	id := primitives.NewUUID().String()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_DEPRECATED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []string{clusterActive, clusterStandby}

	namespaceOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	info := &persistenceblobs.NamespaceInfo{
		Id:          id,
		Name:        name,
		State:       enumspb.NAMESPACE_STATE_DEPRECATED,
		Description: description,
		Owner:       ownerEmail,
		Data:        data,
	}
	config := &persistenceblobs.NamespaceConfig{
		RetentionDays:           retention,
		EmitMetric:              emitMetric,
		HistoryArchivalState:    historyArchivalState,
		HistoryArchivalUri:      historyArchivalURI,
		VisibilityArchivalState: visibilityArchivalState,
		VisibilityArchivalUri:   visibilityArchivalURI,
		BadBinaries:             &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}
	replicationConfig := &persistenceblobs.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := true

	s.kafkaProducer.On("Publish", &replicationspb.ReplicationTask{
		TaskType: taskType,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
			NamespaceTaskAttributes: &replicationspb.NamespaceTaskAttributes{
				NamespaceOperation: namespaceOperation,
				Id:                 id,
				Info: &namespacepb.NamespaceInfo{
					Name:        name,
					State:       state,
					Description: description,
					OwnerEmail:  ownerEmail,
					Data:        data,
				},
				Config: &namespacepb.NamespaceConfig{
					WorkflowExecutionRetentionPeriodInDays: retention,
					EmitMetric:                             &types.BoolValue{Value: emitMetric},
					HistoryArchivalState:                   historyArchivalState,
					HistoryArchivalUri:                     historyArchivalURI,
					VisibilityArchivalState:                visibilityArchivalState,
					VisibilityArchivalUri:                  visibilityArchivalURI,
					BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
				},
				ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
					ActiveClusterName: clusterActive,
					Clusters:          s.namespaceReplicator.convertClusterReplicationConfigToProto(clusters),
				},
				ConfigVersion:   configVersion,
				FailoverVersion: failoverVersion},
		},
	}).Return(nil).Once()

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_UpdateNamespaceTask_NotGlobalNamespace() {
	id := primitives.NewUUID().String()
	name := "some random namespace test name"
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []string{clusterActive, clusterStandby}

	namespaceOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	info := &persistenceblobs.NamespaceInfo{
		Id:          id,
		Name:        name,
		State:       enumspb.NAMESPACE_STATE_DEPRECATED,
		Description: description,
		Owner:       ownerEmail,
		Data:        data,
	}
	config := &persistenceblobs.NamespaceConfig{
		RetentionDays:           retention,
		EmitMetric:              emitMetric,
		HistoryArchivalState:    historyArchivalState,
		HistoryArchivalUri:      historyArchivalURI,
		VisibilityArchivalState: visibilityArchivalState,
		VisibilityArchivalUri:   visibilityArchivalURI,
	}
	replicationConfig := &persistenceblobs.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := false

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}
