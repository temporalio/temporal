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

package persistence

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
)

type (
	metadataPersistenceSuite struct {
		suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestMetadataPersistenceSuite(t *testing.T) {
	s := new(metadataPersistenceSuite)
	suite.Run(t, s)
}

func (m *metadataPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	m.SetupWorkflowStore()
}

func (m *metadataPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	m.Assertions = require.New(m.T())
}

func (m *metadataPersistenceSuite) TearDownSuite() {
	m.TearDownWorkflowStore()
}

func (m *metadataPersistenceSuite) TestCreateDomain() {
	name := "create-domain-test-name"
	status := DomainStatusRegistered
	description := "create-domain-test-description"
	owner := "create-domain-test-owner"
	retention := int32(10)
	emitMetric := true

	resp0, err0 := m.CreateDomain(
		&DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
		},
		&DomainConfig{
			Retention:  retention,
			EmitMetric: emitMetric,
		},
		&DomainReplicationConfig{},
		0,
	)

	m.Nil(err0)
	m.NotNil(resp0)

	id := resp0.ID
	m.True(len(id) > 0)

	// for domain which do not have replication config set, will default to
	// use current cluster as active, with current cluster as all clusters
	resp1, err1 := m.GetDomain(id, "")
	m.Nil(err1)
	m.NotNil(resp1)
	m.Equal(id, resp1.Info.ID)
	m.Equal(name, resp1.Info.Name)
	m.Equal(status, resp1.Info.Status)
	m.Equal(description, resp1.Info.Description)
	m.Equal(owner, resp1.Info.OwnerEmail)
	m.Equal(retention, resp1.Config.Retention)
	m.Equal(emitMetric, resp1.Config.EmitMetric)
	m.Equal(testCurrentClusterName, resp1.ReplicationConfig.ActiveClusterName)
	m.Equal(1, len(resp1.ReplicationConfig.Clusters))
	m.Equal(int64(0), resp1.FailoverVersion)
	m.True(resp1.ReplicationConfig.Clusters[0].ClusterName == testCurrentClusterName)
	m.Equal(int64(0), resp1.DBVersion)

	resp2, err2 := m.CreateDomain(
		&DomainInfo{
			Name:        name,
			Status:      status,
			Description: "fail",
			OwnerEmail:  "fail",
		},
		&DomainConfig{
			Retention:  100,
			EmitMetric: false,
		},
		&DomainReplicationConfig{},
		0,
	)
	m.NotNil(err2)
	m.IsType(&gen.DomainAlreadyExistsError{}, err2)
	m.Nil(resp2)
}

func (m *metadataPersistenceSuite) TestGetDomain() {
	name := "get-domain-test-name"
	status := DomainStatusRegistered
	description := "get-domain-test-description"
	owner := "get-domain-test-owner"
	retention := int32(10)
	emitMetric := true

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	failoverVersion := int64(59)
	clusters := []*ClusterReplicationConfig{
		&ClusterReplicationConfig{
			ClusterName: clusterActive,
		},
		&ClusterReplicationConfig{
			ClusterName: clusterStandby,
		},
	}

	resp0, err0 := m.GetDomain("", "does-not-exist")
	m.Nil(resp0)
	m.NotNil(err0)
	m.IsType(&gen.EntityNotExistsError{}, err0)

	resp1, err1 := m.CreateDomain(
		&DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
		},
		&DomainConfig{
			Retention:  retention,
			EmitMetric: emitMetric,
		},
		&DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		failoverVersion,
	)
	m.Nil(err1)
	m.NotNil(resp1)

	id := resp1.ID
	m.True(len(id) > 0)

	resp2, err2 := m.GetDomain(id, "")
	m.Nil(err2)
	m.NotNil(resp2)
	m.Equal(id, resp2.Info.ID)
	m.Equal(name, resp2.Info.Name)
	m.Equal(status, resp2.Info.Status)
	m.Equal(description, resp2.Info.Description)
	m.Equal(owner, resp2.Info.OwnerEmail)
	m.Equal(retention, resp2.Config.Retention)
	m.Equal(emitMetric, resp2.Config.EmitMetric)
	m.Equal(clusterActive, resp2.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp2.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp2.ReplicationConfig.Clusters[index])
	}
	m.Equal(failoverVersion, resp2.FailoverVersion)
	m.Equal(int64(0), resp2.DBVersion)

	resp3, err3 := m.GetDomain("", name)
	m.Nil(err3)
	m.NotNil(resp3)
	m.Equal(id, resp3.Info.ID)
	m.Equal(name, resp3.Info.Name)
	m.Equal(status, resp3.Info.Status)
	m.Equal(description, resp3.Info.Description)
	m.Equal(owner, resp3.Info.OwnerEmail)
	m.Equal(retention, resp3.Config.Retention)
	m.Equal(emitMetric, resp3.Config.EmitMetric)
	m.Equal(clusterActive, resp3.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp3.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp3.ReplicationConfig.Clusters[index])
	}
	m.Equal(failoverVersion, resp3.FailoverVersion)
	m.Equal(int64(0), resp3.DBVersion)

	resp4, err4 := m.GetDomain(id, name)
	m.NotNil(err4)
	m.IsType(&gen.BadRequestError{}, err4)
	m.Nil(resp4)
}

func (m *metadataPersistenceSuite) TestUpdateDomain() {
	name := "update-domain-test-name"
	status := DomainStatusRegistered
	description := "update-domain-test-description"
	owner := "update-domain-test-owner"
	retention := int32(10)
	emitMetric := true

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	failoverVersion := int64(59)
	clusters := []*ClusterReplicationConfig{
		&ClusterReplicationConfig{
			ClusterName: clusterActive,
		},
		&ClusterReplicationConfig{
			ClusterName: clusterStandby,
		},
	}

	resp1, err1 := m.CreateDomain(
		&DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
		},
		&DomainConfig{
			Retention:  retention,
			EmitMetric: emitMetric,
		},
		&DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		failoverVersion,
	)
	m.Nil(err1)

	id := resp1.ID
	m.True(len(id) > 0)

	resp2, err2 := m.GetDomain(id, "")
	m.Nil(err2)
	updatedStatus := DomainStatusDeprecated
	updatedDescription := "description-updated"
	updatedOwner := "owner-updated"
	updatedRetention := int32(20)
	updatedEmitMetric := false

	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateFailoverVersion := int64(28)
	updateClusters := []*ClusterReplicationConfig{
		&ClusterReplicationConfig{
			ClusterName: updateClusterActive,
		},
		&ClusterReplicationConfig{
			ClusterName: updateClusterStandby,
		},
	}

	err3 := m.UpdateDomain(
		&DomainInfo{
			ID:          resp2.Info.ID,
			Name:        resp2.Info.Name,
			Status:      updatedStatus,
			Description: updatedDescription,
			OwnerEmail:  updatedOwner,
		},
		&DomainConfig{
			Retention:  updatedRetention,
			EmitMetric: updatedEmitMetric,
		},
		&DomainReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		updateFailoverVersion,
		resp2.DBVersion,
	)

	m.Nil(err3)

	resp4, err4 := m.GetDomain("", name)
	m.Nil(err4)
	m.NotNil(resp4)
	m.Equal(id, resp4.Info.ID)
	m.Equal(name, resp4.Info.Name)
	m.Equal(updatedStatus, resp4.Info.Status)
	m.Equal(updatedDescription, resp4.Info.Description)
	m.Equal(updatedOwner, resp4.Info.OwnerEmail)
	m.Equal(updatedRetention, resp4.Config.Retention)
	m.Equal(updatedEmitMetric, resp4.Config.EmitMetric)
	m.Equal(updateClusterActive, resp4.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp4.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp4.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateFailoverVersion, resp4.FailoverVersion)
	m.Equal(resp2.DBVersion+1, resp4.DBVersion)

	resp5, err5 := m.GetDomain("", name)
	m.Nil(err5)
	m.NotNil(resp5)
	m.Equal(id, resp5.Info.ID)
	m.Equal(name, resp5.Info.Name)
	m.Equal(updatedStatus, resp5.Info.Status)
	m.Equal(updatedDescription, resp5.Info.Description)
	m.Equal(updatedOwner, resp5.Info.OwnerEmail)
	m.Equal(updatedRetention, resp5.Config.Retention)
	m.Equal(updatedEmitMetric, resp5.Config.EmitMetric)
	m.Equal(updateClusterActive, resp5.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp5.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp5.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateFailoverVersion, resp5.FailoverVersion)
	m.Equal(resp2.DBVersion+1, resp5.DBVersion)
}

func (m *metadataPersistenceSuite) TestDeleteDomain() {
	name := "delete-domain-test-name"
	status := DomainStatusRegistered
	description := "delete-domain-test-description"
	owner := "delete-domain-test-owner"
	retention := 10
	emitMetric := true

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	failoverVersion := int64(59)
	clusters := []*ClusterReplicationConfig{
		&ClusterReplicationConfig{
			ClusterName: clusterActive,
		},
		&ClusterReplicationConfig{
			ClusterName: clusterStandby,
		},
	}

	resp1, err1 := m.CreateDomain(
		&DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
		},
		&DomainConfig{
			Retention:  int32(retention),
			EmitMetric: emitMetric,
		},
		&DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		failoverVersion,
	)
	m.Nil(err1)
	id := resp1.ID
	m.True(len(id) > 0)

	resp2, err2 := m.GetDomain("", name)
	m.Nil(err2)
	m.NotNil(resp2)

	err3 := m.DeleteDomain("", name)
	m.Nil(err3)

	resp4, err4 := m.GetDomain("", name)
	m.NotNil(err4)
	m.IsType(&gen.EntityNotExistsError{}, err4)
	m.Nil(resp4)

	resp5, err5 := m.GetDomain(id, "")
	m.NotNil(err5)
	m.IsType(&gen.EntityNotExistsError{}, err5)
	m.Nil(resp5)

	resp6, err6 := m.CreateDomain(
		&DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
		},
		&DomainConfig{
			Retention:  int32(retention),
			EmitMetric: emitMetric,
		},
		&DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		failoverVersion,
	)
	m.Nil(err6)
	id = resp6.ID
	m.True(len(id) > 0)

	err7 := m.DeleteDomain(id, "")
	m.Nil(err7)

	resp8, err8 := m.GetDomain("", name)
	m.NotNil(err8)
	m.IsType(&gen.EntityNotExistsError{}, err8)
	m.Nil(resp8)

	resp9, err9 := m.GetDomain(id, "")
	m.NotNil(err9)
	m.IsType(&gen.EntityNotExistsError{}, err9)
	m.Nil(resp9)
}

func (m *metadataPersistenceSuite) CreateDomain(info *DomainInfo, config *DomainConfig,
	replicationConfig *DomainReplicationConfig, failoverVersion int64) (*CreateDomainResponse, error) {
	return m.MetadataManager.CreateDomain(&CreateDomainRequest{
		Name:              info.Name,
		Status:            info.Status,
		Description:       info.Description,
		OwnerEmail:        info.OwnerEmail,
		Config:            config,
		ReplicationConfig: replicationConfig,
		FailoverVersion:   failoverVersion,
	})
}

func (m *metadataPersistenceSuite) GetDomain(id, name string) (*GetDomainResponse, error) {
	return m.MetadataManager.GetDomain(&GetDomainRequest{
		ID:   id,
		Name: name,
	})
}

func (m *metadataPersistenceSuite) UpdateDomain(info *DomainInfo, config *DomainConfig, replicationConfig *DomainReplicationConfig,
	failoverVersion int64, dbVersion int64) error {
	return m.MetadataManager.UpdateDomain(&UpdateDomainRequest{
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,
		FailoverVersion:   failoverVersion,
		DBVersion:         dbVersion,
	})
}

func (m *metadataPersistenceSuite) DeleteDomain(id, name string) error {
	if len(id) > 0 {
		return m.MetadataManager.DeleteDomain(&DeleteDomainRequest{ID: id})
	}
	return m.MetadataManager.DeleteDomainByName(&DeleteDomainByNameRequest{Name: name})
}
