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

package persistencetests

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cluster"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	p "github.com/uber/cadence/common/persistence"
)

type (
	// MetadataPersistenceSuiteV2 is test of the V2 version of metadata persistence
	MetadataPersistenceSuiteV2 struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// SetupSuite implementation
func (m *MetadataPersistenceSuiteV2) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// SetupTest implementation
func (m *MetadataPersistenceSuiteV2) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	m.Assertions = require.New(m.T())

	// cleanup the domain created
	var token []byte
	pageSize := 10
ListLoop:
	for {
		resp, err := m.ListDomains(pageSize, token)
		m.NoError(err)
		token = resp.NextPageToken
		for _, domain := range resp.Domains {
			m.DeleteDomain(domain.Info.ID, "")
		}
		if len(token) == 0 {
			break ListLoop
		}
	}
}

// TearDownTest implementation
func (m *MetadataPersistenceSuiteV2) TearDownTest() {
}

// TearDownSuite implementation
func (m *MetadataPersistenceSuiteV2) TearDownSuite() {
	m.TearDownWorkflowStore()
}

// TestCreateDomain test
func (m *MetadataPersistenceSuiteV2) TestCreateDomain() {
	id := uuid.New()
	name := "create-domain-test-name"
	status := p.DomainStatusRegistered
	description := "create-domain-test-description"
	owner := "create-domain-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	archivalBucketName := "bucket-test-name"
	archivalStatus := gen.ArchivalStatusEnabled
	isGlobalDomain := false
	configVersion := int64(0)
	failoverVersion := int64(0)

	resp0, err0 := m.CreateDomain(
		&p.DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
			Data:        data,
		},
		&p.DomainConfig{
			Retention:      retention,
			EmitMetric:     emitMetric,
			ArchivalBucket: archivalBucketName,
			ArchivalStatus: archivalStatus,
		},
		&p.DomainReplicationConfig{},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.NoError(err0)
	m.NotNil(resp0)
	m.Equal(id, resp0.ID)

	// for domain which do not have replication config set, will default to
	// use current cluster as active, with current cluster as all clusters
	resp1, err1 := m.GetDomain(id, "")
	m.NoError(err1)
	m.NotNil(resp1)
	m.Equal(id, resp1.Info.ID)
	m.Equal(name, resp1.Info.Name)
	m.Equal(status, resp1.Info.Status)
	m.Equal(description, resp1.Info.Description)
	m.Equal(owner, resp1.Info.OwnerEmail)
	m.Equal(data, resp1.Info.Data)
	m.Equal(retention, resp1.Config.Retention)
	m.Equal(emitMetric, resp1.Config.EmitMetric)
	m.Equal(archivalBucketName, resp1.Config.ArchivalBucket)
	m.Equal(archivalStatus, resp1.Config.ArchivalStatus)
	m.Equal(cluster.TestCurrentClusterName, resp1.ReplicationConfig.ActiveClusterName)
	m.Equal(1, len(resp1.ReplicationConfig.Clusters))
	m.Equal(isGlobalDomain, resp1.IsGlobalDomain)
	m.Equal(configVersion, resp1.ConfigVersion)
	m.Equal(failoverVersion, resp1.FailoverVersion)
	m.True(resp1.ReplicationConfig.Clusters[0].ClusterName == cluster.TestCurrentClusterName)
	m.Equal(p.InitialFailoverNotificationVersion, resp1.FailoverNotificationVersion)

	resp2, err2 := m.CreateDomain(
		&p.DomainInfo{
			ID:          uuid.New(),
			Name:        name,
			Status:      status,
			Description: "fail",
			OwnerEmail:  "fail",
			Data:        map[string]string{},
		},
		&p.DomainConfig{
			Retention:      100,
			EmitMetric:     false,
			ArchivalBucket: "",
			ArchivalStatus: gen.ArchivalStatusDisabled,
		},
		&p.DomainReplicationConfig{},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.Error(err2)
	m.IsType(&gen.DomainAlreadyExistsError{}, err2)
	m.Nil(resp2)
}

// TestGetDomain test
func (m *MetadataPersistenceSuiteV2) TestGetDomain() {
	id := uuid.New()
	name := "get-domain-test-name"
	status := p.DomainStatusRegistered
	description := "get-domain-test-description"
	owner := "get-domain-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	archivalBucketName := "bucket-test-name"
	archivalStatus := gen.ArchivalStatusEnabled

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(11)
	failoverVersion := int64(59)
	isGlobalDomain := true
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	resp0, err0 := m.GetDomain("", "does-not-exist")
	m.Nil(resp0)
	m.Error(err0)
	m.IsType(&gen.EntityNotExistsError{}, err0)

	resp1, err1 := m.CreateDomain(
		&p.DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
			Data:        data,
		},
		&p.DomainConfig{
			Retention:      retention,
			EmitMetric:     emitMetric,
			ArchivalBucket: archivalBucketName,
			ArchivalStatus: archivalStatus,
		},
		&p.DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.NotNil(resp1)
	m.Equal(id, resp1.ID)

	resp2, err2 := m.GetDomain(id, "")
	m.NoError(err2)
	m.NotNil(resp2)
	m.Equal(id, resp2.Info.ID)
	m.Equal(name, resp2.Info.Name)
	m.Equal(status, resp2.Info.Status)
	m.Equal(description, resp2.Info.Description)
	m.Equal(owner, resp2.Info.OwnerEmail)
	m.Equal(data, resp2.Info.Data)
	m.Equal(retention, resp2.Config.Retention)
	m.Equal(emitMetric, resp2.Config.EmitMetric)
	m.Equal(archivalBucketName, resp2.Config.ArchivalBucket)
	m.Equal(archivalStatus, resp2.Config.ArchivalStatus)
	m.Equal(clusterActive, resp2.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp2.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp2.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalDomain, resp2.IsGlobalDomain)
	m.Equal(configVersion, resp2.ConfigVersion)
	m.Equal(failoverVersion, resp2.FailoverVersion)
	m.Equal(p.InitialFailoverNotificationVersion, resp2.FailoverNotificationVersion)

	resp3, err3 := m.GetDomain("", name)
	m.NoError(err3)
	m.NotNil(resp3)
	m.Equal(id, resp3.Info.ID)
	m.Equal(name, resp3.Info.Name)
	m.Equal(status, resp3.Info.Status)
	m.Equal(description, resp3.Info.Description)
	m.Equal(owner, resp3.Info.OwnerEmail)
	m.Equal(data, resp3.Info.Data)
	m.Equal(retention, resp3.Config.Retention)
	m.Equal(emitMetric, resp3.Config.EmitMetric)
	m.Equal(archivalBucketName, resp3.Config.ArchivalBucket)
	m.Equal(archivalStatus, resp3.Config.ArchivalStatus)
	m.Equal(clusterActive, resp3.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp3.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp3.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalDomain, resp2.IsGlobalDomain)
	m.Equal(configVersion, resp2.ConfigVersion)
	m.Equal(failoverVersion, resp3.FailoverVersion)
	m.Equal(p.InitialFailoverNotificationVersion, resp3.FailoverNotificationVersion)

	resp4, err4 := m.GetDomain(id, name)
	m.Error(err4)
	m.IsType(&gen.BadRequestError{}, err4)
	m.Nil(resp4)

	resp5, err5 := m.GetDomain("", "")
	m.Nil(resp5)
	m.IsType(&gen.BadRequestError{}, err5)
}

// TestConcurrentCreateDomain test
func (m *MetadataPersistenceSuiteV2) TestConcurrentCreateDomain() {
	id := uuid.New()

	name := "concurrent-create-domain-test-name"
	status := p.DomainStatusRegistered
	description := "concurrent-create-domain-test-description"
	owner := "create-domain-test-owner"
	retention := int32(10)
	emitMetric := true
	archivalBucketName := "bucket-test-name"
	archivalStatus := gen.ArchivalStatusEnabled

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalDomain := true
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	concurrency := 16
	successCount := int32(0)
	var wg sync.WaitGroup
	for i := 1; i <= concurrency; i++ {
		newValue := fmt.Sprintf("v-%v", i)
		wg.Add(1)
		go func(data map[string]string) {
			_, err1 := m.CreateDomain(
				&p.DomainInfo{
					ID:          id,
					Name:        name,
					Status:      status,
					Description: description,
					OwnerEmail:  owner,
					Data:        data,
				},
				&p.DomainConfig{
					Retention:      retention,
					EmitMetric:     emitMetric,
					ArchivalBucket: archivalBucketName,
					ArchivalStatus: archivalStatus,
				},
				&p.DomainReplicationConfig{
					ActiveClusterName: clusterActive,
					Clusters:          clusters,
				},
				isGlobalDomain,
				configVersion,
				failoverVersion,
			)
			if err1 == nil {
				atomic.AddInt32(&successCount, 1)
			}
			wg.Done()
		}(map[string]string{"k0": newValue})
	}
	wg.Wait()
	m.Equal(int32(1), successCount)

	resp, err3 := m.GetDomain("", name)
	m.NoError(err3)
	m.NotNil(resp)
	m.Equal(name, resp.Info.Name)
	m.Equal(status, resp.Info.Status)
	m.Equal(description, resp.Info.Description)
	m.Equal(owner, resp.Info.OwnerEmail)
	m.Equal(retention, resp.Config.Retention)
	m.Equal(emitMetric, resp.Config.EmitMetric)
	m.Equal(archivalBucketName, resp.Config.ArchivalBucket)
	m.Equal(archivalStatus, resp.Config.ArchivalStatus)
	m.Equal(clusterActive, resp.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalDomain, resp.IsGlobalDomain)
	m.Equal(configVersion, resp.ConfigVersion)
	m.Equal(failoverVersion, resp.FailoverVersion)

	//check domain data
	ss := strings.Split(resp.Info.Data["k0"], "-")
	m.Equal(2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	m.NoError(err)
	m.Equal(true, vi > 0 && vi <= concurrency)
}

// TestConcurrentUpdateDomain test
func (m *MetadataPersistenceSuiteV2) TestConcurrentUpdateDomain() {
	id := uuid.New()
	name := "concurrent-update-domain-test-name"
	status := p.DomainStatusRegistered
	description := "update-domain-test-description"
	owner := "update-domain-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	archivalBucketName := "bucket-test-name"
	archivalStatus := gen.ArchivalStatusEnabled

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalDomain := true
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	resp1, err1 := m.CreateDomain(
		&p.DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
			Data:        data,
		},
		&p.DomainConfig{
			Retention:      retention,
			EmitMetric:     emitMetric,
			ArchivalBucket: archivalBucketName,
			ArchivalStatus: archivalStatus,
		},
		&p.DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.Equal(id, resp1.ID)

	resp2, err2 := m.GetDomain(id, "")
	m.NoError(err2)
	metadata, err := m.MetadataManagerV2.GetMetadata()
	m.NoError(err)
	notificationVersion := metadata.NotificationVersion

	concurrency := 16
	successCount := int32(0)
	var wg sync.WaitGroup
	for i := 1; i <= concurrency; i++ {
		newValue := fmt.Sprintf("v-%v", i)
		wg.Add(1)
		go func(updatedData map[string]string) {
			err3 := m.UpdateDomain(
				&p.DomainInfo{
					ID:          resp2.Info.ID,
					Name:        resp2.Info.Name,
					Status:      resp2.Info.Status,
					Description: resp2.Info.Description,
					OwnerEmail:  resp2.Info.OwnerEmail,
					Data:        updatedData,
				},
				&p.DomainConfig{
					Retention:      resp2.Config.Retention,
					EmitMetric:     resp2.Config.EmitMetric,
					ArchivalBucket: resp2.Config.ArchivalBucket,
					ArchivalStatus: resp2.Config.ArchivalStatus,
				},
				&p.DomainReplicationConfig{
					ActiveClusterName: resp2.ReplicationConfig.ActiveClusterName,
					Clusters:          resp2.ReplicationConfig.Clusters,
				},
				resp2.ConfigVersion,
				resp2.FailoverVersion,
				resp2.FailoverNotificationVersion,
				notificationVersion,
			)
			if err3 == nil {
				atomic.AddInt32(&successCount, 1)
			}
			wg.Done()
		}(map[string]string{"k0": newValue})
	}
	wg.Wait()
	m.Equal(int32(1), successCount)

	resp3, err3 := m.GetDomain("", name)
	m.NoError(err3)
	m.NotNil(resp3)
	m.Equal(id, resp3.Info.ID)
	m.Equal(name, resp3.Info.Name)
	m.Equal(status, resp3.Info.Status)
	m.Equal(isGlobalDomain, resp3.IsGlobalDomain)
	m.Equal(description, resp3.Info.Description)
	m.Equal(owner, resp3.Info.OwnerEmail)

	m.Equal(retention, resp3.Config.Retention)
	m.Equal(emitMetric, resp3.Config.EmitMetric)
	m.Equal(archivalBucketName, resp3.Config.ArchivalBucket)
	m.Equal(archivalStatus, resp3.Config.ArchivalStatus)
	m.Equal(clusterActive, resp3.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp3.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp3.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalDomain, resp2.IsGlobalDomain)
	m.Equal(configVersion, resp2.ConfigVersion)
	m.Equal(failoverVersion, resp3.FailoverVersion)

	//check domain data
	ss := strings.Split(resp3.Info.Data["k0"], "-")
	m.Equal(2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	m.NoError(err)
	m.Equal(true, vi > 0 && vi <= concurrency)
}

// TestUpdateDomain test
func (m *MetadataPersistenceSuiteV2) TestUpdateDomain() {
	id := uuid.New()
	name := "update-domain-test-name"
	status := p.DomainStatusRegistered
	description := "update-domain-test-description"
	owner := "update-domain-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	archivalBucketName := "bucket-test-name"
	archivalStatus := gen.ArchivalStatusEnabled

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalDomain := true
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	resp1, err1 := m.CreateDomain(
		&p.DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
			Data:        data,
		},
		&p.DomainConfig{
			Retention:      retention,
			EmitMetric:     emitMetric,
			ArchivalBucket: archivalBucketName,
			ArchivalStatus: archivalStatus,
		},
		&p.DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.Equal(id, resp1.ID)

	resp2, err2 := m.GetDomain(id, "")
	m.NoError(err2)
	metadata, err := m.MetadataManagerV2.GetMetadata()
	m.NoError(err)
	notificationVersion := metadata.NotificationVersion

	updatedStatus := p.DomainStatusDeprecated
	updatedDescription := "description-updated"
	updatedOwner := "owner-updated"
	//This will overriding the previous key-value pair
	updatedData := map[string]string{"k1": "v2"}
	updatedRetention := int32(20)
	updatedEmitMetric := false
	updatedArchivalStatus := gen.ArchivalStatusDisabled

	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := int64(12)
	updateFailoverVersion := int64(28)
	updateFailoverNotificationVersion := int64(14)
	updateClusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}

	err3 := m.UpdateDomain(
		&p.DomainInfo{
			ID:          resp2.Info.ID,
			Name:        resp2.Info.Name,
			Status:      updatedStatus,
			Description: updatedDescription,
			OwnerEmail:  updatedOwner,
			Data:        updatedData,
		},
		&p.DomainConfig{
			Retention:      updatedRetention,
			EmitMetric:     updatedEmitMetric,
			ArchivalBucket: archivalBucketName,
			ArchivalStatus: updatedArchivalStatus,
		},
		&p.DomainReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		updateConfigVersion,
		updateFailoverVersion,
		updateFailoverNotificationVersion,
		notificationVersion,
	)
	m.NoError(err3)

	resp4, err4 := m.GetDomain("", name)
	m.NoError(err4)
	m.NotNil(resp4)
	m.Equal(id, resp4.Info.ID)
	m.Equal(name, resp4.Info.Name)
	m.Equal(isGlobalDomain, resp4.IsGlobalDomain)
	m.Equal(updatedStatus, resp4.Info.Status)
	m.Equal(updatedDescription, resp4.Info.Description)
	m.Equal(updatedOwner, resp4.Info.OwnerEmail)
	m.Equal(updatedData, resp4.Info.Data)
	m.Equal(updatedRetention, resp4.Config.Retention)
	m.Equal(updatedEmitMetric, resp4.Config.EmitMetric)
	m.Equal(archivalBucketName, resp4.Config.ArchivalBucket)
	m.Equal(updatedArchivalStatus, resp4.Config.ArchivalStatus)
	m.Equal(updateClusterActive, resp4.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp4.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp4.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateConfigVersion, resp4.ConfigVersion)
	m.Equal(updateFailoverVersion, resp4.FailoverVersion)
	m.Equal(updateFailoverNotificationVersion, resp4.FailoverNotificationVersion)
	m.Equal(notificationVersion, resp4.NotificationVersion)

	resp5, err5 := m.GetDomain("", name)
	m.NoError(err5)
	m.NotNil(resp5)
	m.Equal(id, resp5.Info.ID)
	m.Equal(name, resp5.Info.Name)
	m.Equal(isGlobalDomain, resp5.IsGlobalDomain)
	m.Equal(updatedStatus, resp5.Info.Status)
	m.Equal(updatedDescription, resp5.Info.Description)
	m.Equal(updatedOwner, resp5.Info.OwnerEmail)
	m.Equal(updatedData, resp5.Info.Data)
	m.Equal(updatedRetention, resp5.Config.Retention)
	m.Equal(updatedEmitMetric, resp5.Config.EmitMetric)
	m.Equal(archivalBucketName, resp5.Config.ArchivalBucket)
	m.Equal(updatedArchivalStatus, resp5.Config.ArchivalStatus)
	m.Equal(updateClusterActive, resp5.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp5.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp5.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateConfigVersion, resp5.ConfigVersion)
	m.Equal(updateFailoverVersion, resp5.FailoverVersion)
	m.Equal(updateFailoverNotificationVersion, resp5.FailoverNotificationVersion)
	m.Equal(notificationVersion, resp5.NotificationVersion)
}

// TestDeleteDomain test
func (m *MetadataPersistenceSuiteV2) TestDeleteDomain() {
	id := uuid.New()
	name := "delete-domain-test-name"
	status := p.DomainStatusRegistered
	description := "delete-domain-test-description"
	owner := "delete-domain-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := 10
	emitMetric := true
	archivalBucketName := "bucket-test-name"
	archivalStatus := gen.ArchivalStatusEnabled

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalDomain := true
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	resp1, err1 := m.CreateDomain(
		&p.DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
			Data:        data,
		},
		&p.DomainConfig{
			Retention:      int32(retention),
			EmitMetric:     emitMetric,
			ArchivalBucket: archivalBucketName,
			ArchivalStatus: archivalStatus,
		},
		&p.DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.Equal(id, resp1.ID)

	resp2, err2 := m.GetDomain("", name)
	m.NoError(err2)
	m.NotNil(resp2)

	err3 := m.DeleteDomain("", name)
	m.NoError(err3)

	resp4, err4 := m.GetDomain("", name)
	m.Error(err4)
	m.IsType(&gen.EntityNotExistsError{}, err4)
	m.Nil(resp4)

	resp5, err5 := m.GetDomain(id, "")
	m.Error(err5)
	m.IsType(&gen.EntityNotExistsError{}, err5)
	m.Nil(resp5)

	id = uuid.New()
	resp6, err6 := m.CreateDomain(
		&p.DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
			Data:        data,
		},
		&p.DomainConfig{
			Retention:      int32(retention),
			EmitMetric:     emitMetric,
			ArchivalBucket: archivalBucketName,
			ArchivalStatus: archivalStatus,
		},
		&p.DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.NoError(err6)
	m.Equal(id, resp6.ID)

	err7 := m.DeleteDomain(id, "")
	m.NoError(err7)

	resp8, err8 := m.GetDomain("", name)
	m.Error(err8)
	m.IsType(&gen.EntityNotExistsError{}, err8)
	m.Nil(resp8)

	resp9, err9 := m.GetDomain(id, "")
	m.Error(err9)
	m.IsType(&gen.EntityNotExistsError{}, err9)
	m.Nil(resp9)
}

// TestListDomains test
func (m *MetadataPersistenceSuiteV2) TestListDomains() {
	clusterActive1 := "some random active cluster name"
	clusterStandby1 := "some random standby cluster name"
	clusters1 := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive1,
		},
		{
			ClusterName: clusterStandby1,
		},
	}

	clusterActive2 := "other random active cluster name"
	clusterStandby2 := "other random standby cluster name"
	clusters2 := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive2,
		},
		{
			ClusterName: clusterStandby2,
		},
	}

	inputDomains := []*p.GetDomainResponse{
		{
			Info: &p.DomainInfo{
				ID:          uuid.New(),
				Name:        "list-domain-test-name-1",
				Status:      p.DomainStatusRegistered,
				Description: "list-domain-test-description-1",
				OwnerEmail:  "list-domain-test-owner-1",
				Data:        map[string]string{"k1": "v1"},
			},
			Config: &p.DomainConfig{
				Retention:      109,
				EmitMetric:     true,
				ArchivalBucket: "bucket-test-name",
				ArchivalStatus: gen.ArchivalStatusEnabled,
			},
			ReplicationConfig: &p.DomainReplicationConfig{
				ActiveClusterName: clusterActive1,
				Clusters:          clusters1,
			},
			IsGlobalDomain:  true,
			ConfigVersion:   133,
			FailoverVersion: 266,
			TableVersion:    p.DomainTableVersionV2,
		},
		{
			Info: &p.DomainInfo{
				ID:          uuid.New(),
				Name:        "list-domain-test-name-2",
				Status:      p.DomainStatusRegistered,
				Description: "list-domain-test-description-2",
				OwnerEmail:  "list-domain-test-owner-2",
				Data:        map[string]string{"k1": "v2"},
			},
			Config: &p.DomainConfig{
				Retention:      326,
				EmitMetric:     false,
				ArchivalBucket: "",
				ArchivalStatus: gen.ArchivalStatusDisabled,
			},
			ReplicationConfig: &p.DomainReplicationConfig{
				ActiveClusterName: clusterActive2,
				Clusters:          clusters2,
			},
			IsGlobalDomain:  false,
			ConfigVersion:   400,
			FailoverVersion: 667,
			TableVersion:    p.DomainTableVersionV2,
		},
	}
	for _, domain := range inputDomains {
		_, err := m.CreateDomain(
			domain.Info,
			domain.Config,
			domain.ReplicationConfig,
			domain.IsGlobalDomain,
			domain.ConfigVersion,
			domain.FailoverVersion,
		)
		m.NoError(err)
	}

	var token []byte
	pageSize := 1
	outputDomains := make(map[string]*p.GetDomainResponse)
ListLoop:
	for {
		resp, err := m.ListDomains(pageSize, token)
		m.NoError(err)
		token = resp.NextPageToken
		for _, domain := range resp.Domains {
			outputDomains[domain.Info.ID] = domain
			// global notification version is already tested, so here we make it 0
			// so we can test == easily
			domain.NotificationVersion = 0
		}
		if len(token) == 0 {
			break ListLoop
		}
	}

	m.Equal(len(inputDomains), len(outputDomains))
	for _, domain := range inputDomains {
		m.Equal(domain, outputDomains[domain.Info.ID])
	}
}

// CreateDomain helper method
func (m *MetadataPersistenceSuiteV2) CreateDomain(info *p.DomainInfo, config *p.DomainConfig,
	replicationConfig *p.DomainReplicationConfig, isGlobaldomain bool, configVersion int64, failoverVersion int64) (*p.CreateDomainResponse, error) {
	return m.MetadataManagerV2.CreateDomain(&p.CreateDomainRequest{
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,
		IsGlobalDomain:    isGlobaldomain,
		ConfigVersion:     configVersion,
		FailoverVersion:   failoverVersion,
	})
}

// GetDomain helper method
func (m *MetadataPersistenceSuiteV2) GetDomain(id, name string) (*p.GetDomainResponse, error) {
	return m.MetadataManagerV2.GetDomain(&p.GetDomainRequest{
		ID:   id,
		Name: name,
	})
}

// UpdateDomain helper method
func (m *MetadataPersistenceSuiteV2) UpdateDomain(info *p.DomainInfo, config *p.DomainConfig, replicationConfig *p.DomainReplicationConfig,
	configVersion int64, failoverVersion int64, failoverNotificationVersion int64, notificationVersion int64) error {
	return m.MetadataManagerV2.UpdateDomain(&p.UpdateDomainRequest{
		Info:                        info,
		Config:                      config,
		ReplicationConfig:           replicationConfig,
		ConfigVersion:               configVersion,
		FailoverVersion:             failoverVersion,
		FailoverNotificationVersion: failoverNotificationVersion,
		NotificationVersion:         notificationVersion,
	})
}

// DeleteDomain helper method
func (m *MetadataPersistenceSuiteV2) DeleteDomain(id, name string) error {
	if len(id) > 0 {
		return m.MetadataManagerV2.DeleteDomain(&p.DeleteDomainRequest{ID: id})
	}
	return m.MetadataManagerV2.DeleteDomainByName(&p.DeleteDomainByNameRequest{Name: name})
}

// ListDomains helper method
func (m *MetadataPersistenceSuiteV2) ListDomains(pageSize int, pageToken []byte) (*p.ListDomainsResponse, error) {
	return m.MetadataManagerV2.ListDomains(&p.ListDomainsRequest{
		PageSize:      pageSize,
		NextPageToken: pageToken,
	})
}
