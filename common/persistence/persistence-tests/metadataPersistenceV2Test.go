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

package persistencetests

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/cluster"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
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

	// cleanup the namespace created
	var token []byte
	pageSize := 10
ListLoop:
	for {
		resp, err := m.ListNamespaces(pageSize, token)
		m.NoError(err)
		token = resp.NextPageToken
		for _, n := range resp.Namespaces {
			m.NoError(m.DeleteNamespace(n.Namespace.Info.Id, ""))
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

// TestCreateNamespace test
func (m *MetadataPersistenceSuiteV2) TestCreateNamespace() {
	id := primitives.UUID(uuid.NewRandom()[:])
	name := "create-namespace-test-name"
	status := namespacepb.NamespaceStatus_Registered
	description := "create-namespace-test-description"
	owner := "create-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "test://history/uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{map[string]*namespacepb.BadBinaryInfo{}}
	isGlobalNamespace := false
	configVersion := int64(0)
	failoverVersion := int64(0)

	resp0, err0 := m.CreateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          id,
			Name:        name,
			Status:      status,
			Description: description,
			Owner:  owner,
			Data:        data,
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            retention,
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
			BadBinaries:              badBinaries,
		},
		&persistenceblobs.NamespaceReplicationConfig{},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err0)
	m.NotNil(resp0)
	m.EqualValues(id, resp0.ID)

	// for namespace which do not have replication config set, will default to
	// use current cluster as active, with current cluster as all clusters
	resp1, err1 := m.GetNamespace(id, "")
	m.NoError(err1)
	m.NotNil(resp1)
	m.EqualValues(id, resp1.Namespace.Info.Id)
	m.Equal(name, resp1.Namespace.Info.Name)
	m.Equal(status, resp1.Namespace.Info.Status)
	m.Equal(description, resp1.Namespace.Info.Description)
	m.Equal(owner, resp1.Namespace.Info.Owner)
	m.Equal(data, resp1.Namespace.Info.Data)
	m.Equal(retention, resp1.Namespace.Config.RetentionDays)
	m.Equal(emitMetric, resp1.Namespace.Config.EmitMetric)
	m.Equal(historyArchivalStatus, resp1.Namespace.Config.HistoryArchivalStatus)
	m.Equal(historyArchivalURI, resp1.Namespace.Config.HistoryArchivalURI)
	m.Equal(visibilityArchivalStatus, resp1.Namespace.Config.VisibilityArchivalStatus)
	m.Equal(visibilityArchivalURI, resp1.Namespace.Config.VisibilityArchivalURI)
	m.Equal(badBinaries, resp1.Namespace.Config.BadBinaries)
	m.Equal(cluster.TestCurrentClusterName, resp1.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(1, len(resp1.Namespace.ReplicationConfig.Clusters))
	m.Equal(isGlobalNamespace, resp1.IsGlobalNamespace)
	m.Equal(configVersion, resp1.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp1.Namespace.FailoverVersion)
	m.True(resp1.Namespace.ReplicationConfig.Clusters[0] == cluster.TestCurrentClusterName)
	m.Equal(p.InitialFailoverNotificationVersion, resp1.Namespace.FailoverNotificationVersion)

	resp2, err2 := m.CreateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          uuid.NewRandom(),
			Name:        name,
			Status:      status,
			Description: "fail",
			Owner:       "fail",
			Data:        map[string]string{},
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            100,
			EmitMetric:               false,
			HistoryArchivalStatus:    namespacepb.ArchivalStatus_Disabled,
			HistoryArchivalURI:       "",
			VisibilityArchivalStatus: namespacepb.ArchivalStatus_Disabled,
			VisibilityArchivalURI:    "",
		},
		&persistenceblobs.NamespaceReplicationConfig{},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.Error(err2)
	m.IsType(&serviceerror.NamespaceAlreadyExists{}, err2)
	m.Nil(resp2)
}

// TestGetNamespace test
func (m *MetadataPersistenceSuiteV2) TestGetNamespace() {
	id := primitives.UUID(uuid.NewRandom()[:])
	name := "get-namespace-test-name"
	status := namespacepb.NamespaceStatus_Registered
	description := "get-namespace-test-description"
	owner := "get-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "test://history/uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(11)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{ clusterActive, clusterStandby }

	resp0, err0 := m.GetNamespace(nil, "does-not-exist")
	m.Nil(resp0)
	m.Error(err0)
	m.IsType(&serviceerror.NotFound{}, err0)
	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:          "test-reason",
				Operator:        "test-operator",
				CreatedTimeNano: 123,
			},
		},
	}

	resp1, err1 := m.CreateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          id,
			Name:        name,
			Status:      status,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            retention,
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
			BadBinaries:              testBinaries,
		},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.NotNil(resp1)
	m.EqualValues(id, resp1.ID)

	resp2, err2 := m.GetNamespace(id, "")
	m.NoError(err2)
	m.NotNil(resp2)
	m.EqualValues(id, resp2.Namespace.Info.Id)
	m.Equal(name, resp2.Namespace.Info.Name)
	m.Equal(status, resp2.Namespace.Info.Status)
	m.Equal(description, resp2.Namespace.Info.Description)
	m.Equal(owner, resp2.Namespace.Info.Owner)
	m.Equal(data, resp2.Namespace.Info.Data)
	m.Equal(retention, resp2.Namespace.Config.RetentionDays)
	m.Equal(emitMetric, resp2.Namespace.Config.EmitMetric)
	m.Equal(historyArchivalStatus, resp2.Namespace.Config.HistoryArchivalStatus)
	m.Equal(historyArchivalURI, resp2.Namespace.Config.HistoryArchivalURI)
	m.Equal(visibilityArchivalStatus, resp2.Namespace.Config.VisibilityArchivalStatus)
	m.Equal(visibilityArchivalURI, resp2.Namespace.Config.VisibilityArchivalURI)
	m.True(reflect.DeepEqual(testBinaries, resp2.Namespace.Config.BadBinaries))
	m.Equal(clusterActive, resp2.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp2.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp2.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalNamespace, resp2.IsGlobalNamespace)
	m.Equal(configVersion, resp2.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp2.Namespace.FailoverVersion)
	m.Equal(p.InitialFailoverNotificationVersion, resp2.Namespace.FailoverNotificationVersion)

	resp3, err3 := m.GetNamespace(nil, name)
	m.NoError(err3)
	m.NotNil(resp3)
	m.EqualValues(id, resp3.Namespace.Info.Id)
	m.Equal(name, resp3.Namespace.Info.Name)
	m.Equal(status, resp3.Namespace.Info.Status)
	m.Equal(description, resp3.Namespace.Info.Description)
	m.Equal(owner, resp3.Namespace.Info.Owner)
	m.Equal(data, resp3.Namespace.Info.Data)
	m.Equal(retention, resp3.Namespace.Config.RetentionDays)
	m.Equal(emitMetric, resp3.Namespace.Config.EmitMetric)
	m.Equal(historyArchivalStatus, resp3.Namespace.Config.HistoryArchivalStatus)
	m.Equal(historyArchivalURI, resp3.Namespace.Config.HistoryArchivalURI)
	m.Equal(visibilityArchivalStatus, resp3.Namespace.Config.VisibilityArchivalStatus)
	m.Equal(visibilityArchivalURI, resp3.Namespace.Config.VisibilityArchivalURI)
	m.Equal(clusterActive, resp3.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp3.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp3.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalNamespace, resp3.IsGlobalNamespace)
	m.Equal(configVersion, resp3.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp3.Namespace.FailoverVersion)
	m.Equal(p.InitialFailoverNotificationVersion, resp3.Namespace.FailoverNotificationVersion)

	resp4, err4 := m.GetNamespace(id, name)
	m.Error(err4)
	m.IsType(&serviceerror.InvalidArgument{}, err4)
	m.Nil(resp4)

	resp5, err5 := m.GetNamespace(nil, "")
	m.Nil(resp5)
	m.IsType(&serviceerror.InvalidArgument{}, err5)
}

// TestConcurrentCreateNamespace test
func (m *MetadataPersistenceSuiteV2) TestConcurrentCreateNamespace() {
	id := primitives.UUID(uuid.NewRandom()[:])

	name := "concurrent-create-namespace-test-name"
	status := namespacepb.NamespaceStatus_Registered
	description := "concurrent-create-namespace-test-description"
	owner := "create-namespace-test-owner"
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "test://history/uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{ clusterActive,  clusterStandby }


	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:          "test-reason",
				Operator:        "test-operator",
				CreatedTimeNano: 123,
			},
		},
	}
	concurrency := 16
	successCount := int32(0)
	var wg sync.WaitGroup
	for i := 1; i <= concurrency; i++ {
		newValue := fmt.Sprintf("v-%v", i)
		wg.Add(1)
		go func(data map[string]string) {
			_, err1 := m.CreateNamespace(
				&persistenceblobs.NamespaceInfo{
					Id:          id,
					Name:        name,
					Status:      status,
					Description: description,
					Owner:       owner,
					Data:        data,
				},
				&persistenceblobs.NamespaceConfig{
					RetentionDays:            retention,
					EmitMetric:               emitMetric,
					HistoryArchivalStatus:    historyArchivalStatus,
					HistoryArchivalURI:       historyArchivalURI,
					VisibilityArchivalStatus: visibilityArchivalStatus,
					VisibilityArchivalURI:    visibilityArchivalURI,
					BadBinaries:              testBinaries,
				},
				&persistenceblobs.NamespaceReplicationConfig{
					ActiveClusterName: clusterActive,
					Clusters:          clusters,
				},
				isGlobalNamespace,
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

	resp, err3 := m.GetNamespace(nil, name)
	m.NoError(err3)
	m.NotNil(resp)
	m.Equal(name, resp.Namespace.Info.Name)
	m.Equal(status, resp.Namespace.Info.Status)
	m.Equal(description, resp.Namespace.Info.Description)
	m.Equal(owner, resp.Namespace.Info.Owner)
	m.Equal(retention, resp.Namespace.Config.RetentionDays)
	m.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	m.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	m.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalURI)
	m.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	m.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalURI)
	m.True(reflect.DeepEqual(testBinaries, resp.Namespace.Config.BadBinaries))
	m.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalNamespace, resp.IsGlobalNamespace)
	m.Equal(configVersion, resp.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp.Namespace.FailoverVersion)

	//check namespace data
	ss := strings.Split(resp.Namespace.Info.Data["k0"], "-")
	m.Equal(2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	m.NoError(err)
	m.Equal(true, vi > 0 && vi <= concurrency)
}

// TestConcurrentUpdateNamespace test
func (m *MetadataPersistenceSuiteV2) TestConcurrentUpdateNamespace() {
	id := primitives.UUID(uuid.NewRandom()[:])
	name := "concurrent-update-namespace-test-name"
	status := namespacepb.NamespaceStatus_Registered
	description := "update-namespace-test-description"
	owner := "update-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "test://history/uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{map[string]*namespacepb.BadBinaryInfo{}}

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{ clusterActive,  clusterStandby, }

	resp1, err1 := m.CreateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          id,
			Name:        name,
			Status:      status,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            retention,
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
			BadBinaries:              badBinaries,
		},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.EqualValues(id, resp1.ID)

	resp2, err2 := m.GetNamespace(id, "")
	m.NoError(err2)
	m.Equal(badBinaries, resp2.Namespace.Config.BadBinaries)
	metadata, err := m.MetadataManager.GetMetadata()
	m.NoError(err)
	notificationVersion := metadata.NotificationVersion

	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:          "test-reason",
				Operator:        "test-operator",
				CreatedTimeNano: 123,
			},
		},
	}
	concurrency := 16
	successCount := int32(0)
	var wg sync.WaitGroup
	for i := 1; i <= concurrency; i++ {
		newValue := fmt.Sprintf("v-%v", i)
		wg.Add(1)
		go func(updatedData map[string]string) {
			err3 := m.UpdateNamespace(
				&persistenceblobs.NamespaceInfo{
					Id:          resp2.Namespace.Info.Id,
					Name:        resp2.Namespace.Info.Name,
					Status:      resp2.Namespace.Info.Status,
					Description: resp2.Namespace.Info.Description,
					Owner:       resp2.Namespace.Info.Owner,
					Data:        updatedData,
				},
				&persistenceblobs.NamespaceConfig{
					RetentionDays:            resp2.Namespace.Config.RetentionDays,
					EmitMetric:               resp2.Namespace.Config.EmitMetric,
					HistoryArchivalStatus:    resp2.Namespace.Config.HistoryArchivalStatus,
					HistoryArchivalURI:       resp2.Namespace.Config.HistoryArchivalURI,
					VisibilityArchivalStatus: resp2.Namespace.Config.VisibilityArchivalStatus,
					VisibilityArchivalURI:    resp2.Namespace.Config.VisibilityArchivalURI,
					BadBinaries:              testBinaries,
				},
				&persistenceblobs.NamespaceReplicationConfig{
					ActiveClusterName: resp2.Namespace.ReplicationConfig.ActiveClusterName,
					Clusters:          resp2.Namespace.ReplicationConfig.Clusters,
				},
				resp2.Namespace.ConfigVersion,
				resp2.Namespace.FailoverVersion,
				resp2.Namespace.FailoverNotificationVersion,
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

	resp3, err3 := m.GetNamespace(nil, name)
	m.NoError(err3)
	m.NotNil(resp3)
	m.EqualValues(id, resp3.Namespace.Info.Id)
	m.Equal(name, resp3.Namespace.Info.Name)
	m.Equal(status, resp3.Namespace.Info.Status)
	m.Equal(isGlobalNamespace, resp3.IsGlobalNamespace)
	m.Equal(description, resp3.Namespace.Info.Description)
	m.Equal(owner, resp3.Namespace.Info.Owner)

	m.Equal(retention, resp3.Namespace.Config.RetentionDays)
	m.Equal(emitMetric, resp3.Namespace.Config.EmitMetric)
	m.Equal(historyArchivalStatus, resp3.Namespace.Config.HistoryArchivalStatus)
	m.Equal(historyArchivalURI, resp3.Namespace.Config.HistoryArchivalURI)
	m.Equal(visibilityArchivalStatus, resp3.Namespace.Config.VisibilityArchivalStatus)
	m.Equal(visibilityArchivalURI, resp3.Namespace.Config.VisibilityArchivalURI)
	m.True(reflect.DeepEqual(testBinaries, resp3.Namespace.Config.BadBinaries))
	m.Equal(clusterActive, resp3.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp3.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp3.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalNamespace, resp3.IsGlobalNamespace)
	m.Equal(configVersion, resp3.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp3.Namespace.FailoverVersion)

	//check namespace data
	ss := strings.Split(resp3.Namespace.Info.Data["k0"], "-")
	m.Equal(2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	m.NoError(err)
	m.Equal(true, vi > 0 && vi <= concurrency)
}

// TestUpdateNamespace test
func (m *MetadataPersistenceSuiteV2) TestUpdateNamespace() {
	id := primitives.UUID(uuid.NewRandom()[:])
	name := "update-namespace-test-name"
	status := namespacepb.NamespaceStatus_Registered
	description := "update-namespace-test-description"
	owner := "update-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "test://history/uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{ clusterActive, clusterStandby }

	resp1, err1 := m.CreateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          id,
			Name:        name,
			Status:      status,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            retention,
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
		},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.EqualValues(id, resp1.ID)

	resp2, err2 := m.GetNamespace(id, "")
	m.NoError(err2)
	metadata, err := m.MetadataManager.GetMetadata()
	m.NoError(err)
	notificationVersion := metadata.NotificationVersion

	updatedStatus := namespacepb.NamespaceStatus_Deprecated
	updatedDescription := "description-updated"
	updatedOwner := "owner-updated"
	//This will overriding the previous key-value pair
	updatedData := map[string]string{"k1": "v2"}
	updatedRetention := int32(20)
	updatedEmitMetric := false
	updatedHistoryArchivalStatus := namespacepb.ArchivalStatus_Disabled
	updatedHistoryArchivalURI := ""
	updatedVisibilityArchivalStatus := namespacepb.ArchivalStatus_Disabled
	updatedVisibilityArchivalURI := ""

	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := int64(12)
	updateFailoverVersion := int64(28)
	updateFailoverNotificationVersion := int64(14)
	updateClusters := []string{ updateClusterActive, updateClusterStandby }

	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:          "test-reason",
				Operator:        "test-operator",
				CreatedTimeNano: 123,
			},
		},
	}

	err3 := m.UpdateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          resp2.Namespace.Info.Id,
			Name:        resp2.Namespace.Info.Name,
			Status:      updatedStatus,
			Description: updatedDescription,
			Owner:       updatedOwner,
			Data:        updatedData,
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            updatedRetention,
			EmitMetric:               updatedEmitMetric,
			HistoryArchivalStatus:    updatedHistoryArchivalStatus,
			HistoryArchivalURI:       updatedHistoryArchivalURI,
			VisibilityArchivalStatus: updatedVisibilityArchivalStatus,
			VisibilityArchivalURI:    updatedVisibilityArchivalURI,
			BadBinaries:              testBinaries,
		},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		updateConfigVersion,
		updateFailoverVersion,
		updateFailoverNotificationVersion,
		notificationVersion,
	)
	m.NoError(err3)

	resp4, err4 := m.GetNamespace(nil, name)
	m.NoError(err4)
	m.NotNil(resp4)
	m.EqualValues(id, resp4.Namespace.Info.Id)
	m.Equal(name, resp4.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp4.IsGlobalNamespace)
	m.Equal(updatedStatus, resp4.Namespace.Info.Status)
	m.Equal(updatedDescription, resp4.Namespace.Info.Description)
	m.Equal(updatedOwner, resp4.Namespace.Info.Owner)
	m.Equal(updatedData, resp4.Namespace.Info.Data)
	m.Equal(updatedRetention, resp4.Namespace.Config.RetentionDays)
	m.Equal(updatedEmitMetric, resp4.Namespace.Config.EmitMetric)
	m.Equal(updatedHistoryArchivalStatus, resp4.Namespace.Config.HistoryArchivalStatus)
	m.Equal(updatedHistoryArchivalURI, resp4.Namespace.Config.HistoryArchivalURI)
	m.Equal(updatedVisibilityArchivalStatus, resp4.Namespace.Config.VisibilityArchivalStatus)
	m.Equal(updatedVisibilityArchivalURI, resp4.Namespace.Config.VisibilityArchivalURI)
	m.True(reflect.DeepEqual(testBinaries, resp4.Namespace.Config.BadBinaries))
	m.Equal(updateClusterActive, resp4.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp4.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp4.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateConfigVersion, resp4.Namespace.ConfigVersion)
	m.Equal(updateFailoverVersion, resp4.Namespace.FailoverVersion)
	m.Equal(updateFailoverNotificationVersion, resp4.Namespace.FailoverNotificationVersion)
	m.Equal(notificationVersion, resp4.NotificationVersion)

	resp5, err5 := m.GetNamespace(id, "")
	m.NoError(err5)
	m.NotNil(resp5)
	m.EqualValues(id, resp5.Namespace.Info.Id)
	m.Equal(name, resp5.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp5.IsGlobalNamespace)
	m.Equal(updatedStatus, resp5.Namespace.Info.Status)
	m.Equal(updatedDescription, resp5.Namespace.Info.Description)
	m.Equal(updatedOwner, resp5.Namespace.Info.Owner)
	m.Equal(updatedData, resp5.Namespace.Info.Data)
	m.Equal(updatedRetention, resp5.Namespace.Config.RetentionDays)
	m.Equal(updatedEmitMetric, resp5.Namespace.Config.EmitMetric)
	m.Equal(updatedHistoryArchivalStatus, resp5.Namespace.Config.HistoryArchivalStatus)
	m.Equal(updatedHistoryArchivalURI, resp5.Namespace.Config.HistoryArchivalURI)
	m.Equal(updatedVisibilityArchivalStatus, resp5.Namespace.Config.VisibilityArchivalStatus)
	m.Equal(updatedVisibilityArchivalURI, resp5.Namespace.Config.VisibilityArchivalURI)
	m.Equal(updateClusterActive, resp5.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp5.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp5.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateConfigVersion, resp5.Namespace.ConfigVersion)
	m.Equal(updateFailoverVersion, resp5.Namespace.FailoverVersion)
	m.Equal(updateFailoverNotificationVersion, resp5.Namespace.FailoverNotificationVersion)
	m.Equal(notificationVersion, resp5.NotificationVersion)
}

// TestDeleteNamespace test
func (m *MetadataPersistenceSuiteV2) TestDeleteNamespace() {
	id := primitives.UUID(uuid.NewRandom()[:])
	name := "delete-namespace-test-name"
	status := namespacepb.NamespaceStatus_Registered
	description := "delete-namespace-test-description"
	owner := "delete-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := 10
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "test://history/uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{ clusterActive, clusterStandby }

	resp1, err1 := m.CreateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          id,
			Name:        name,
			Status:      status,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            int32(retention),
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
		},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.EqualValues(id, resp1.ID)

	resp2, err2 := m.GetNamespace(nil, name)
	m.NoError(err2)
	m.NotNil(resp2)

	err3 := m.DeleteNamespace(nil, name)
	m.NoError(err3)

	resp4, err4 := m.GetNamespace(nil, name)
	m.Error(err4)
	m.IsType(&serviceerror.NotFound{}, err4)
	m.Nil(resp4)

	resp5, err5 := m.GetNamespace(id, "")
	m.Error(err5)
	m.IsType(&serviceerror.NotFound{}, err5)
	m.Nil(resp5)

	id = primitives.UUID(uuid.NewRandom())
	resp6, err6 := m.CreateNamespace(
		&persistenceblobs.NamespaceInfo{
			Id:          id,
			Name:        name,
			Status:      status,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistenceblobs.NamespaceConfig{
			RetentionDays:            int32(retention),
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
		},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err6)
	m.EqualValues(id, resp6.ID)

	err7 := m.DeleteNamespace(id, "")
	m.NoError(err7)

	resp8, err8 := m.GetNamespace(nil, name)
	m.Error(err8)
	m.IsType(&serviceerror.NotFound{}, err8)
	m.Nil(resp8)

	resp9, err9 := m.GetNamespace(id, "")
	m.Error(err9)
	m.IsType(&serviceerror.NotFound{}, err9)
	m.Nil(resp9)
}

// TestListNamespaces test
func (m *MetadataPersistenceSuiteV2) TestListNamespaces() {
	clusterActive1 := "some random active cluster name"
	clusterStandby1 := "some random standby cluster name"
	clusters1 := []string{ clusterActive1,  clusterStandby1 }


	clusterActive2 := "other random active cluster name"
	clusterStandby2 := "other random standby cluster name"
	clusters2 := []string{ clusterActive2, clusterStandby2 }


	testBinaries1 := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:          "test-reason1",
				Operator:        "test-operator1",
				CreatedTimeNano: 123,
			},
		},
	}
	testBinaries2 := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"efg": {
				Reason:          "test-reason2",
				Operator:        "test-operator2",
				CreatedTimeNano: 456,
			},
		},
	}

	inputNamespaces := []*p.GetNamespaceResponse{
		{
			Namespace: &persistenceblobs.NamespaceDetail{
				Info: &persistenceblobs.NamespaceInfo{
					Id:          uuid.NewRandom(),
					Name:        "list-namespace-test-name-1",
					Status:      namespacepb.NamespaceStatus_Registered,
					Description: "list-namespace-test-description-1",
					Owner:       "list-namespace-test-owner-1",
					Data:        map[string]string{"k1": "v1"},
				},
				Config: &persistenceblobs.NamespaceConfig{
					RetentionDays:            109,
					EmitMetric:               true,
					HistoryArchivalStatus:    namespacepb.ArchivalStatus_Enabled,
					HistoryArchivalURI:       "test://history/uri",
					VisibilityArchivalStatus: namespacepb.ArchivalStatus_Enabled,
					VisibilityArchivalURI:    "test://visibility/uri",
					BadBinaries:              testBinaries1,
				},
				ReplicationConfig: &persistenceblobs.NamespaceReplicationConfig{
					ActiveClusterName: clusterActive1,
					Clusters:          clusters1,
				},

				ConfigVersion:   133,
				FailoverVersion: 266,
			},
			IsGlobalNamespace: true,
		},
		{
			Namespace: &persistenceblobs.NamespaceDetail{
				Info: &persistenceblobs.NamespaceInfo{
					Id:          uuid.NewRandom(),
					Name:        "list-namespace-test-name-2",
					Status:      namespacepb.NamespaceStatus_Registered,
					Description: "list-namespace-test-description-2",
					Owner:       "list-namespace-test-owner-2",
					Data:        map[string]string{"k1": "v2"},
				},
				Config: &persistenceblobs.NamespaceConfig{
					RetentionDays:            326,
					EmitMetric:               false,
					HistoryArchivalStatus:    namespacepb.ArchivalStatus_Disabled,
					HistoryArchivalURI:       "",
					VisibilityArchivalStatus: namespacepb.ArchivalStatus_Disabled,
					VisibilityArchivalURI:    "",
					BadBinaries:              testBinaries2,
				},
				ReplicationConfig: &persistenceblobs.NamespaceReplicationConfig{
					ActiveClusterName: clusterActive2,
					Clusters:          clusters2,
				},
				ConfigVersion:     400,
				FailoverVersion:   667,
			},
			IsGlobalNamespace: false,
		},
	}
	for _, namespace := range inputNamespaces {
		_, err := m.CreateNamespace(
			namespace.Namespace.Info,
			namespace.Namespace.Config,
			namespace.Namespace.ReplicationConfig,
			namespace.IsGlobalNamespace,
			namespace.Namespace.ConfigVersion,
			namespace.Namespace.FailoverVersion,
		)
		m.NoError(err)
	}

	var token []byte
	pageSize := 1
	outputNamespaces := make(map[string]*p.GetNamespaceResponse)
ListLoop:
	for {
		resp, err := m.ListNamespaces(pageSize, token)
		m.NoError(err)
		token = resp.NextPageToken
		for _, namespace := range resp.Namespaces {
			outputNamespaces[string(namespace.Namespace.Info.Id)] = namespace
			// global notification version is already tested, so here we make it 0
			// so we can test == easily
			namespace.NotificationVersion = 0
		}
		if len(token) == 0 {
			break ListLoop
		}
	}

	m.Equal(len(inputNamespaces), len(outputNamespaces))
	for _, namespace := range inputNamespaces {
		m.Equal(namespace, outputNamespaces[string(namespace.Namespace.Info.Id)])
	}
}

// CreateNamespace helper method
func (m *MetadataPersistenceSuiteV2) CreateNamespace(info *persistenceblobs.NamespaceInfo, config *persistenceblobs.NamespaceConfig,
	replicationConfig *persistenceblobs.NamespaceReplicationConfig, isGlobalnamespace bool, configVersion int64, failoverVersion int64) (*p.CreateNamespaceResponse, error) {
	return m.MetadataManager.CreateNamespace(&p.CreateNamespaceRequest{
		Namespace: &persistenceblobs.NamespaceDetail{
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,

		ConfigVersion:     configVersion,
		FailoverVersion:   failoverVersion,
	}, IsGlobalNamespace: isGlobalnamespace,
	})
}

// GetNamespace helper method
func (m *MetadataPersistenceSuiteV2) GetNamespace(id primitives.UUID, name string) (*p.GetNamespaceResponse, error) {
	return m.MetadataManager.GetNamespace(&p.GetNamespaceRequest{
		ID:   id,
		Name: name,
	})
}

// UpdateNamespace helper method
func (m *MetadataPersistenceSuiteV2) UpdateNamespace(
	info *persistenceblobs.NamespaceInfo,
	config *persistenceblobs.NamespaceConfig,
	replicationConfig *persistenceblobs.NamespaceReplicationConfig,
	configVersion int64, failoverVersion int64,
	failoverNotificationVersion int64, notificationVersion int64,
	) error {
	return m.MetadataManager.UpdateNamespace(&p.UpdateNamespaceRequest{
		Namespace: &persistenceblobs.NamespaceDetail{
			Info:                        info,
			Config:                      config,
			ReplicationConfig:           replicationConfig,
			ConfigVersion:               configVersion,
			FailoverVersion:             failoverVersion,
			FailoverNotificationVersion: failoverNotificationVersion,
		},
		NotificationVersion: notificationVersion,
	})
}

// DeleteNamespace helper method
func (m *MetadataPersistenceSuiteV2) DeleteNamespace(id primitives.UUID, name string) error {
	if len(id) > 0 {
		return m.MetadataManager.DeleteNamespace(&p.DeleteNamespaceRequest{ID: id})
	}
	return m.MetadataManager.DeleteNamespaceByName(&p.DeleteNamespaceByNameRequest{Name: name})
}

// ListNamespaces helper method
func (m *MetadataPersistenceSuiteV2) ListNamespaces(pageSize int, pageToken []byte) (*p.ListNamespacesResponse, error) {
	return m.MetadataManager.ListNamespaces(&p.ListNamespacesRequest{
		PageSize:      pageSize,
		NextPageToken: pageToken,
	})
}
