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
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	// MetadataPersistenceSuiteV2 is test of the V2 version of metadata persistence
	MetadataPersistenceSuiteV2 struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions

		ctx    context.Context
		cancel context.CancelFunc
	}
)

// SetupSuite implementation
func (m *MetadataPersistenceSuiteV2) SetupSuite() {
}

// SetupTest implementation
func (m *MetadataPersistenceSuiteV2) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	m.Assertions = require.New(m.T())
	m.ctx, m.cancel = context.WithTimeout(context.Background(), time.Second*30)

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
	m.cancel()
}

// TearDownSuite implementation
func (m *MetadataPersistenceSuiteV2) TearDownSuite() {
	m.TearDownWorkflowStore()
}

// TestCreateNamespace test
func (m *MetadataPersistenceSuiteV2) TestCreateNamespace() {
	id := uuid.New()
	name := "create-namespace-test-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "create-namespace-test-description"
	owner := "create-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{map[string]*namespacepb.BadBinaryInfo{}}
	isGlobalNamespace := false
	configVersion := int64(0)
	failoverVersion := int64(0)

	resp0, err0 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(retention),
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
			BadBinaries:             badBinaries,
		},
		&persistencespb.NamespaceReplicationConfig{},
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
	m.Equal(state, resp1.Namespace.Info.State)
	m.Equal(description, resp1.Namespace.Info.Description)
	m.Equal(owner, resp1.Namespace.Info.Owner)
	m.Equal(data, resp1.Namespace.Info.Data)
	m.EqualValues(time.Duration(retention)*time.Hour*24, *resp1.Namespace.Config.Retention)
	m.Equal(historyArchivalState, resp1.Namespace.Config.HistoryArchivalState)
	m.Equal(historyArchivalURI, resp1.Namespace.Config.HistoryArchivalUri)
	m.Equal(visibilityArchivalState, resp1.Namespace.Config.VisibilityArchivalState)
	m.Equal(visibilityArchivalURI, resp1.Namespace.Config.VisibilityArchivalUri)
	m.Equal(badBinaries, resp1.Namespace.Config.BadBinaries)
	m.Equal(cluster.TestCurrentClusterName, resp1.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(1, len(resp1.Namespace.ReplicationConfig.Clusters))
	m.Equal(isGlobalNamespace, resp1.IsGlobalNamespace)
	m.Equal(configVersion, resp1.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp1.Namespace.FailoverVersion)
	m.True(resp1.Namespace.ReplicationConfig.Clusters[0] == cluster.TestCurrentClusterName)
	m.Equal(p.InitialFailoverNotificationVersion, resp1.Namespace.FailoverNotificationVersion)

	resp2, err2 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          uuid.New(),
			Name:        name,
			State:       state,
			Description: "fail",
			Owner:       "fail",
			Data:        map[string]string{},
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(100),
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:      "",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		&persistencespb.NamespaceReplicationConfig{},
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
	id := uuid.New()
	name := "get-namespace-test-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "get-namespace-test-description"
	owner := "get-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(11)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{clusterActive, clusterStandby}

	resp0, err0 := m.GetNamespace("", "does-not-exist")
	m.Nil(resp0)
	m.Error(err0)
	m.IsType(&serviceerror.NamespaceNotFound{}, err0)
	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:     "test-reason",
				Operator:   "test-operator",
				CreateTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(retention),
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
			BadBinaries:             testBinaries,
		},
		&persistencespb.NamespaceReplicationConfig{
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
	m.Equal(state, resp2.Namespace.Info.State)
	m.Equal(description, resp2.Namespace.Info.Description)
	m.Equal(owner, resp2.Namespace.Info.Owner)
	m.Equal(data, resp2.Namespace.Info.Data)
	m.EqualValues(time.Duration(retention)*time.Hour*24, *resp2.Namespace.Config.Retention)
	m.Equal(historyArchivalState, resp2.Namespace.Config.HistoryArchivalState)
	m.Equal(historyArchivalURI, resp2.Namespace.Config.HistoryArchivalUri)
	m.Equal(visibilityArchivalState, resp2.Namespace.Config.VisibilityArchivalState)
	m.Equal(visibilityArchivalURI, resp2.Namespace.Config.VisibilityArchivalUri)
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

	resp3, err3 := m.GetNamespace("", name)
	m.NoError(err3)
	m.NotNil(resp3)
	m.EqualValues(id, resp3.Namespace.Info.Id)
	m.Equal(name, resp3.Namespace.Info.Name)
	m.Equal(state, resp3.Namespace.Info.State)
	m.Equal(description, resp3.Namespace.Info.Description)
	m.Equal(owner, resp3.Namespace.Info.Owner)
	m.Equal(data, resp3.Namespace.Info.Data)
	m.EqualValues(time.Duration(retention)*time.Hour*24, *resp3.Namespace.Config.Retention)
	m.Equal(historyArchivalState, resp3.Namespace.Config.HistoryArchivalState)
	m.Equal(historyArchivalURI, resp3.Namespace.Config.HistoryArchivalUri)
	m.Equal(visibilityArchivalState, resp3.Namespace.Config.VisibilityArchivalState)
	m.Equal(visibilityArchivalURI, resp3.Namespace.Config.VisibilityArchivalUri)
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

	resp5, err5 := m.GetNamespace("", "")
	m.Nil(resp5)
	m.IsType(&serviceerror.InvalidArgument{}, err5)
}

// TestConcurrentCreateNamespace test
func (m *MetadataPersistenceSuiteV2) TestConcurrentCreateNamespace() {
	id := uuid.New()

	name := "concurrent-create-namespace-test-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "concurrent-create-namespace-test-description"
	owner := "create-namespace-test-owner"
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{clusterActive, clusterStandby}

	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:     "test-reason",
				Operator:   "test-operator",
				CreateTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
				&persistencespb.NamespaceInfo{
					Id:          id,
					Name:        name,
					State:       state,
					Description: description,
					Owner:       owner,
					Data:        data,
				},
				&persistencespb.NamespaceConfig{
					Retention:               timestamp.DurationFromDays(retention),
					HistoryArchivalState:    historyArchivalState,
					HistoryArchivalUri:      historyArchivalURI,
					VisibilityArchivalState: visibilityArchivalState,
					VisibilityArchivalUri:   visibilityArchivalURI,
					BadBinaries:             testBinaries,
				},
				&persistencespb.NamespaceReplicationConfig{
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

	resp, err3 := m.GetNamespace("", name)
	m.NoError(err3)
	m.NotNil(resp)
	m.Equal(name, resp.Namespace.Info.Name)
	m.Equal(state, resp.Namespace.Info.State)
	m.Equal(description, resp.Namespace.Info.Description)
	m.Equal(owner, resp.Namespace.Info.Owner)
	m.EqualValues(time.Duration(retention)*time.Hour*24, *resp.Namespace.Config.Retention)
	m.Equal(historyArchivalState, resp.Namespace.Config.HistoryArchivalState)
	m.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	m.Equal(visibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	m.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	m.True(reflect.DeepEqual(testBinaries, resp.Namespace.Config.BadBinaries))
	m.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalNamespace, resp.IsGlobalNamespace)
	m.Equal(configVersion, resp.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp.Namespace.FailoverVersion)

	// check namespace data
	ss := strings.Split(resp.Namespace.Info.Data["k0"], "-")
	m.Equal(2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	m.NoError(err)
	m.Equal(true, vi > 0 && vi <= concurrency)
}

// TestConcurrentUpdateNamespace test
func (m *MetadataPersistenceSuiteV2) TestConcurrentUpdateNamespace() {
	id := uuid.New()
	name := "concurrent-update-namespace-test-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "update-namespace-test-description"
	owner := "update-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{map[string]*namespacepb.BadBinaryInfo{}}

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{clusterActive, clusterStandby}

	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(retention),
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
			BadBinaries:             badBinaries,
		},
		&persistencespb.NamespaceReplicationConfig{
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
	metadata, err := m.MetadataManager.GetMetadata(m.ctx)
	m.NoError(err)
	notificationVersion := metadata.NotificationVersion

	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:     "test-reason",
				Operator:   "test-operator",
				CreateTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
				&persistencespb.NamespaceInfo{
					Id:          resp2.Namespace.Info.Id,
					Name:        resp2.Namespace.Info.Name,
					State:       resp2.Namespace.Info.State,
					Description: resp2.Namespace.Info.Description,
					Owner:       resp2.Namespace.Info.Owner,
					Data:        updatedData,
				},
				&persistencespb.NamespaceConfig{
					Retention:               resp2.Namespace.Config.Retention,
					HistoryArchivalState:    resp2.Namespace.Config.HistoryArchivalState,
					HistoryArchivalUri:      resp2.Namespace.Config.HistoryArchivalUri,
					VisibilityArchivalState: resp2.Namespace.Config.VisibilityArchivalState,
					VisibilityArchivalUri:   resp2.Namespace.Config.VisibilityArchivalUri,
					BadBinaries:             testBinaries,
				},
				&persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: resp2.Namespace.ReplicationConfig.ActiveClusterName,
					Clusters:          resp2.Namespace.ReplicationConfig.Clusters,
				},
				resp2.Namespace.ConfigVersion,
				resp2.Namespace.FailoverVersion,
				resp2.Namespace.FailoverNotificationVersion,
				&time.Time{},
				notificationVersion,
				isGlobalNamespace,
			)
			if err3 == nil {
				atomic.AddInt32(&successCount, 1)
			}
			wg.Done()
		}(map[string]string{"k0": newValue})
	}
	wg.Wait()
	m.Equal(int32(1), successCount)

	resp3, err3 := m.GetNamespace("", name)
	m.NoError(err3)
	m.NotNil(resp3)
	m.EqualValues(id, resp3.Namespace.Info.Id)
	m.Equal(name, resp3.Namespace.Info.Name)
	m.Equal(state, resp3.Namespace.Info.State)
	m.Equal(isGlobalNamespace, resp3.IsGlobalNamespace)
	m.Equal(description, resp3.Namespace.Info.Description)
	m.Equal(owner, resp3.Namespace.Info.Owner)

	m.EqualValues(time.Duration(retention)*time.Hour*24, *resp3.Namespace.Config.Retention)
	m.Equal(historyArchivalState, resp3.Namespace.Config.HistoryArchivalState)
	m.Equal(historyArchivalURI, resp3.Namespace.Config.HistoryArchivalUri)
	m.Equal(visibilityArchivalState, resp3.Namespace.Config.VisibilityArchivalState)
	m.Equal(visibilityArchivalURI, resp3.Namespace.Config.VisibilityArchivalUri)
	m.True(reflect.DeepEqual(testBinaries, resp3.Namespace.Config.BadBinaries))
	m.Equal(clusterActive, resp3.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(clusters), len(resp3.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(clusters[index], resp3.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(isGlobalNamespace, resp3.IsGlobalNamespace)
	m.Equal(configVersion, resp3.Namespace.ConfigVersion)
	m.Equal(failoverVersion, resp3.Namespace.FailoverVersion)

	// check namespace data
	ss := strings.Split(resp3.Namespace.Info.Data["k0"], "-")
	m.Equal(2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	m.NoError(err)
	m.Equal(true, vi > 0 && vi <= concurrency)
}

// TestUpdateNamespace test
func (m *MetadataPersistenceSuiteV2) TestUpdateNamespace() {
	id := uuid.New()
	name := "update-namespace-test-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "update-namespace-test-description"
	owner := "update-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	failoverEndTime := time.Now().UTC()
	isGlobalNamespace := true
	clusters := []string{clusterActive, clusterStandby}

	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(retention),
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
		},
		&persistencespb.NamespaceReplicationConfig{
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
	metadata, err := m.MetadataManager.GetMetadata(m.ctx)
	m.NoError(err)
	notificationVersion := metadata.NotificationVersion

	updatedState := enumspb.NAMESPACE_STATE_DEPRECATED
	updatedDescription := "description-updated"
	updatedOwner := "owner-updated"
	// This will overriding the previous key-value pair
	updatedData := map[string]string{"k1": "v2"}
	updatedRetention := timestamp.DurationFromDays(20)
	updatedHistoryArchivalState := enumspb.ARCHIVAL_STATE_DISABLED
	updatedHistoryArchivalURI := ""
	updatedVisibilityArchivalState := enumspb.ARCHIVAL_STATE_DISABLED
	updatedVisibilityArchivalURI := ""

	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := int64(12)
	updateFailoverVersion := int64(28)
	updateFailoverNotificationVersion := int64(14)
	updateClusters := []string{updateClusterActive, updateClusterStandby}

	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:     "test-reason",
				Operator:   "test-operator",
				CreateTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	err3 := m.UpdateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          resp2.Namespace.Info.Id,
			Name:        resp2.Namespace.Info.Name,
			State:       updatedState,
			Description: updatedDescription,
			Owner:       updatedOwner,
			Data:        updatedData,
		},
		&persistencespb.NamespaceConfig{
			Retention:               updatedRetention,
			HistoryArchivalState:    updatedHistoryArchivalState,
			HistoryArchivalUri:      updatedHistoryArchivalURI,
			VisibilityArchivalState: updatedVisibilityArchivalState,
			VisibilityArchivalUri:   updatedVisibilityArchivalURI,
			BadBinaries:             testBinaries,
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		updateConfigVersion,
		updateFailoverVersion,
		updateFailoverNotificationVersion,
		&failoverEndTime,
		notificationVersion,
		isGlobalNamespace,
	)
	m.NoError(err3)

	resp4, err4 := m.GetNamespace("", name)
	m.NoError(err4)
	m.NotNil(resp4)
	m.EqualValues(id, resp4.Namespace.Info.Id)
	m.Equal(name, resp4.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp4.IsGlobalNamespace)
	m.Equal(updatedState, resp4.Namespace.Info.State)
	m.Equal(updatedDescription, resp4.Namespace.Info.Description)
	m.Equal(updatedOwner, resp4.Namespace.Info.Owner)
	m.Equal(updatedData, resp4.Namespace.Info.Data)
	m.EqualValues(*updatedRetention, *resp4.Namespace.Config.Retention)
	m.Equal(updatedHistoryArchivalState, resp4.Namespace.Config.HistoryArchivalState)
	m.Equal(updatedHistoryArchivalURI, resp4.Namespace.Config.HistoryArchivalUri)
	m.Equal(updatedVisibilityArchivalState, resp4.Namespace.Config.VisibilityArchivalState)
	m.Equal(updatedVisibilityArchivalURI, resp4.Namespace.Config.VisibilityArchivalUri)
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
	m.EqualTimes(failoverEndTime, *resp4.Namespace.FailoverEndTime)

	resp5, err5 := m.GetNamespace(id, "")
	m.NoError(err5)
	m.NotNil(resp5)
	m.EqualValues(id, resp5.Namespace.Info.Id)
	m.Equal(name, resp5.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp5.IsGlobalNamespace)
	m.Equal(updatedState, resp5.Namespace.Info.State)
	m.Equal(updatedDescription, resp5.Namespace.Info.Description)
	m.Equal(updatedOwner, resp5.Namespace.Info.Owner)
	m.Equal(updatedData, resp5.Namespace.Info.Data)
	m.EqualValues(*updatedRetention, *resp5.Namespace.Config.Retention)
	m.Equal(updatedHistoryArchivalState, resp5.Namespace.Config.HistoryArchivalState)
	m.Equal(updatedHistoryArchivalURI, resp5.Namespace.Config.HistoryArchivalUri)
	m.Equal(updatedVisibilityArchivalState, resp5.Namespace.Config.VisibilityArchivalState)
	m.Equal(updatedVisibilityArchivalURI, resp5.Namespace.Config.VisibilityArchivalUri)
	m.Equal(updateClusterActive, resp5.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp5.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp5.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateConfigVersion, resp5.Namespace.ConfigVersion)
	m.Equal(updateFailoverVersion, resp5.Namespace.FailoverVersion)
	m.Equal(updateFailoverNotificationVersion, resp5.Namespace.FailoverNotificationVersion)
	m.Equal(notificationVersion, resp5.NotificationVersion)
	m.EqualTimes(failoverEndTime, *resp4.Namespace.FailoverEndTime)

	notificationVersion++
	err6 := m.UpdateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          resp2.Namespace.Info.Id,
			Name:        resp2.Namespace.Info.Name,
			State:       updatedState,
			Description: updatedDescription,
			Owner:       updatedOwner,
			Data:        updatedData,
		},
		&persistencespb.NamespaceConfig{
			Retention:               updatedRetention,
			HistoryArchivalState:    updatedHistoryArchivalState,
			HistoryArchivalUri:      updatedHistoryArchivalURI,
			VisibilityArchivalState: updatedVisibilityArchivalState,
			VisibilityArchivalUri:   updatedVisibilityArchivalURI,
			BadBinaries:             testBinaries,
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		updateConfigVersion,
		updateFailoverVersion,
		updateFailoverNotificationVersion,
		&time.Time{},
		notificationVersion,
		isGlobalNamespace,
	)
	m.NoError(err6)

	resp6, err6 := m.GetNamespace(id, "")
	m.NoError(err6)
	m.NotNil(resp6)
	m.EqualValues(id, resp6.Namespace.Info.Id)
	m.Equal(name, resp6.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp6.IsGlobalNamespace)
	m.Equal(updatedState, resp6.Namespace.Info.State)
	m.Equal(updatedDescription, resp6.Namespace.Info.Description)
	m.Equal(updatedOwner, resp6.Namespace.Info.Owner)
	m.Equal(updatedData, resp6.Namespace.Info.Data)
	m.EqualValues(*updatedRetention, *resp6.Namespace.Config.Retention)
	m.Equal(updatedHistoryArchivalState, resp6.Namespace.Config.HistoryArchivalState)
	m.Equal(updatedHistoryArchivalURI, resp6.Namespace.Config.HistoryArchivalUri)
	m.Equal(updatedVisibilityArchivalState, resp6.Namespace.Config.VisibilityArchivalState)
	m.Equal(updatedVisibilityArchivalURI, resp6.Namespace.Config.VisibilityArchivalUri)
	m.True(reflect.DeepEqual(testBinaries, resp6.Namespace.Config.BadBinaries))
	m.Equal(updateClusterActive, resp6.Namespace.ReplicationConfig.ActiveClusterName)
	m.Equal(len(updateClusters), len(resp6.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		m.Equal(updateClusters[index], resp4.Namespace.ReplicationConfig.Clusters[index])
	}
	m.Equal(updateConfigVersion, resp6.Namespace.ConfigVersion)
	m.Equal(updateFailoverVersion, resp6.Namespace.FailoverVersion)
	m.Equal(updateFailoverNotificationVersion, resp6.Namespace.FailoverNotificationVersion)
	m.Equal(notificationVersion, resp6.NotificationVersion)
	m.EqualTimes(time.Unix(0, 0).UTC(), *resp6.Namespace.FailoverEndTime)
}

func (m *MetadataPersistenceSuiteV2) TestRenameNamespace() {
	id := uuid.New()
	name := "rename-namespace-test-name"
	newName := "rename-namespace-test-new-name"
	newNewName := "rename-namespace-test-new-new-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "rename-namespace-test-description"
	owner := "rename-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{clusterActive, clusterStandby}

	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(retention),
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.EqualValues(id, resp1.ID)

	_, err2 := m.GetNamespace(id, "")
	m.NoError(err2)

	err3 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: name,
		NewName:      newName,
	})
	m.NoError(err3)

	resp4, err4 := m.GetNamespace("", newName)
	m.NoError(err4)
	m.NotNil(resp4)
	m.EqualValues(id, resp4.Namespace.Info.Id)
	m.Equal(newName, resp4.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp4.IsGlobalNamespace)

	resp5, err5 := m.GetNamespace(id, "")
	m.NoError(err5)
	m.NotNil(resp5)
	m.EqualValues(id, resp5.Namespace.Info.Id)
	m.Equal(newName, resp5.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp5.IsGlobalNamespace)

	err6 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: newName,
		NewName:      newNewName,
	})
	m.NoError(err6)

	resp6, err6 := m.GetNamespace(id, "")
	m.NoError(err6)
	m.NotNil(resp6)
	m.EqualValues(id, resp6.Namespace.Info.Id)
	m.Equal(newNewName, resp6.Namespace.Info.Name)
	m.Equal(isGlobalNamespace, resp6.IsGlobalNamespace)
}

// TestDeleteNamespace test
func (m *MetadataPersistenceSuiteV2) TestDeleteNamespace() {
	id := uuid.New()
	name := "delete-namespace-test-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "delete-namespace-test-description"
	owner := "delete-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := timestamp.DurationFromDays(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"

	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := true
	clusters := []string{clusterActive, clusterStandby}

	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               retention,
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	m.NoError(err1)
	m.EqualValues(id, resp1.ID)

	resp2, err2 := m.GetNamespace("", name)
	m.NoError(err2)
	m.NotNil(resp2)

	err3 := m.DeleteNamespace("", name)
	m.NoError(err3)

	// May need to loop here to avoid potential inconsistent read-after-write in cassandra
	var err4 error
	var resp4 *p.GetNamespaceResponse
	for i := 0; i < 3; i++ {
		resp4, err4 = m.GetNamespace("", name)
		if err4 != nil {
			break
		}
		time.Sleep(time.Second * time.Duration(i))
	}
	m.Error(err4)
	m.IsType(&serviceerror.NamespaceNotFound{}, err4)
	m.Nil(resp4)

	resp5, err5 := m.GetNamespace(id, "")
	m.Error(err5)
	m.IsType(&serviceerror.NamespaceNotFound{}, err5)
	m.Nil(resp5)

	id = uuid.New()
	resp6, err6 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               retention,
			HistoryArchivalState:    historyArchivalState,
			HistoryArchivalUri:      historyArchivalURI,
			VisibilityArchivalState: visibilityArchivalState,
			VisibilityArchivalUri:   visibilityArchivalURI,
		},
		&persistencespb.NamespaceReplicationConfig{
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

	resp8, err8 := m.GetNamespace("", name)
	m.Error(err8)
	m.IsType(&serviceerror.NamespaceNotFound{}, err8)
	m.Nil(resp8)

	resp9, err9 := m.GetNamespace(id, "")
	m.Error(err9)
	m.IsType(&serviceerror.NamespaceNotFound{}, err9)
	m.Nil(resp9)
}

// TestListNamespaces test
func (m *MetadataPersistenceSuiteV2) TestListNamespaces() {
	clusterActive1 := "some random active cluster name"
	clusterStandby1 := "some random standby cluster name"
	clusters1 := []string{clusterActive1, clusterStandby1}

	clusterActive2 := "other random active cluster name"
	clusterStandby2 := "other random standby cluster name"
	clusters2 := []string{clusterActive2, clusterStandby2}

	testBinaries1 := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:     "test-reason1",
				Operator:   "test-operator1",
				CreateTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}
	testBinaries2 := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"efg": {
				Reason:     "test-reason2",
				Operator:   "test-operator2",
				CreateTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	inputNamespaces := []*p.GetNamespaceResponse{
		{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:          uuid.New(),
					Name:        "list-namespace-test-name-1",
					State:       enumspb.NAMESPACE_STATE_REGISTERED,
					Description: "list-namespace-test-description-1",
					Owner:       "list-namespace-test-owner-1",
					Data:        map[string]string{"k1": "v1"},
				},
				Config: &persistencespb.NamespaceConfig{
					Retention:               timestamp.DurationFromDays(109),
					HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
					HistoryArchivalUri:      "test://history/uri",
					VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
					VisibilityArchivalUri:   "test://visibility/uri",
					BadBinaries:             testBinaries1,
				},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: clusterActive1,
					Clusters:          clusters1,
				},

				ConfigVersion:   133,
				FailoverVersion: 266,
			},
			IsGlobalNamespace: true,
		},
		{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:          uuid.New(),
					Name:        "list-namespace-test-name-2",
					State:       enumspb.NAMESPACE_STATE_REGISTERED,
					Description: "list-namespace-test-description-2",
					Owner:       "list-namespace-test-owner-2",
					Data:        map[string]string{"k1": "v2"},
				},
				Config: &persistencespb.NamespaceConfig{
					Retention:               timestamp.DurationFromDays(326),
					HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
					HistoryArchivalUri:      "",
					VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
					VisibilityArchivalUri:   "",
					BadBinaries:             testBinaries2,
				},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: clusterActive2,
					Clusters:          clusters2,
				},
				ConfigVersion:   400,
				FailoverVersion: 667,
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
	const pageSize = 1
	pageCount := 0
	outputNamespaces := make(map[string]*p.GetNamespaceResponse)
	for {
		resp, err := m.ListNamespaces(pageSize, token)
		m.NoError(err)
		token = resp.NextPageToken
		for _, namespace := range resp.Namespaces {
			outputNamespaces[namespace.Namespace.Info.Id] = namespace
			// global notification version is already tested, so here we make it 0
			// so we can test == easily
			namespace.NotificationVersion = 0
		}
		pageCount++
		if len(token) == 0 {
			break
		}
	}

	// 2 pages with data and 1 empty page which is unavoidable.
	m.Equal(pageCount, 3)
	m.Equal(len(inputNamespaces), len(outputNamespaces))
	for _, namespace := range inputNamespaces {
		m.Equal(namespace, outputNamespaces[namespace.Namespace.Info.Id])
	}
}

func (m *MetadataPersistenceSuiteV2) TestListNamespaces_DeletedNamespace() {
	inputNamespaces := []*p.GetNamespaceResponse{
		{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:    uuid.New(),
					Name:  "list-namespace-test-name-1",
					State: enumspb.NAMESPACE_STATE_REGISTERED,
				},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
		},
		{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:    uuid.New(),
					Name:  "list-namespace-test-name-2",
					State: enumspb.NAMESPACE_STATE_DELETED,
				},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
		},
		{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:    uuid.New(),
					Name:  "list-namespace-test-name-3",
					State: enumspb.NAMESPACE_STATE_REGISTERED,
				},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
		},
		{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:    uuid.New(),
					Name:  "list-namespace-test-name-4",
					State: enumspb.NAMESPACE_STATE_DELETED,
				},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
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
	var listNamespacesPageSize2 []*p.GetNamespaceResponse
	pageCount := 0
	for {
		resp, err := m.ListNamespaces(2, token)
		m.NoError(err)
		token = resp.NextPageToken
		listNamespacesPageSize2 = append(listNamespacesPageSize2, resp.Namespaces...)
		pageCount++
		if len(token) == 0 {
			break
		}
	}

	// 1 page with data and 1 empty page which is unavoidable.
	m.Equal(2, pageCount)
	m.Len(listNamespacesPageSize2, 2)
	for _, namespace := range listNamespacesPageSize2 {
		m.NotEqual(namespace.Namespace.Info.State, enumspb.NAMESPACE_STATE_DELETED)
	}

	pageCount = 0
	var listNamespacesPageSize1 []*p.GetNamespaceResponse
	for {
		resp, err := m.ListNamespaces(1, token)
		m.NoError(err)
		token = resp.NextPageToken
		listNamespacesPageSize1 = append(listNamespacesPageSize1, resp.Namespaces...)
		pageCount++
		if len(token) == 0 {
			break
		}
	}

	// 2 pages with data and 1 empty page which is unavoidable.
	m.Equal(3, pageCount)
	m.Len(listNamespacesPageSize1, 2)
	for _, namespace := range listNamespacesPageSize1 {
		m.NotEqual(namespace.Namespace.Info.State, enumspb.NAMESPACE_STATE_DELETED)
	}
}

// CreateNamespace helper method
func (m *MetadataPersistenceSuiteV2) CreateNamespace(info *persistencespb.NamespaceInfo, config *persistencespb.NamespaceConfig,
	replicationConfig *persistencespb.NamespaceReplicationConfig, isGlobalnamespace bool, configVersion int64, failoverVersion int64) (*p.CreateNamespaceResponse, error) {
	return m.MetadataManager.CreateNamespace(m.ctx, &p.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:              info,
			Config:            config,
			ReplicationConfig: replicationConfig,

			ConfigVersion:   configVersion,
			FailoverVersion: failoverVersion,
		}, IsGlobalNamespace: isGlobalnamespace,
	})
}

// GetNamespace helper method
func (m *MetadataPersistenceSuiteV2) GetNamespace(id string, name string) (*p.GetNamespaceResponse, error) {
	return m.MetadataManager.GetNamespace(m.ctx, &p.GetNamespaceRequest{
		ID:   id,
		Name: name,
	})
}

// UpdateNamespace helper method
func (m *MetadataPersistenceSuiteV2) UpdateNamespace(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	replicationConfig *persistencespb.NamespaceReplicationConfig,
	configVersion int64,
	failoverVersion int64,
	failoverNotificationVersion int64,
	failoverEndTime *time.Time,
	notificationVersion int64,
	isGlobalNamespace bool,
) error {
	return m.MetadataManager.UpdateNamespace(m.ctx, &p.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:                        info,
			Config:                      config,
			ReplicationConfig:           replicationConfig,
			ConfigVersion:               configVersion,
			FailoverVersion:             failoverVersion,
			FailoverEndTime:             failoverEndTime,
			FailoverNotificationVersion: failoverNotificationVersion,
		},
		NotificationVersion: notificationVersion,
		IsGlobalNamespace:   isGlobalNamespace,
	})
}

// DeleteNamespace helper method
func (m *MetadataPersistenceSuiteV2) DeleteNamespace(id string, name string) error {
	if len(id) > 0 {
		return m.MetadataManager.DeleteNamespace(m.ctx, &p.DeleteNamespaceRequest{ID: id})
	}
	return m.MetadataManager.DeleteNamespaceByName(m.ctx, &p.DeleteNamespaceByNameRequest{Name: name})
}

// ListNamespaces helper method
func (m *MetadataPersistenceSuiteV2) ListNamespaces(pageSize int, pageToken []byte) (*p.ListNamespacesResponse, error) {
	return m.MetadataManager.ListNamespaces(m.ctx, &p.ListNamespacesRequest{
		PageSize:      pageSize,
		NextPageToken: pageToken,
	})
}
