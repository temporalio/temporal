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

package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	namespaceCacheSuite struct {
		suite.Suite
		*require.Assertions

		logger          log.Logger
		clusterMetadata *mocks.ClusterMetadata
		metadataMgr     *mocks.MetadataManager
		namespaceCache  *namespaceCache
	}
)

func TestNamespaceCacheSuite(t *testing.T) {
	s := new(namespaceCacheSuite)
	suite.Run(t, s)
}

func (s *namespaceCacheSuite) SetupSuite() {
}

func (s *namespaceCacheSuite) TearDownSuite() {

}

func (s *namespaceCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.clusterMetadata = &mocks.ClusterMetadata{}
	s.metadataMgr = &mocks.MetadataManager{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.namespaceCache = NewNamespaceCache(s.metadataMgr, s.clusterMetadata, metricsClient, s.logger).(*namespaceCache)
}

func (s *namespaceCacheSuite) TearDownTest() {
	s.namespaceCache.Stop()
	s.clusterMetadata.AssertExpectations(s.T())
	s.metadataMgr.AssertExpectations(s.T())
}

func (s *namespaceCacheSuite) TestListNamespace() {
	namespaceNotificationVersion := int64(0)
	namespaceRecord1 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: uuid.New(), Name: "some random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry1 := s.buildEntryFromRecord(namespaceRecord1)
	namespaceNotificationVersion++

	namespaceRecord2 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: uuid.New(), Name: "another random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry2 := s.buildEntryFromRecord(namespaceRecord2)
	namespaceNotificationVersion++

	namespaceRecord3 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: uuid.New(), Name: "yet another random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(3),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	// there is no namespaceNotificationVersion++ here
	// this is to test that if new namespace change event happen during the pagination,
	// new change will not be loaded to namespace cache

	pageToken := []byte("some random page token")

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: namespaceNotificationVersion}, nil)
	s.clusterMetadata.On("IsGlobalNamespaceEnabled").Return(true)
	s.metadataMgr.On("ListNamespaces", &persistence.ListNamespacesRequest{
		PageSize:      namespaceCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1},
		NextPageToken: pageToken,
	}, nil).Once()

	s.metadataMgr.On("ListNamespaces", &persistence.ListNamespacesRequest{
		PageSize:      namespaceCacheRefreshPageSize,
		NextPageToken: pageToken,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord2, namespaceRecord3},
		NextPageToken: nil,
	}, nil).Once()

	// load namespaces
	s.namespaceCache.Start()
	defer s.namespaceCache.Stop()

	entryByName1, err := s.namespaceCache.GetNamespace(namespaceRecord1.Namespace.Info.Name)
	s.Nil(err)
	s.Equal(entry1, entryByName1)
	entryByID1, err := s.namespaceCache.GetNamespaceByID(namespaceRecord1.Namespace.Info.Id)
	s.Nil(err)
	s.Equal(entry1, entryByID1)

	entryByName2, err := s.namespaceCache.GetNamespace(namespaceRecord2.Namespace.Info.Name)
	s.Nil(err)
	s.Equal(entry2, entryByName2)
	entryByID2, err := s.namespaceCache.GetNamespaceByID(namespaceRecord2.Namespace.Info.Id)
	s.Nil(err)
	s.Equal(entry2, entryByID2)

	allNamespaces := s.namespaceCache.GetAllNamespace()
	s.Equal(map[string]*NamespaceCacheEntry{
		entry1.GetInfo().Id: entry1,
		entry2.GetInfo().Id: entry2,
	}, allNamespaces)
}

func (s *namespaceCacheSuite) TestRegisterCallback_CatchUp() {
	namespaceNotificationVersion := int64(0)
	namespaceRecord1 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: uuid.New(), Name: "some random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			ConfigVersion:               10,
			FailoverVersion:             11,
			FailoverNotificationVersion: 0,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry1 := s.buildEntryFromRecord(namespaceRecord1)
	namespaceNotificationVersion++

	namespaceRecord2 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: uuid.New(), Name: "another random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			ConfigVersion:               20,
			FailoverVersion:             21,
			FailoverNotificationVersion: 0,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry2 := s.buildEntryFromRecord(namespaceRecord2)
	namespaceNotificationVersion++

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: namespaceNotificationVersion}, nil).Once()
	s.clusterMetadata.On("IsGlobalNamespaceEnabled").Return(true)
	s.metadataMgr.On("ListNamespaces", &persistence.ListNamespacesRequest{
		PageSize:      namespaceCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1, namespaceRecord2},
		NextPageToken: nil,
	}, nil).Once()

	// load namespaces
	s.Nil(s.namespaceCache.refreshNamespaces())

	prepareCallbacckInvoked := false
	var entriesNotification []*NamespaceCacheEntry
	// we are not testing catching up, so make this really large
	currentNamespaceNotificationVersion := int64(0)
	s.namespaceCache.RegisterNamespaceChangeCallback(
		0,
		currentNamespaceNotificationVersion,
		func() {
			prepareCallbacckInvoked = true
		},
		func(prevNamespaces []*NamespaceCacheEntry, nextNamespaces []*NamespaceCacheEntry) {
			s.Equal(len(prevNamespaces), len(nextNamespaces))
			for index := range prevNamespaces {
				s.Nil(prevNamespaces[index])
			}
			entriesNotification = nextNamespaces
		},
	)

	// the order matters here, should be ordered by notification version
	s.True(prepareCallbacckInvoked)
	s.Equal([]*NamespaceCacheEntry{entry1, entry2}, entriesNotification)
}

func (s *namespaceCacheSuite) TestUpdateCache_TriggerCallBack() {
	namespaceNotificationVersion := int64(0)
	namespaceRecord1Old := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: uuid.New(), Name: "some random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			ConfigVersion:               10,
			FailoverVersion:             11,
			FailoverNotificationVersion: 0,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry1Old := s.buildEntryFromRecord(namespaceRecord1Old)
	namespaceNotificationVersion++

	namespaceRecord2Old := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: uuid.New(), Name: "another random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			ConfigVersion:               20,
			FailoverVersion:             21,
			FailoverNotificationVersion: 0,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry2Old := s.buildEntryFromRecord(namespaceRecord2Old)
	namespaceNotificationVersion++

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: namespaceNotificationVersion}, nil).Once()
	s.clusterMetadata.On("IsGlobalNamespaceEnabled").Return(true)
	s.metadataMgr.On("ListNamespaces", &persistence.ListNamespacesRequest{
		PageSize:      namespaceCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1Old, namespaceRecord2Old},
		NextPageToken: nil,
	}, nil).Once()

	// load namespaces
	s.Nil(s.namespaceCache.refreshNamespaces())

	namespaceRecord2New := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info:   namespaceRecord2Old.Namespace.Info,
			Config: namespaceRecord2Old.Namespace.Config,
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName, // only this changed
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			ConfigVersion:               namespaceRecord2Old.Namespace.ConfigVersion,
			FailoverVersion:             namespaceRecord2Old.Namespace.FailoverVersion + 1,
			FailoverNotificationVersion: namespaceNotificationVersion,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry2New := s.buildEntryFromRecord(namespaceRecord2New)
	namespaceNotificationVersion++

	namespaceRecord1New := &persistence.GetNamespaceResponse{ // only the description changed
		Namespace: &persistencespb.NamespaceDetail{
			Info:   &persistencespb.NamespaceInfo{Id: namespaceRecord1Old.Namespace.Info.Id, Name: namespaceRecord1Old.Namespace.Info.Name, Description: "updated description", Data: make(map[string]string)},
			Config: namespaceRecord2Old.Namespace.Config,
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			ConfigVersion:               namespaceRecord1Old.Namespace.ConfigVersion + 1,
			FailoverVersion:             namespaceRecord1Old.Namespace.FailoverVersion,
			FailoverNotificationVersion: namespaceRecord1Old.Namespace.FailoverNotificationVersion,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	entry1New := s.buildEntryFromRecord(namespaceRecord1New)
	namespaceNotificationVersion++

	prepareCallbacckInvoked := false
	var entriesOld []*NamespaceCacheEntry
	var entriesNew []*NamespaceCacheEntry
	// we are not testing catching up, so make this really large
	currentNamespaceNotificationVersion := int64(9999999)
	s.namespaceCache.RegisterNamespaceChangeCallback(
		0,
		currentNamespaceNotificationVersion,
		func() {
			prepareCallbacckInvoked = true
		},
		func(prevNamespaces []*NamespaceCacheEntry, nextNamespaces []*NamespaceCacheEntry) {
			entriesOld = prevNamespaces
			entriesNew = nextNamespaces
		},
	)
	s.False(prepareCallbacckInvoked)
	s.Empty(entriesOld)
	s.Empty(entriesNew)

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: namespaceNotificationVersion}, nil).Once()
	s.metadataMgr.On("ListNamespaces", &persistence.ListNamespacesRequest{
		PageSize:      namespaceCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1New, namespaceRecord2New},
		NextPageToken: nil,
	}, nil).Once()
	s.Nil(s.namespaceCache.refreshNamespaces())

	// the order matters here: the record 2 got updated first, thus with a lower notification version
	// the record 1 got updated later, thus a higher notification version.
	// making sure notifying from lower to higher version helps the shard to keep track the
	// namespace change events
	s.True(prepareCallbacckInvoked)
	s.Equal([]*NamespaceCacheEntry{entry2Old, entry1Old}, entriesOld)
	s.Equal([]*NamespaceCacheEntry{entry2New, entry1New}, entriesNew)
}

func (s *namespaceCacheSuite) TestGetTriggerListAndUpdateCache_ConcurrentAccess() {
	s.clusterMetadata.On("IsGlobalNamespaceEnabled").Return(true)
	namespaceNotificationVersion := int64(999999) // make this notification version really large for test
	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: namespaceNotificationVersion}, nil)
	id := uuid.NewRandom().String()
	namespaceRecordOld := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: id, Name: "some random namespace name", Data: make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			ConfigVersion:   0,
			FailoverVersion: 0,
		},
	}
	entryOld := s.buildEntryFromRecord(namespaceRecordOld)

	s.metadataMgr.On("ListNamespaces", &persistence.ListNamespacesRequest{
		PageSize:      namespaceCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecordOld},
		NextPageToken: nil,
	}, nil).Once()

	// load namespaces
	s.namespaceCache.Start()
	defer s.namespaceCache.Stop()

	coroutineCountGet := 1000
	waitGroup := &sync.WaitGroup{}
	startChan := make(chan struct{})
	testGetFn := func() {
		<-startChan
		entryNew, err := s.namespaceCache.GetNamespaceByID(id)
		switch err.(type) {
		case nil:
			// make the config version the same so we can easily compare those
			entryNew.configVersion = 0
			entryNew.failoverVersion = 0
			s.Equal(entryOld, entryNew)
			waitGroup.Done()
		case *serviceerror.NotFound:
			time.Sleep(2 * NamespaceCacheMinRefreshInterval)
			entryNew, err := s.namespaceCache.GetNamespaceByID(id)
			s.NoError(err)
			// make the config version the same so we can easily compare those
			entryNew.configVersion = 0
			entryNew.failoverVersion = 0
			s.Equal(entryOld, entryNew)
			waitGroup.Done()
		default:
			s.NoError(err)
			waitGroup.Done()
		}
	}

	for i := 0; i < coroutineCountGet; i++ {
		waitGroup.Add(1)
		go testGetFn()
	}
	close(startChan)
	waitGroup.Wait()
}

func (s *namespaceCacheSuite) buildEntryFromRecord(record *persistence.GetNamespaceResponse) *NamespaceCacheEntry {
	newEntry := newNamespaceCacheEntry(s.clusterMetadata)
	newEntry.info = record.Namespace.Info
	newEntry.config = record.Namespace.Config
	newEntry.replicationConfig = &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: record.Namespace.ReplicationConfig.ActiveClusterName,
	}
	for _, cluster := range record.Namespace.ReplicationConfig.Clusters {
		newEntry.replicationConfig.Clusters = append(newEntry.replicationConfig.Clusters, cluster)
	}
	newEntry.configVersion = record.Namespace.ConfigVersion
	newEntry.failoverVersion = record.Namespace.FailoverVersion
	newEntry.isGlobalNamespace = record.IsGlobalNamespace
	newEntry.failoverNotificationVersion = record.Namespace.FailoverNotificationVersion
	newEntry.notificationVersion = record.NotificationVersion
	newEntry.initialized = true
	return newEntry
}

func Test_GetRetentionDays(t *testing.T) {
	d := &NamespaceCacheEntry{
		info: &persistencespb.NamespaceInfo{
			Data: make(map[string]string),
		},
		config: &persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(7),
		},
	}
	d.info.Data[SampleRetentionKey] = "30"
	d.info.Data[SampleRateKey] = "0"

	wid := uuid.New()
	rd := d.GetRetentionDays(wid)
	require.Equal(t, int32(7), rd)

	d.info.Data[SampleRateKey] = "1"
	rd = d.GetRetentionDays(wid)
	require.Equal(t, int32(30), rd)

	d.info.Data[SampleRetentionKey] = "invalid-value"
	rd = d.GetRetentionDays(wid)
	require.Equal(t, int32(7), rd) // fallback to normal retention

	d.info.Data[SampleRetentionKey] = "30"
	d.info.Data[SampleRateKey] = "invalid-value"
	rd = d.GetRetentionDays(wid)
	require.Equal(t, int32(7), rd) // fallback to normal retention

	wid = "3aef42a8-db0a-4a3b-b8b7-9829d74b4ebf"
	d.info.Data[SampleRetentionKey] = "30"
	d.info.Data[SampleRateKey] = "0.8"
	rd = d.GetRetentionDays(wid)
	require.Equal(t, int32(7), rd) // fallback to normal retention
	d.info.Data[SampleRateKey] = "0.9"
	rd = d.GetRetentionDays(wid)
	require.Equal(t, int32(30), rd)
}

func Test_IsSampledForLongerRetentionEnabled(t *testing.T) {
	d := &NamespaceCacheEntry{
		info: &persistencespb.NamespaceInfo{
			Data: make(map[string]string),
		},
		config: &persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(7),
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{},
			},
		},
	}
	wid := uuid.New()
	require.False(t, d.IsSampledForLongerRetentionEnabled(wid))
	d.info.Data[SampleRetentionKey] = "30"
	d.info.Data[SampleRateKey] = "0"
	require.True(t, d.IsSampledForLongerRetentionEnabled(wid))
}

func Test_IsSampledForLongerRetention(t *testing.T) {
	d := &NamespaceCacheEntry{
		info: &persistencespb.NamespaceInfo{
			Data: make(map[string]string),
		},
		config: &persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(7),
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{},
			},
		},
	}
	wid := uuid.New()
	require.False(t, d.IsSampledForLongerRetention(wid))

	d.info.Data[SampleRetentionKey] = "30"
	d.info.Data[SampleRateKey] = "0"
	require.False(t, d.IsSampledForLongerRetention(wid))

	d.info.Data[SampleRateKey] = "1"
	require.True(t, d.IsSampledForLongerRetention(wid))

	d.info.Data[SampleRateKey] = "invalid-value"
	require.False(t, d.IsSampledForLongerRetention(wid))
}

func Test_NamespaceCacheEntry_GetNamespaceNotActiveErr(t *testing.T) {
	clusterMetadata := cluster.NewMetadata(
		loggerimpl.NewNopLogger(),
		dynamicconfig.GetBoolPropertyFn(true),
		int64(10),
		cluster.TestCurrentClusterName,
		cluster.TestCurrentClusterName,
		cluster.TestAllClusterInfo,
		&config.ReplicationConsumerConfig{
			Type: config.ReplicationConsumerTypeRPC,
		},
	)
	namespaceEntry := NewGlobalNamespaceCacheEntryForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
		clusterMetadata,
	)

	require.Nil(t, namespaceEntry.GetNamespaceNotActiveErr())

	// update to become not active
	namespaceEntry.replicationConfig.ActiveClusterName = cluster.TestAlternativeClusterName
	err := namespaceEntry.GetNamespaceNotActiveErr()
	require.NotNil(t, err)
	_, ok := err.(*serviceerror.NamespaceNotActive)
	require.True(t, ok)
}
