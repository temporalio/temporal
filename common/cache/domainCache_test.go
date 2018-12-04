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

package cache

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	domainCacheSuite struct {
		suite.Suite

		logger          bark.Logger
		clusterMetadata *mocks.ClusterMetadata
		metadataMgr     *mocks.MetadataManager
		domainCache     *domainCache
	}
)

func TestDomainCacheSuite(t *testing.T) {
	s := new(domainCacheSuite)
	suite.Run(t, s)
}

func (s *domainCacheSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *domainCacheSuite) TearDownSuite() {

}

func (s *domainCacheSuite) SetupTest() {
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.clusterMetadata = &mocks.ClusterMetadata{}
	s.metadataMgr = &mocks.MetadataManager{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.domainCache = NewDomainCache(s.metadataMgr, s.clusterMetadata, metricsClient, s.logger).(*domainCache)
}

func (s *domainCacheSuite) TearDownTest() {
	s.domainCache.Stop()
	s.clusterMetadata.AssertExpectations(s.T())
	s.metadataMgr.AssertExpectations(s.T())
}

func (s *domainCacheSuite) TestListDomain() {
	domainNotificationVersion := int64(0)
	domainRecord1 := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "some random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 1},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		FailoverNotificationVersion: 0,
		NotificationVersion:         domainNotificationVersion,
	}
	entry1 := s.buildEntryFromRecord(domainRecord1)
	domainNotificationVersion++

	domainRecord2 := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "another random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 2},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		FailoverNotificationVersion: 0,
		NotificationVersion:         domainNotificationVersion,
	}
	entry2 := s.buildEntryFromRecord(domainRecord2)
	domainNotificationVersion++

	domainRecord3 := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "yet another random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 3},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		FailoverNotificationVersion: 0,
		NotificationVersion:         domainNotificationVersion,
	}
	// there is no domainNotificationVersion++ here
	// this is to test that if new domain change event happen during the pagination,
	// new change will not be loaded to domain cache

	pageToken := []byte("some random page token")

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: domainNotificationVersion}, nil)
	s.clusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.metadataMgr.On("ListDomains", &persistence.ListDomainsRequest{
		PageSize:      domainCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListDomainsResponse{
		Domains:       []*persistence.GetDomainResponse{domainRecord1},
		NextPageToken: pageToken,
	}, nil).Once()

	s.metadataMgr.On("ListDomains", &persistence.ListDomainsRequest{
		PageSize:      domainCacheRefreshPageSize,
		NextPageToken: pageToken,
	}).Return(&persistence.ListDomainsResponse{
		Domains:       []*persistence.GetDomainResponse{domainRecord2, domainRecord3},
		NextPageToken: nil,
	}, nil).Once()

	// load domains
	s.domainCache.Start()
	defer s.domainCache.Stop()

	entryByName1, err := s.domainCache.GetDomain(domainRecord1.Info.Name)
	s.Nil(err)
	s.Equal(entry1, s.clearExpiry(entryByName1))
	entryByID1, err := s.domainCache.GetDomainByID(domainRecord1.Info.ID)
	s.Nil(err)
	s.Equal(entry1, s.clearExpiry(entryByID1))

	entryByName2, err := s.domainCache.GetDomain(domainRecord2.Info.Name)
	s.Nil(err)
	s.Equal(entry2, s.clearExpiry(entryByName2))
	entryByID2, err := s.domainCache.GetDomainByID(domainRecord2.Info.ID)
	s.Nil(err)
	s.Equal(entry2, s.clearExpiry(entryByID2))

	allDomains := s.domainCache.GetAllDomain()
	for _, domain := range allDomains {
		s.clearExpiry(domain)
	}
	s.Equal(map[string]*DomainCacheEntry{
		entry1.GetInfo().ID: entry1,
		entry2.GetInfo().ID: entry2,
	}, allDomains)
}

func (s *domainCacheSuite) TestGetDomain_NonLoaded_GetByName() {
	s.clusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "some random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 1},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		TableVersion: persistence.DomainTableVersionV1,
	}
	entry := s.buildEntryFromRecord(domainRecord)

	s.metadataMgr.On("GetDomain", &persistence.GetDomainRequest{Name: entry.info.Name}).Return(domainRecord, nil).Once()

	entryByName, err := s.domainCache.GetDomain(domainRecord.Info.Name)
	s.Nil(err)
	s.Equal(entry, s.clearExpiry(entryByName))
	entryByName, err = s.domainCache.GetDomain(domainRecord.Info.Name)
	s.Nil(err)
	s.Equal(entry, s.clearExpiry(entryByName))
}

func (s *domainCacheSuite) TestGetDomain_NonLoaded_GetByID() {
	s.clusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "some random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 1},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		TableVersion: persistence.DomainTableVersionV1,
	}
	entry := s.buildEntryFromRecord(domainRecord)

	s.metadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: entry.info.ID}).Return(domainRecord, nil).Once()

	entryByID, err := s.domainCache.GetDomainByID(domainRecord.Info.ID)
	s.Nil(err)
	s.Equal(entry, s.clearExpiry(entryByID))
	entryByID, err = s.domainCache.GetDomainByID(domainRecord.Info.ID)
	s.Nil(err)
	s.Equal(entry, s.clearExpiry(entryByID))
}

func (s *domainCacheSuite) TestRegisterCallback_CatchUp() {
	domainNotificationVersion := int64(0)
	domainRecord1 := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "some random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 1},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion:               10,
		FailoverVersion:             11,
		FailoverNotificationVersion: 0,
		NotificationVersion:         domainNotificationVersion,
	}
	entry1 := s.buildEntryFromRecord(domainRecord1)
	domainNotificationVersion++

	domainRecord2 := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "another random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 2},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion:               20,
		FailoverVersion:             21,
		FailoverNotificationVersion: 0,
		NotificationVersion:         domainNotificationVersion,
	}
	entry2 := s.buildEntryFromRecord(domainRecord2)
	domainNotificationVersion++

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: domainNotificationVersion}, nil).Once()
	s.clusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.metadataMgr.On("ListDomains", &persistence.ListDomainsRequest{
		PageSize:      domainCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListDomainsResponse{
		Domains:       []*persistence.GetDomainResponse{domainRecord1, domainRecord2},
		NextPageToken: nil,
	}, nil).Once()

	// load domains
	s.Nil(s.domainCache.refreshDomains())

	prepareCallbacckInvoked := false
	entriesNotification := []*DomainCacheEntry{}
	// we are not testing catching up, so make this really large
	currentDomainNotificationVersion := int64(0)
	s.domainCache.RegisterDomainChangeCallback(
		0,
		currentDomainNotificationVersion,
		func() {
			prepareCallbacckInvoked = true
		},
		func(prevDomains []*DomainCacheEntry, nextDomains []*DomainCacheEntry) {
			s.Equal(len(prevDomains), len(nextDomains))
			for index := range prevDomains {
				s.Nil(prevDomains[index])
			}
			entriesNotification = nextDomains
		},
	)

	// the order matters here, should be ordered by notification version
	s.True(prepareCallbacckInvoked)
	for _, domain := range entriesNotification {
		s.clearExpiry(domain)
	}
	s.Equal([]*DomainCacheEntry{entry1, entry2}, entriesNotification)
}

func (s *domainCacheSuite) TestUpdateCache_ListTrigger() {
	domainNotificationVersion := int64(0)
	domainRecord1Old := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "some random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 1},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion:               10,
		FailoverVersion:             11,
		FailoverNotificationVersion: 0,
		NotificationVersion:         domainNotificationVersion,
	}
	entry1Old := s.buildEntryFromRecord(domainRecord1Old)
	domainNotificationVersion++

	domainRecord2Old := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "another random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 2},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion:               20,
		FailoverVersion:             21,
		FailoverNotificationVersion: 0,
		NotificationVersion:         domainNotificationVersion,
	}
	entry2Old := s.buildEntryFromRecord(domainRecord2Old)
	domainNotificationVersion++

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: domainNotificationVersion}, nil).Once()
	s.clusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.metadataMgr.On("ListDomains", &persistence.ListDomainsRequest{
		PageSize:      domainCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListDomainsResponse{
		Domains:       []*persistence.GetDomainResponse{domainRecord1Old, domainRecord2Old},
		NextPageToken: nil,
	}, nil).Once()

	// load domains
	s.Nil(s.domainCache.refreshDomains())

	domainRecord2New := &persistence.GetDomainResponse{
		Info:   &*domainRecord2Old.Info,
		Config: &*domainRecord2Old.Config,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName, // only this changed
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion:               domainRecord2Old.ConfigVersion,
		FailoverVersion:             domainRecord2Old.FailoverVersion + 1,
		FailoverNotificationVersion: domainNotificationVersion,
		NotificationVersion:         domainNotificationVersion,
	}
	entry2New := s.buildEntryFromRecord(domainRecord2New)
	domainNotificationVersion++

	domainRecord1New := &persistence.GetDomainResponse{ // only the description changed
		Info:   &persistence.DomainInfo{ID: domainRecord1Old.Info.ID, Name: domainRecord1Old.Info.Name, Description: "updated description", Data: make(map[string]string)},
		Config: &*domainRecord2Old.Config,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion:               domainRecord1Old.ConfigVersion + 1,
		FailoverVersion:             domainRecord1Old.FailoverVersion,
		FailoverNotificationVersion: domainRecord1Old.FailoverNotificationVersion,
		NotificationVersion:         domainNotificationVersion,
	}
	entry1New := s.buildEntryFromRecord(domainRecord1New)
	domainNotificationVersion++

	prepareCallbacckInvoked := false
	entriesOld := []*DomainCacheEntry{}
	entriesNew := []*DomainCacheEntry{}
	// we are not testing catching up, so make this really large
	currentDomainNotificationVersion := int64(9999999)
	s.domainCache.RegisterDomainChangeCallback(
		0,
		currentDomainNotificationVersion,
		func() {
			prepareCallbacckInvoked = true
		},
		func(prevDomains []*DomainCacheEntry, nextDomains []*DomainCacheEntry) {
			entriesOld = prevDomains
			entriesNew = nextDomains
		},
	)
	s.False(prepareCallbacckInvoked)
	s.Empty(entriesOld)
	s.Empty(entriesNew)

	s.metadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{NotificationVersion: domainNotificationVersion}, nil).Once()
	s.metadataMgr.On("ListDomains", &persistence.ListDomainsRequest{
		PageSize:      domainCacheRefreshPageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListDomainsResponse{
		Domains:       []*persistence.GetDomainResponse{domainRecord1New, domainRecord2New},
		NextPageToken: nil,
	}, nil).Once()
	s.Nil(s.domainCache.refreshDomains())

	// the order matters here: the record 2 got updated first, thus with a lower notification version
	// the record 1 got updated later, thus a higher notification version.
	// making sure notifying from lower to higher version helps the shard to keep track the
	// domain change events
	s.True(prepareCallbacckInvoked)
	for _, domain := range entriesOld {
		s.clearExpiry(domain)
	}
	for _, domain := range entriesNew {
		s.clearExpiry(domain)
	}
	s.Equal([]*DomainCacheEntry{entry2Old, entry1Old}, entriesOld)
	s.Equal([]*DomainCacheEntry{entry2New, entry1New}, entriesNew)
}

func (s *domainCacheSuite) TestUpdateCache_GetNotTrigger() {
	s.clusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	domainRecordOld := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: uuid.New(), Name: "some random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 1},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		TableVersion: persistence.DomainTableVersionV1,
	}
	entryOld := s.buildEntryFromRecord(domainRecordOld)

	domainRecordNew := &persistence.GetDomainResponse{
		Info:   entryOld.info,
		Config: &persistence.DomainConfig{Retention: 2},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion: domainRecordOld.ConfigVersion + 1,
		TableVersion:  persistence.DomainTableVersionV1,
	}
	entryNew := s.buildEntryFromRecord(domainRecordNew)

	s.metadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: entryOld.info.ID}).Return(domainRecordOld, nil).Once()
	entry, err := s.domainCache.GetDomainByID(entryOld.info.ID)
	s.Nil(err)
	s.Equal(entryOld, s.clearExpiry(entry))

	prepareCallbackInvoked := false
	callbackInvoked := false
	// we are not testing catching up, so make this really large
	currentDomainNotificationVersion := int64(9999999)
	s.domainCache.RegisterDomainChangeCallback(
		0,
		currentDomainNotificationVersion,
		func() {
			prepareCallbackInvoked = true
		},
		func(prevDomains []*DomainCacheEntry, nextDomains []*DomainCacheEntry) {
			callbackInvoked = true
		},
	)

	prevEntry, nextEntry, err := s.domainCache.updateIDToDomainCache(s.domainCache.cacheByID.Load().(Cache), domainRecordNew.Info.ID, entryNew)
	s.Nil(err)
	s.Nil(prevEntry)
	s.Equal(entryNew, s.clearExpiry(nextEntry))
	s.False(prepareCallbackInvoked)
	s.False(callbackInvoked)
}

func (s *domainCacheSuite) TestGetUpdateCache_ConcurrentAccess() {
	s.clusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	id := uuid.New()
	domainRecordOld := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: id, Name: "some random domain name", Data: make(map[string]string)},
		Config: &persistence.DomainConfig{Retention: 1},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		ConfigVersion:   0,
		FailoverVersion: 0,
		TableVersion:    persistence.DomainTableVersionV1,
	}
	entryOld := s.buildEntryFromRecord(domainRecordOld)

	s.metadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: id}).Return(domainRecordOld, nil).Once()
	s.domainCache.GetDomainByID(id)

	coroutineCountGet := 100
	coroutineCountUpdate := 100
	waitGroup := &sync.WaitGroup{}
	stopChan := make(chan struct{})
	testGetFn := func() {
		<-stopChan
		entryNew, err := s.domainCache.GetDomainByID(id)
		s.Nil(err)
		// make the config version the same so we can easily compare those
		entryNew.configVersion = 0
		entryNew.failoverVersion = 0
		s.Equal(entryOld, s.clearExpiry(entryNew))
		waitGroup.Done()
	}

	testUpdateFn := func() {
		<-stopChan
		entryNew, err := s.domainCache.GetDomainByID(id)
		s.Nil(err)
		entryNew.configVersion = entryNew.configVersion + 1
		entryNew.failoverVersion = entryNew.failoverVersion + 1
		s.domainCache.updateIDToDomainCache(s.domainCache.cacheNameToID.Load().(Cache), id, entryNew)
		waitGroup.Done()
	}

	for i := 0; i < coroutineCountGet; i++ {
		waitGroup.Add(1)
		go testGetFn()
	}
	for i := 0; i < coroutineCountUpdate; i++ {
		waitGroup.Add(1)
		go testUpdateFn()
	}
	close(stopChan)
	waitGroup.Wait()
}

func (s *domainCacheSuite) buildEntryFromRecord(record *persistence.GetDomainResponse) *DomainCacheEntry {
	newEntry := newDomainCacheEntry(s.clusterMetadata)
	newEntry.info = &*record.Info
	newEntry.config = &*record.Config
	newEntry.replicationConfig = &persistence.DomainReplicationConfig{
		ActiveClusterName: record.ReplicationConfig.ActiveClusterName,
	}
	for _, cluster := range record.ReplicationConfig.Clusters {
		newEntry.replicationConfig.Clusters = append(newEntry.replicationConfig.Clusters, &*cluster)
	}
	newEntry.configVersion = record.ConfigVersion
	newEntry.failoverVersion = record.FailoverVersion
	newEntry.isGlobalDomain = record.IsGlobalDomain
	newEntry.failoverNotificationVersion = record.FailoverNotificationVersion
	newEntry.notificationVersion = record.NotificationVersion
	return newEntry
}

func (s *domainCacheSuite) clearExpiry(entry *DomainCacheEntry) *DomainCacheEntry {
	entry.expiry = time.Time{}
	return entry
}

func Test_GetRetentionDays(t *testing.T) {
	d := &DomainCacheEntry{
		info: &persistence.DomainInfo{
			Data: make(map[string]string),
		},
		config: &persistence.DomainConfig{
			Retention: 7,
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
	d := &DomainCacheEntry{
		info: &persistence.DomainInfo{
			Data: make(map[string]string),
		},
		config: &persistence.DomainConfig{
			Retention: 7,
		},
	}
	wid := uuid.New()
	require.False(t, d.IsSampledForLongerRetentionEnabled(wid))
	d.info.Data[SampleRetentionKey] = "30"
	d.info.Data[SampleRateKey] = "0"
	require.True(t, d.IsSampledForLongerRetentionEnabled(wid))
}

func Test_IsSampledForLongerRetention(t *testing.T) {
	d := &DomainCacheEntry{
		info: &persistence.DomainInfo{
			Data: make(map[string]string),
		},
		config: &persistence.DomainConfig{
			Retention: 7,
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
