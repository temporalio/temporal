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

package namespace_test

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	registrySuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		regPersistence *namespace.MockPersistence
		registry       namespace.Registry
	}
)

func TestRegistrySuite(t *testing.T) {
	s := new(registrySuite)
	suite.Run(t, s)
}

func (s *registrySuite) SetupSuite() {}

func (s *registrySuite) TearDownSuite() {}

func (s *registrySuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.regPersistence = namespace.NewMockPersistence(s.controller)
	s.registry = namespace.NewRegistry(
		s.regPersistence,
		true,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		metrics.NoopClient,
		log.NewTestLogger())
}

func (s *registrySuite) TearDownTest() {
	s.registry.Stop()
	s.controller.Finish()
}

func (s *registrySuite) TestListNamespace() {
	namespaceNotificationVersion := int64(0)
	namespaceRecord1 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "some random namespace name",
				State: enumspb.NAMESPACE_STATE_REGISTERED,
				Data:  make(map[string]string)},
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
	entry1 := namespace.FromPersistentState(namespaceRecord1)
	namespaceNotificationVersion++

	namespaceRecord2 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED, // Still must be included.
				Data:  make(map[string]string)},
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
	entry2 := namespace.FromPersistentState(namespaceRecord2)
	namespaceNotificationVersion++

	namespaceRecord3 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "yet another random namespace name",
				State: enumspb.NAMESPACE_STATE_DEPRECATED, // Still must be included.
				Data:  make(map[string]string)},
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

	s.regPersistence.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{
			NotificationVersion: namespaceNotificationVersion,
		}, nil)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1},
		NextPageToken: pageToken,
	}, nil)

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  pageToken,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{
			namespaceRecord2,
			namespaceRecord3},
		NextPageToken: nil,
	}, nil)

	// load namespaces
	s.registry.Start()
	defer s.registry.Stop()

	entryByName1, err := s.registry.GetNamespace(namespace.Name(namespaceRecord1.Namespace.Info.Name))
	s.Nil(err)
	s.Equal(entry1, entryByName1)
	entryByID1, err := s.registry.GetNamespaceByID(namespace.ID(namespaceRecord1.Namespace.Info.Id))
	s.Nil(err)
	s.Equal(entry1, entryByID1)

	entryByName2, err := s.registry.GetNamespace(namespace.Name(namespaceRecord2.Namespace.Info.Name))
	s.Nil(err)
	s.Equal(entry2, entryByName2)
	entryByID2, err := s.registry.GetNamespaceByID(namespace.ID(namespaceRecord2.Namespace.Info.Id))
	s.Nil(err)
	s.Equal(entry2, entryByID2)
}

func (s *registrySuite) TestRegisterCallback_CatchUp() {
	namespaceNotificationVersion := int64(0)
	namespaceRecord1 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "some random namespace name",
				Data: make(map[string]string)},
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
	entry1 := namespace.FromPersistentState(namespaceRecord1)
	namespaceNotificationVersion++

	namespaceRecord2 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "another random namespace name",
				Data: make(map[string]string)},
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
	entry2 := namespace.FromPersistentState(namespaceRecord2)
	namespaceNotificationVersion++

	s.regPersistence.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{
			NotificationVersion: namespaceNotificationVersion,
		}, nil)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{
			namespaceRecord1,
			namespaceRecord2},
		NextPageToken: nil,
	}, nil)

	// load namespaces
	s.registry.Start()
	defer s.registry.Stop()

	prepareCallbackInvoked := false
	var entriesNotification []*namespace.Namespace
	// we are not testing catching up, so make this really large
	currentNamespaceNotificationVersion := int64(0)
	s.registry.RegisterNamespaceChangeCallback(
		"0",
		currentNamespaceNotificationVersion,
		func() {
			prepareCallbackInvoked = true
		},
		func(prevNamespaces []*namespace.Namespace, nextNamespaces []*namespace.Namespace) {
			s.Equal(len(prevNamespaces), len(nextNamespaces))
			for index := range prevNamespaces {
				s.Nil(prevNamespaces[index])
			}
			entriesNotification = nextNamespaces
		},
	)

	// the order matters here, should be ordered by notification version
	s.True(prepareCallbackInvoked)
	s.Equal([]*namespace.Namespace{entry1, entry2}, entriesNotification)
}

func (s *registrySuite) TestUpdateCache_TriggerCallBack() {
	namespaceNotificationVersion := int64(0)
	namespaceRecord1Old := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "some random namespace name",
				Data: make(map[string]string)},
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
	entry1Old := namespace.FromPersistentState(namespaceRecord1Old)
	namespaceNotificationVersion++

	namespaceRecord2Old := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "another random namespace name",
				Data: make(map[string]string)},
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
	entry2Old := namespace.FromPersistentState(namespaceRecord2Old)
	namespaceNotificationVersion++

	s.regPersistence.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{
			NotificationVersion: namespaceNotificationVersion,
		}, nil)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1Old, namespaceRecord2Old},
		NextPageToken: nil,
	}, nil)

	// load namespaces
	s.registry.Start()
	defer s.registry.Stop()

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
	entry2New := namespace.FromPersistentState(namespaceRecord2New)
	namespaceNotificationVersion++

	namespaceRecord1New := &persistence.GetNamespaceResponse{ // only the description changed
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          namespaceRecord1Old.Namespace.Info.Id,
				Name:        namespaceRecord1Old.Namespace.Info.Name,
				Description: "updated description", Data: make(map[string]string)},
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
	entry1New := namespace.FromPersistentState(namespaceRecord1New)
	namespaceNotificationVersion++

	prepareCallbackInvoked := false
	var entriesOld []*namespace.Namespace
	var entriesNew []*namespace.Namespace
	// we are not testing catching up, so make this really large
	currentNamespaceNotificationVersion := int64(9999999)
	s.registry.RegisterNamespaceChangeCallback(
		"0",
		currentNamespaceNotificationVersion,
		func() {
			prepareCallbackInvoked = true
		},
		func(prevNamespaces []*namespace.Namespace, nextNamespaces []*namespace.Namespace) {
			entriesOld = prevNamespaces
			entriesNew = nextNamespaces
		},
	)
	s.False(prepareCallbackInvoked)
	s.Empty(entriesOld)
	s.Empty(entriesNew)

	s.regPersistence.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{
			NotificationVersion: namespaceNotificationVersion,
		}, nil)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{
			namespaceRecord1New,
			namespaceRecord2New},
		NextPageToken: nil,
	}, nil)

	s.registry.Refresh()

	// the order matters here: the record 2 got updated first, thus with a lower notification version
	// the record 1 got updated later, thus a higher notification version.
	// making sure notifying from lower to higher version helps the shard to keep track the
	// namespace change events
	s.True(prepareCallbackInvoked)
	s.Equal([]*namespace.Namespace{entry2Old, entry1Old}, entriesOld)
	s.Equal([]*namespace.Namespace{entry2New, entry1New}, entriesNew)
}

func (s *registrySuite) TestGetTriggerListAndUpdateCache_ConcurrentAccess() {
	namespaceNotificationVersion := int64(999999) // make this notification version really large for test
	s.regPersistence.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{
			NotificationVersion: namespaceNotificationVersion,
		}, nil)
	id := namespace.NewID()
	namespaceRecordOld := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: id.String(), Name: "some random namespace name", Data: make(map[string]string)},
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
	entryOld := namespace.FromPersistentState(namespaceRecordOld)

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecordOld},
		NextPageToken: nil,
	}, nil)

	// load namespaces
	s.registry.Start()
	defer s.registry.Stop()

	coroutineCountGet := 1000
	waitGroup := &sync.WaitGroup{}
	startChan := make(chan struct{})
	testGetFn := func() {
		<-startChan
		entryNew, err := s.registry.GetNamespaceByID(id)
		switch err.(type) {
		case nil:
			s.Equal(entryOld, entryNew)
			waitGroup.Done()
		case *serviceerror.NamespaceNotFound:
			time.Sleep(4 * time.Second)
			entryNew, err := s.registry.GetNamespaceByID(id)
			s.NoError(err)
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

func (s *registrySuite) TestRemoveDeletedNamespace() {
	namespaceNotificationVersion := int64(0)
	namespaceRecord1 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "some random namespace name",
				Data: make(map[string]string)},
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
	namespaceNotificationVersion++

	namespaceRecord2 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "another random namespace name",
				Data: make(map[string]string)},
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
	namespaceNotificationVersion++

	s.regPersistence.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{
			NotificationVersion: namespaceNotificationVersion,
		}, nil)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{
			namespaceRecord1,
			namespaceRecord2},
		NextPageToken: nil,
	}, nil)

	// load namespaces
	s.registry.Start()
	defer s.registry.Stop()

	s.regPersistence.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{
			NotificationVersion: namespaceNotificationVersion,
		}, nil)
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{
			// namespaceRecord1 is removed
			namespaceRecord2},
		NextPageToken: nil,
	}, nil)

	s.registry.Refresh()

	ns2FromRegistry, err := s.registry.GetNamespace(namespace.Name(namespaceRecord2.Namespace.Info.Name))
	s.NotNil(ns2FromRegistry)
	s.NoError(err)

	ns1FromRegistry, err := s.registry.GetNamespace(namespace.Name(namespaceRecord1.Namespace.Info.Name))
	s.Nil(ns1FromRegistry)
	s.Error(err)
	var notFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &notFound)
}

func TestCacheByName(t *testing.T) {
	nsrec := persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "foo",
			},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		},
	}
	regPersist := persistence.NewMockMetadataManager(gomock.NewController(t))
	regPersist.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{NotificationVersion: nsrec.NotificationVersion + 1}, nil)
	regPersist.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{&nsrec},
	}, nil)
	reg := namespace.NewRegistry(
		regPersist, false, dynamicconfig.GetDurationPropertyFn(time.Second), metrics.NoopClient, log.NewNoopLogger())
	reg.Start()
	defer reg.Stop()
	ns, err := reg.GetNamespace(namespace.Name("foo"))
	require.NoError(t, err)
	require.Equal(t, namespace.Name("foo"), ns.Name())
}
