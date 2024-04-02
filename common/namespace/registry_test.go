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
		dynamicconfig.GetBoolPropertyFn(false),
		metrics.NoopMetricsHandler,
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

func (s *registrySuite) TestRegisterStateChangeCallback_CatchUp() {
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

	var entriesNotification []*namespace.Namespace
	s.registry.RegisterStateChangeCallback(
		"0",
		func(ns *namespace.Namespace, deletedFromDb bool) {
			s.False(deletedFromDb)
			entriesNotification = append(entriesNotification, ns)
		},
	)

	s.Len(entriesNotification, 2)
	if entriesNotification[0].NotificationVersion() > entriesNotification[1].NotificationVersion() {
		entriesNotification[0], entriesNotification[1] = entriesNotification[1], entriesNotification[0]
	}
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

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       namespace.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1Old, namespaceRecord2Old},
		NextPageToken: nil,
	}, nil)

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
	namespaceNotificationVersion++

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

	// load namespaces
	s.registry.Start()
	defer s.registry.Stop()

	var entries []*namespace.Namespace

	wg := &sync.WaitGroup{}
	wg.Add(2)
	s.registry.RegisterStateChangeCallback(
		"0",
		func(ns *namespace.Namespace, deletedFromDb bool) {
			defer wg.Done()
			s.False(deletedFromDb)
			entries = append(entries, ns)
		},
	)
	wg.Wait()

	s.Len(entries, 2)
	if entries[0].NotificationVersion() > entries[1].NotificationVersion() {
		entries[0], entries[1] = entries[1], entries[0]
	}
	s.Equal([]*namespace.Namespace{entry1Old, entry2Old}, entries)

	wg.Add(1)
	wg.Wait()

	newEntries := entries[2:]

	// entry1 only has descrption update, so won't trigger the state change callback
	s.Len(newEntries, 1)
	s.Equal([]*namespace.Namespace{entry2New}, newEntries)
}

func (s *registrySuite) TestGetTriggerListAndUpdateCache_ConcurrentAccess() {
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

	// load namespaces
	s.registry.Start()
	defer s.registry.Stop()

	// use WaitGroup and callback to wait until refresh loop picks up delete
	wg := &sync.WaitGroup{}
	wg.Add(1)
	s.registry.RegisterStateChangeCallback(
		"1",
		func(ns *namespace.Namespace, deletedFromDb bool) {
			if deletedFromDb {
				wg.Done()
			}
		},
	)
	wg.Wait()

	ns2FromRegistry, err := s.registry.GetNamespace(namespace.Name(namespaceRecord2.Namespace.Info.Name))
	s.NotNil(ns2FromRegistry)
	s.NoError(err)

	// expect readthrough call for missing ns
	s.regPersistence.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: namespaceRecord1.Namespace.Info.Name,
	}).Return(nil, serviceerror.NewNamespaceNotFound(namespaceRecord1.Namespace.Info.Name))

	ns1FromRegistry, err := s.registry.GetNamespace(namespace.Name(namespaceRecord1.Namespace.Info.Name))
	s.Nil(ns1FromRegistry)
	s.Error(err)
	var notFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &notFound)
}

func (s *registrySuite) TestCacheByName() {
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

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{&nsrec},
	}, nil)

	s.registry.Start()
	defer s.registry.Stop()
	ns, err := s.registry.GetNamespace(namespace.Name("foo"))
	s.NoError(err)
	s.Equal(namespace.Name("foo"), ns.Name())
}

func (s *registrySuite) TestGetByNameWithoutReadthrough() {
	// registry start will refresh once
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).Return(&persistence.ListNamespacesResponse{
		Namespaces: nil,
	}, nil)
	// second call will readthrough
	s.regPersistence.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "foo",
	}).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespace.NewID().String(),
				Name: "foo",
			},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		},
	}, nil)

	s.registry.Start()
	defer s.registry.Stop()

	ns, err := s.registry.GetNamespaceWithOptions(namespace.Name("foo"), namespace.GetNamespaceOptions{DisableReadthrough: true})
	var notFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &notFound)

	ns, err = s.registry.GetNamespaceWithOptions(namespace.Name("foo"), namespace.GetNamespaceOptions{DisableReadthrough: false})
	s.NoError(err)
	s.Equal(namespace.Name("foo"), ns.Name())
}

func (s *registrySuite) TestGetByIDWithoutReadthrough() {
	id := namespace.NewID()

	// registry start will refresh once
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).Return(&persistence.ListNamespacesResponse{
		Namespaces: nil,
	}, nil)
	// second call will readthrough
	s.regPersistence.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: id.String(),
	}).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   id.String(),
				Name: "foo",
			},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		},
	}, nil)

	s.registry.Start()
	defer s.registry.Stop()

	ns, err := s.registry.GetNamespaceByIDWithOptions(id, namespace.GetNamespaceOptions{DisableReadthrough: true})
	var notFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &notFound)

	ns, err = s.registry.GetNamespaceByIDWithOptions(id, namespace.GetNamespaceOptions{DisableReadthrough: false})
	s.NoError(err)
	s.Equal(namespace.Name("foo"), ns.Name())
}
