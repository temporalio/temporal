package nsregistry_test

import (
	"context"
	"sync"
	"testing"
	"time"

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
	"go.temporal.io/server/common/namespace/nsregistry"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.uber.org/mock/gomock"
)

type (
	registrySuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		regPersistence *nsregistry.MockPersistence
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
	s.regPersistence = nsregistry.NewMockPersistence(s.controller)
	s.registry = nsregistry.NewRegistry(
		s.regPersistence,
		true,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetBoolPropertyFn(false),
		metrics.NoopMetricsHandler,
		log.NewTestLogger(),
		namespace.NewDefaultReplicationResolverFactory(),
	)
}

func (s *registrySuite) TearDownTest() {
	s.registry.Stop()
	s.controller.Finish()
}

func (s *registrySuite) TestListNamespace() {
	factory := namespace.NewDefaultReplicationResolverFactory()
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
	entry1, err := namespace.FromPersistentState(
		namespaceRecord1.Namespace,
		factory(namespaceRecord1.Namespace),
		namespace.WithNotificationVersion(namespaceRecord1.NotificationVersion))
	s.NoError(err)

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
	entry2, err := namespace.FromPersistentState(
		namespaceRecord2.Namespace,
		factory(namespaceRecord2.Namespace),
		namespace.WithNotificationVersion(namespaceRecord2.NotificationVersion))
	s.NoError(err)

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
		PageSize:       nsregistry.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecord1},
		NextPageToken: pageToken,
	}, nil)

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       nsregistry.CacheRefreshPageSize,
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
	factory := namespace.NewDefaultReplicationResolverFactory()
	entry1, err := namespace.FromPersistentState(
		namespaceRecord1.Namespace,
		factory(namespaceRecord1.Namespace),
		namespace.WithNotificationVersion(namespaceRecord1.NotificationVersion))
	s.NoError(err)

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
	entry2, err := namespace.FromPersistentState(
		namespaceRecord2.Namespace,
		factory(namespaceRecord2.Namespace),
		namespace.WithNotificationVersion(namespaceNotificationVersion))
	s.NoError(err)

	namespaceNotificationVersion++

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       nsregistry.CacheRefreshPageSize,
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
	// Compare by ID and key properties instead of pointer equality
	s.Equal(entry1.ID(), entriesNotification[0].ID())
	s.Equal(entry1.Name(), entriesNotification[0].Name())
	s.Equal(entry1.NotificationVersion(), entriesNotification[0].NotificationVersion())
	s.Equal(entry2.ID(), entriesNotification[1].ID())
	s.Equal(entry2.Name(), entriesNotification[1].Name())
	s.Equal(entry2.NotificationVersion(), entriesNotification[1].NotificationVersion())
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
	namespaceNotificationVersion++
	factory := namespace.NewDefaultReplicationResolverFactory()
	entry1Old, err := namespace.FromPersistentState(
		namespaceRecord1Old.Namespace,
		factory(namespaceRecord1Old.Namespace),
		namespace.WithNotificationVersion(namespaceRecord1Old.NotificationVersion))
	s.NoError(err)
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
	entry2Old, err := namespace.FromPersistentState(
		namespaceRecord2Old.Namespace,
		factory(namespaceRecord2Old.Namespace),
		namespace.WithNotificationVersion(namespaceRecord2Old.NotificationVersion))
	s.NoError(err)
	namespaceNotificationVersion++

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       nsregistry.CacheRefreshPageSize,
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
	entry2New, err := namespace.FromPersistentState(
		namespaceRecord2New.Namespace,
		factory(namespaceRecord2New.Namespace),
		namespace.WithNotificationVersion(namespaceRecord2New.NotificationVersion))
	s.NoError(err)
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
		PageSize:       nsregistry.CacheRefreshPageSize,
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
	// Compare by ID and key properties instead of pointer equality
	s.Equal(entry1Old.ID(), entries[0].ID())
	s.Equal(entry1Old.Name(), entries[0].Name())
	s.Equal(entry1Old.NotificationVersion(), entries[0].NotificationVersion())
	s.Equal(entry2Old.ID(), entries[1].ID())
	s.Equal(entry2Old.Name(), entries[1].Name())
	s.Equal(entry2Old.NotificationVersion(), entries[1].NotificationVersion())

	wg.Add(1)
	wg.Wait()

	newEntries := entries[2:]

	// entry1 only has descrption update, so won't trigger the state change callback
	s.Len(newEntries, 1)
	// Compare by ID and key properties instead of pointer equality
	s.Equal(entry2New.ID(), newEntries[0].ID())
	s.Equal(entry2New.Name(), newEntries[0].Name())
	s.Equal(entry2New.NotificationVersion(), newEntries[0].NotificationVersion())
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
	factory := namespace.NewDefaultReplicationResolverFactory()
	entryOld, err := namespace.FromPersistentState(
		namespaceRecordOld.Namespace,
		factory(namespaceRecordOld.Namespace))
	s.NoError(err)

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       nsregistry.CacheRefreshPageSize,
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
		PageSize:       nsregistry.CacheRefreshPageSize,
		IncludeDeleted: true,
		NextPageToken:  nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{
			namespaceRecord1,
			namespaceRecord2},
		NextPageToken: nil,
	}, nil)

	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:       nsregistry.CacheRefreshPageSize,
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

// TestNamespaceRename validates that when a namespace is renamed via the
// background refresh loop (refreshNamespaces), the old name no longer resolves
// and the new name resolves correctly. This tests that stale name->ID mappings
// are properly cleaned up when the namespace name changes in the database.
func (s *registrySuite) TestNamespaceRename() {
	id := namespace.NewID()
	namespaceNotificationVersion := int64(0)

	// Initial namespace with original name
	namespaceRecordOld := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   id.String(),
				Name: "original-name",
				Data: make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
			ConfigVersion:   1,
			FailoverVersion: 1,
		},
		NotificationVersion: namespaceNotificationVersion,
	}
	namespaceNotificationVersion++

	// Renamed namespace (same ID, different name, changed ActiveClusterName to trigger callback)
	namespaceRecordNew := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   id.String(),
				Name: "renamed-name",
				Data: make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName, // changed to trigger callback
				Clusters:          []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
			},
			ConfigVersion:   2,
			FailoverVersion: 2,
		},
		NotificationVersion: namespaceNotificationVersion,
	}

	// First ListNamespaces returns original name
	s.regPersistence.EXPECT().ListNamespaces(
		gomock.Any(), &persistence.ListNamespacesRequest{
			PageSize:       nsregistry.CacheRefreshPageSize,
			IncludeDeleted: true,
			NextPageToken:  nil,
		},
	).Return(
		&persistence.ListNamespacesResponse{
			Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecordOld},
			NextPageToken: nil,
		}, nil,
	)

	// Used to temporarily block the second refresh
	blockRefreshCh := make(chan struct{})

	// Second and subsequent ListNamespaces returns renamed namespace
	s.regPersistence.EXPECT().ListNamespaces(
		gomock.Any(), &persistence.ListNamespacesRequest{
			PageSize:       nsregistry.CacheRefreshPageSize,
			IncludeDeleted: true,
			NextPageToken:  nil,
		},
	).DoAndReturn(func(context.Context, *persistence.ListNamespacesRequest) (*persistence.ListNamespacesResponse, error) {
		<-blockRefreshCh
		return &persistence.ListNamespacesResponse{
			Namespaces:    []*persistence.GetNamespaceResponse{namespaceRecordNew},
			NextPageToken: nil,
		}, nil
	}).MinTimes(1)

	s.registry.Start()
	// Register callback to detect when the rename is applied
	refreshCompletedCh := make(chan struct{})
	s.registry.RegisterStateChangeCallback("test", func(ns *namespace.Namespace, deletedFromDb bool) {
		if ns.Name() == "renamed-name" {
			close(refreshCompletedCh)
		}
	})
	defer s.registry.Stop()

	// Verify original name works before rename
	ns, err := s.registry.GetNamespace("original-name")
	s.NoError(err)
	s.Equal(namespace.Name("original-name"), ns.Name())
	s.Equal(id, ns.ID())

	// Allow second refresh through and wait for it to happen
	close(blockRefreshCh)
	<-refreshCompletedCh

	// Verify new name works after rename
	ns, err = s.registry.GetNamespace("renamed-name")
	s.NoError(err)
	s.Equal(namespace.Name("renamed-name"), ns.Name())
	s.Equal(id, ns.ID())

	// Verify ID lookup returns namespace with new name
	ns, err = s.registry.GetNamespaceByID(id)
	s.NoError(err)
	s.Equal(namespace.Name("renamed-name"), ns.Name())

	// Verify old name no longer resolves
	// Disable readthrough so we only check the cache
	_, err = s.registry.GetNamespaceWithOptions(
		"original-name",
		namespace.GetNamespaceOptions{DisableReadthrough: true},
	)
	s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
}

// TestNamespaceRenameViaRefreshById validates that when a namespace is renamed
// and refreshed via RefreshNamespaceById (which calls updateSingleNamespace),
// the old name no longer resolves and the new name resolves correctly. This tests
// that stale name->ID mappings are properly cleaned up during single-namespace
// refresh operations triggered by readthrough or explicit refresh calls.
func (s *registrySuite) TestNamespaceRenameViaRefreshById() {
	id := namespace.NewID()

	// Initial namespace with original name
	nsOriginal := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   id.String(),
				Name: "original-name",
				Data: make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
			FailoverVersion: 1,
		},
		NotificationVersion: 1,
	}

	// Renamed namespace (same ID, different name)
	nsRenamed := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   id.String(),
				Name: "renamed-name",
				Data: make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(1),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
			FailoverVersion: 1,
		},
		NotificationVersion: 2,
	}

	// Load original namespace via ListNamespaces on Start
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).Return(
		&persistence.ListNamespacesResponse{
			Namespaces: []*persistence.GetNamespaceResponse{nsOriginal},
		}, nil,
	).MinTimes(1)

	s.registry.Start()
	defer s.registry.Stop()

	// Verify original name works
	ns, err := s.registry.GetNamespace("original-name")
	s.NoError(err)
	s.Equal(namespace.Name("original-name"), ns.Name())
	s.Equal(id, ns.ID())

	// Mock GetNamespace to return the renamed namespace
	s.regPersistence.EXPECT().GetNamespace(
		gomock.Any(), &persistence.GetNamespaceRequest{
			ID: id.String(),
		},
	).Return(nsRenamed, nil).Times(1)

	// Refresh the namespace by ID (triggers updateSingleNamespace)
	ns, err = s.registry.RefreshNamespaceById(id)
	s.NoError(err)
	s.Equal(namespace.Name("renamed-name"), ns.Name())

	// Verify new name works
	ns, err = s.registry.GetNamespace("renamed-name")
	s.NoError(err)
	s.Equal(namespace.Name("renamed-name"), ns.Name())
	s.Equal(id, ns.ID())

	// Verify ID lookup returns namespace with new name
	ns, err = s.registry.GetNamespaceByID(id)
	s.NoError(err)
	s.Equal(namespace.Name("renamed-name"), ns.Name())

	// Verify old name no longer resolves
	_, err = s.registry.GetNamespaceWithOptions(
		"original-name",
		namespace.GetNamespaceOptions{DisableReadthrough: true},
	)
	s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
}

func (s *registrySuite) TestRefreshSingleCacheKeyById() {
	id := namespace.NewID()

	nsV1 := persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   id.String(),
				Name: "foo",
			},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			FailoverVersion:   1,
		},
		NotificationVersion: 1,
	}
	nsV2 := persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   id.String(),
				Name: "foo",
			},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			FailoverVersion:   2,
		},
		NotificationVersion: 2,
	}

	// registry start will refresh once
	s.regPersistence.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).Return(&persistence.ListNamespacesResponse{
		Namespaces: nil,
	}, nil)

	s.registry.Start()
	defer s.registry.Stop()

	// first call will get old ns
	s.regPersistence.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: id.String(),
	}).Return(&nsV1, nil).Times(1)
	ns, err := s.registry.GetNamespaceByID(id)
	s.NoError(err)
	s.Equal(nsV1.Namespace.FailoverVersion, ns.FailoverVersion())

	s.regPersistence.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: id.String(),
	}).Return(&nsV2, nil).Times(1)

	ns, err = s.registry.RefreshNamespaceById(id)
	s.NoError(err)
	s.Equal(nsV2.Namespace.FailoverVersion, ns.FailoverVersion())

	ns, err = s.registry.GetNamespaceByID(id)
	s.NoError(err)
	s.Equal(nsV2.Namespace.FailoverVersion, ns.FailoverVersion())
}
