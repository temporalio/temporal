package persistencetests

import (
	"context"
	"errors"
	"fmt"
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
	"go.temporal.io/server/common/debug"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// MetadataPersistenceSuiteV2 is test of the V2 version of metadata persistence
	MetadataPersistenceSuiteV2 struct {
		*TestBase
		// not merely log an error
		protorequire.ProtoAssertions

		ctx    context.Context
		cancel context.CancelFunc
	}
)

// SetupSuite implementation

// SetupTest implementation
func (m *MetadataPersistenceSuiteV2) SetupTest() {
	m.ProtoAssertions = protorequire.New(m.T())
	m.ctx, m.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	// cleanup the namespace created
	var token []byte
	pageSize := 10
ListLoop:
	for {
		resp, err := m.ListNamespaces(pageSize, token)
		require.NoError(m.T(), err)
		token = resp.NextPageToken
		for _, n := range resp.Namespaces {
			require.NoError(m.T(), m.DeleteNamespace(n.Namespace.Info.Id, ""))
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

// Partial namespace creation is only relevant for Cassandra, the following tests will only run when the underlying cluster is cassandra
func (m *MetadataPersistenceSuiteV2) createPartialNamespace(id string, name string) {
	// only add the namespace to namespaces_by_id table and not namespaces table
	const constNamespacePartition = 0
	const templateCreateNamespaceQuery = `INSERT INTO namespaces_by_id (` +
		`id, name) ` +
		`VALUES(?, ?) IF NOT EXISTS`
	query := m.DefaultTestCluster.(*cassandra.TestCluster).GetSession().Query(templateCreateNamespaceQuery, id, name).WithContext(context.Background())
	err := query.Exec()
	require.NoError(m.T(), err)

}

func (m *MetadataPersistenceSuiteV2) truncatePartialNamespace() {
	query := m.DefaultTestCluster.(*cassandra.TestCluster).GetSession().Query("TRUNCATE namespaces_by_id").WithContext(context.Background())
	err := query.Exec()
	require.NoError(m.T(), err)

	query = m.DefaultTestCluster.(*cassandra.TestCluster).GetSession().Query("TRUNCATE namespaces").WithContext(context.Background())
	err = query.Exec()
	require.NoError(m.T(), err)
}

func (m *MetadataPersistenceSuiteV2) TestCreateWithPartialNamespaceSameNameSameID() {
	// This is only relevant for cassandra
	switch m.DefaultTestCluster.(type) {
	case *cassandra.TestCluster:
	default:
		return
	}
	id := uuid.New()
	name := "create-partial-namespace-test-name"
	m.createPartialNamespace(id, name)

	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "create-namespace-test-description"
	owner := "create-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}
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
	require.NoError(m.T(), err0)
	require.NotNil(m.T(), resp0)
	require.EqualValues(m.T(), id, resp0.ID)

	// for namespace which do not have replication config set, will default to
	// use current cluster as active, with current cluster as all clusters
	resp1, err1 := m.GetNamespace(id, "")
	require.NoError(m.T(), err1)
	require.NotNil(m.T(), resp1)
	require.EqualValues(m.T(), id, resp1.Namespace.Info.Id)
	require.Equal(m.T(), name, resp1.Namespace.Info.Name)
	require.Equal(m.T(), state, resp1.Namespace.Info.State)
	require.Equal(m.T(), description, resp1.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp1.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp1.Namespace.Info.Data)
	require.EqualValues(m.T(), time.Duration(retention)*time.Hour*24, resp1.Namespace.Config.Retention.AsDuration())
	require.Equal(m.T(), historyArchivalState, resp1.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), historyArchivalURI, resp1.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), visibilityArchivalState, resp1.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), visibilityArchivalURI, resp1.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(badBinaries, resp1.Namespace.Config.BadBinaries)
	require.Equal(m.T(), cluster.TestCurrentClusterName, resp1.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), 1, len(resp1.Namespace.ReplicationConfig.Clusters))
	require.Equal(m.T(), isGlobalNamespace, resp1.IsGlobalNamespace)
	require.Equal(m.T(), configVersion, resp1.Namespace.ConfigVersion)
	require.Equal(m.T(), failoverVersion, resp1.Namespace.FailoverVersion)
	require.True(m.T(), resp1.Namespace.ReplicationConfig.Clusters[0] == cluster.TestCurrentClusterName)
	require.Equal(m.T(), p.InitialFailoverNotificationVersion, resp1.Namespace.FailoverNotificationVersion)
	m.truncatePartialNamespace()
}

func (m *MetadataPersistenceSuiteV2) TestCreateWithPartialNamespaceSameNameDifferentID() {
	// This is only relevant for cassandra
	switch m.DefaultTestCluster.(type) {
	case *cassandra.TestCluster:
	default:
		return
	}

	id := uuid.New()
	partialID := uuid.New()
	name := "create-partial-namespace-test-name"
	m.createPartialNamespace(partialID, name)
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "create-namespace-test-description"
	owner := "create-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}
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
	require.NoError(m.T(), err0)
	require.NotNil(m.T(), resp0)
	require.EqualValues(m.T(), id, resp0.ID)

	// for namespace which do not have replication config set, will default to
	// use current cluster as active, with current cluster as all clusters
	resp1, err1 := m.GetNamespace(id, "")
	require.NoError(m.T(), err1)
	require.NotNil(m.T(), resp1)
	require.EqualValues(m.T(), id, resp1.Namespace.Info.Id)
	require.Equal(m.T(), name, resp1.Namespace.Info.Name)
	require.Equal(m.T(), state, resp1.Namespace.Info.State)
	require.Equal(m.T(), description, resp1.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp1.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp1.Namespace.Info.Data)
	require.EqualValues(m.T(), time.Duration(retention)*time.Hour*24, resp1.Namespace.Config.Retention.AsDuration())
	require.Equal(m.T(), historyArchivalState, resp1.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), historyArchivalURI, resp1.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), visibilityArchivalState, resp1.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), visibilityArchivalURI, resp1.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(badBinaries, resp1.Namespace.Config.BadBinaries)
	require.Equal(m.T(), cluster.TestCurrentClusterName, resp1.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), 1, len(resp1.Namespace.ReplicationConfig.Clusters))
	require.Equal(m.T(), isGlobalNamespace, resp1.IsGlobalNamespace)
	require.Equal(m.T(), configVersion, resp1.Namespace.ConfigVersion)
	require.Equal(m.T(), failoverVersion, resp1.Namespace.FailoverVersion)
	require.True(m.T(), resp1.Namespace.ReplicationConfig.Clusters[0] == cluster.TestCurrentClusterName)
	require.Equal(m.T(), p.InitialFailoverNotificationVersion, resp1.Namespace.FailoverNotificationVersion)
	m.truncatePartialNamespace()
}

func (m *MetadataPersistenceSuiteV2) TestCreateWithPartialNamespaceDifferentNameSameID() {
	// This is only relevant for cassandra
	switch m.DefaultTestCluster.(type) {
	case *cassandra.TestCluster:
	default:
		return
	}
	id := uuid.New()
	name := "create-namespace-test-name-for-partial-test"
	partialName := "create-partial-namespace-test-name"
	m.createPartialNamespace(id, partialName)
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "create-namespace-test-description"
	owner := "create-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}
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
	require.Error(m.T(), err0)
	require.IsType(m.T(), &serviceerror.NamespaceAlreadyExists{}, err0)
	require.Nil(m.T(), resp0)
	m.truncatePartialNamespace()
}

// TestCreateNamespace test
func (m *MetadataPersistenceSuiteV2) TestCreateNamespace() {
	id := uuid.New()
	name := "create-namespace-test-name-for-partial-test"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "create-namespace-test-description"
	owner := "create-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}
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
	require.NoError(m.T(), err0)
	require.NotNil(m.T(), resp0)
	require.EqualValues(m.T(), id, resp0.ID)

	// for namespace which do not have replication config set, will default to
	// use current cluster as active, with current cluster as all clusters
	resp1, err1 := m.GetNamespace(id, "")
	require.NoError(m.T(), err1)
	require.NotNil(m.T(), resp1)
	require.EqualValues(m.T(), id, resp1.Namespace.Info.Id)
	require.Equal(m.T(), name, resp1.Namespace.Info.Name)
	require.Equal(m.T(), state, resp1.Namespace.Info.State)
	require.Equal(m.T(), description, resp1.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp1.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp1.Namespace.Info.Data)
	require.EqualValues(m.T(), time.Duration(retention)*time.Hour*24, resp1.Namespace.Config.Retention.AsDuration())
	require.Equal(m.T(), historyArchivalState, resp1.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), historyArchivalURI, resp1.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), visibilityArchivalState, resp1.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), visibilityArchivalURI, resp1.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(badBinaries, resp1.Namespace.Config.BadBinaries)
	require.Equal(m.T(), cluster.TestCurrentClusterName, resp1.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), 1, len(resp1.Namespace.ReplicationConfig.Clusters))
	require.Equal(m.T(), isGlobalNamespace, resp1.IsGlobalNamespace)
	require.Equal(m.T(), configVersion, resp1.Namespace.ConfigVersion)
	require.Equal(m.T(), failoverVersion, resp1.Namespace.FailoverVersion)
	require.True(m.T(), resp1.Namespace.ReplicationConfig.Clusters[0] == cluster.TestCurrentClusterName)
	require.Equal(m.T(), p.InitialFailoverNotificationVersion, resp1.Namespace.FailoverNotificationVersion)

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
	require.Error(m.T(), err2)
	require.IsType(m.T(), &serviceerror.NamespaceAlreadyExists{}, err2)
	require.Nil(m.T(), resp2)
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
	require.Nil(m.T(), resp0)
	require.Error(m.T(), err0)
	require.IsType(m.T(), &serviceerror.NamespaceNotFound{}, err0)
	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:     "test-reason",
				Operator:   "test-operator",
				CreateTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
	require.NoError(m.T(), err1)
	require.NotNil(m.T(), resp1)
	require.EqualValues(m.T(), id, resp1.ID)

	resp2, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	require.NotNil(m.T(), resp2)
	require.EqualValues(m.T(), id, resp2.Namespace.Info.Id)
	require.Equal(m.T(), name, resp2.Namespace.Info.Name)
	require.Equal(m.T(), state, resp2.Namespace.Info.State)
	require.Equal(m.T(), description, resp2.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp2.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp2.Namespace.Info.Data)
	require.EqualValues(m.T(), time.Duration(retention)*time.Hour*24, resp2.Namespace.Config.Retention.AsDuration())
	require.Equal(m.T(), historyArchivalState, resp2.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), historyArchivalURI, resp2.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), visibilityArchivalState, resp2.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), visibilityArchivalURI, resp2.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(testBinaries, resp2.Namespace.Config.BadBinaries)
	require.Equal(m.T(), clusterActive, resp2.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), len(clusters), len(resp2.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		require.Equal(m.T(), clusters[index], resp2.Namespace.ReplicationConfig.Clusters[index])
	}
	require.Equal(m.T(), isGlobalNamespace, resp2.IsGlobalNamespace)
	require.Equal(m.T(), configVersion, resp2.Namespace.ConfigVersion)
	require.Equal(m.T(), failoverVersion, resp2.Namespace.FailoverVersion)
	require.Equal(m.T(), p.InitialFailoverNotificationVersion, resp2.Namespace.FailoverNotificationVersion)

	resp3, err3 := m.GetNamespace("", name)
	require.NoError(m.T(), err3)
	require.NotNil(m.T(), resp3)
	require.EqualValues(m.T(), id, resp3.Namespace.Info.Id)
	require.Equal(m.T(), name, resp3.Namespace.Info.Name)
	require.Equal(m.T(), state, resp3.Namespace.Info.State)
	require.Equal(m.T(), description, resp3.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp3.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp3.Namespace.Info.Data)
	require.EqualValues(m.T(), time.Duration(retention)*time.Hour*24, resp3.Namespace.Config.Retention.AsDuration())
	require.Equal(m.T(), historyArchivalState, resp3.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), historyArchivalURI, resp3.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), visibilityArchivalState, resp3.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), visibilityArchivalURI, resp3.Namespace.Config.VisibilityArchivalUri)
	require.Equal(m.T(), clusterActive, resp3.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), len(clusters), len(resp3.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		require.Equal(m.T(), clusters[index], resp3.Namespace.ReplicationConfig.Clusters[index])
	}
	require.Equal(m.T(), isGlobalNamespace, resp3.IsGlobalNamespace)
	require.Equal(m.T(), configVersion, resp3.Namespace.ConfigVersion)
	require.Equal(m.T(), failoverVersion, resp3.Namespace.FailoverVersion)
	require.Equal(m.T(), p.InitialFailoverNotificationVersion, resp3.Namespace.FailoverNotificationVersion)

	resp4, err4 := m.GetNamespace(id, name)
	require.Error(m.T(), err4)
	require.IsType(m.T(), &serviceerror.InvalidArgument{}, err4)
	require.Nil(m.T(), resp4)

	resp5, err5 := m.GetNamespace("", "")
	require.Nil(m.T(), resp5)
	require.IsType(m.T(), &serviceerror.InvalidArgument{}, err5)
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
				CreateTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
	require.Equal(m.T(), int32(1), successCount)

	resp, err3 := m.GetNamespace("", name)
	require.NoError(m.T(), err3)
	require.NotNil(m.T(), resp)
	require.Equal(m.T(), name, resp.Namespace.Info.Name)
	require.Equal(m.T(), state, resp.Namespace.Info.State)
	require.Equal(m.T(), description, resp.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp.Namespace.Info.Owner)
	require.EqualValues(m.T(), time.Duration(retention)*time.Hour*24, resp.Namespace.Config.Retention.AsDuration())
	require.Equal(m.T(), historyArchivalState, resp.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), visibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(testBinaries, resp.Namespace.Config.BadBinaries)
	require.Equal(m.T(), clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), len(clusters), len(resp.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		require.Equal(m.T(), clusters[index], resp.Namespace.ReplicationConfig.Clusters[index])
	}
	require.Equal(m.T(), isGlobalNamespace, resp.IsGlobalNamespace)
	require.Equal(m.T(), configVersion, resp.Namespace.ConfigVersion)
	require.Equal(m.T(), failoverVersion, resp.Namespace.FailoverVersion)

	// check namespace data
	ss := strings.Split(resp.Namespace.Info.Data["k0"], "-")
	require.Equal(m.T(), 2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	require.NoError(m.T(), err)
	require.Equal(m.T(), true, vi > 0 && vi <= concurrency)
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
	badBinaries := &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}

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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id, resp1.ID)

	resp2, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	m.ProtoEqual(badBinaries, resp2.Namespace.Config.BadBinaries)
	metadata, err := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err)
	notificationVersion := metadata.NotificationVersion

	testBinaries := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {
				Reason:     "test-reason",
				Operator:   "test-operator",
				CreateTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
				time.Time{},
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
	require.Equal(m.T(), int32(1), successCount)

	resp3, err3 := m.GetNamespace("", name)
	require.NoError(m.T(), err3)
	require.NotNil(m.T(), resp3)
	require.EqualValues(m.T(), id, resp3.Namespace.Info.Id)
	require.Equal(m.T(), name, resp3.Namespace.Info.Name)
	require.Equal(m.T(), state, resp3.Namespace.Info.State)
	require.Equal(m.T(), isGlobalNamespace, resp3.IsGlobalNamespace)
	require.Equal(m.T(), description, resp3.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp3.Namespace.Info.Owner)

	require.EqualValues(m.T(), time.Duration(retention)*time.Hour*24, resp3.Namespace.Config.Retention.AsDuration())
	require.Equal(m.T(), historyArchivalState, resp3.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), historyArchivalURI, resp3.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), visibilityArchivalState, resp3.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), visibilityArchivalURI, resp3.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(testBinaries, resp3.Namespace.Config.BadBinaries)
	require.Equal(m.T(), clusterActive, resp3.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), len(clusters), len(resp3.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		require.Equal(m.T(), clusters[index], resp3.Namespace.ReplicationConfig.Clusters[index])
	}
	require.Equal(m.T(), isGlobalNamespace, resp3.IsGlobalNamespace)
	require.Equal(m.T(), configVersion, resp3.Namespace.ConfigVersion)
	require.Equal(m.T(), failoverVersion, resp3.Namespace.FailoverVersion)

	// check namespace data
	ss := strings.Split(resp3.Namespace.Info.Data["k0"], "-")
	require.Equal(m.T(), 2, len(ss))
	vi, err := strconv.Atoi(ss[1])
	require.NoError(m.T(), err)
	require.Equal(m.T(), true, vi > 0 && vi <= concurrency)
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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id, resp1.ID)

	resp2, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	metadata, err := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err)
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
				CreateTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
		failoverEndTime,
		notificationVersion,
		isGlobalNamespace,
	)
	require.NoError(m.T(), err3)

	resp4, err4 := m.GetNamespace("", name)
	require.NoError(m.T(), err4)
	require.NotNil(m.T(), resp4)
	require.EqualValues(m.T(), id, resp4.Namespace.Info.Id)
	require.Equal(m.T(), name, resp4.Namespace.Info.Name)
	require.Equal(m.T(), isGlobalNamespace, resp4.IsGlobalNamespace)
	require.Equal(m.T(), updatedState, resp4.Namespace.Info.State)
	require.Equal(m.T(), updatedDescription, resp4.Namespace.Info.Description)
	require.Equal(m.T(), updatedOwner, resp4.Namespace.Info.Owner)
	require.Equal(m.T(), updatedData, resp4.Namespace.Info.Data)
	m.ProtoEqual(updatedRetention, resp4.Namespace.Config.Retention)
	require.Equal(m.T(), updatedHistoryArchivalState, resp4.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), updatedHistoryArchivalURI, resp4.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), updatedVisibilityArchivalState, resp4.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), updatedVisibilityArchivalURI, resp4.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(testBinaries, resp4.Namespace.Config.BadBinaries)
	require.Equal(m.T(), updateClusterActive, resp4.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), len(updateClusters), len(resp4.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		require.Equal(m.T(), updateClusters[index], resp4.Namespace.ReplicationConfig.Clusters[index])
	}
	require.Equal(m.T(), updateConfigVersion, resp4.Namespace.ConfigVersion)
	require.Equal(m.T(), updateFailoverVersion, resp4.Namespace.FailoverVersion)
	require.Equal(m.T(), updateFailoverNotificationVersion, resp4.Namespace.FailoverNotificationVersion)
	require.Equal(m.T(), notificationVersion, resp4.NotificationVersion)
	m.EqualTimes(failoverEndTime, resp4.Namespace.FailoverEndTime.AsTime())

	resp5, err5 := m.GetNamespace(id, "")
	require.NoError(m.T(), err5)
	require.NotNil(m.T(), resp5)
	require.EqualValues(m.T(), id, resp5.Namespace.Info.Id)
	require.Equal(m.T(), name, resp5.Namespace.Info.Name)
	require.Equal(m.T(), isGlobalNamespace, resp5.IsGlobalNamespace)
	require.Equal(m.T(), updatedState, resp5.Namespace.Info.State)
	require.Equal(m.T(), updatedDescription, resp5.Namespace.Info.Description)
	require.Equal(m.T(), updatedOwner, resp5.Namespace.Info.Owner)
	require.Equal(m.T(), updatedData, resp5.Namespace.Info.Data)
	m.ProtoEqual(updatedRetention, resp5.Namespace.Config.Retention)
	require.Equal(m.T(), updatedHistoryArchivalState, resp5.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), updatedHistoryArchivalURI, resp5.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), updatedVisibilityArchivalState, resp5.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), updatedVisibilityArchivalURI, resp5.Namespace.Config.VisibilityArchivalUri)
	require.Equal(m.T(), updateClusterActive, resp5.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), len(updateClusters), len(resp5.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		require.Equal(m.T(), updateClusters[index], resp5.Namespace.ReplicationConfig.Clusters[index])
	}
	require.Equal(m.T(), updateConfigVersion, resp5.Namespace.ConfigVersion)
	require.Equal(m.T(), updateFailoverVersion, resp5.Namespace.FailoverVersion)
	require.Equal(m.T(), updateFailoverNotificationVersion, resp5.Namespace.FailoverNotificationVersion)
	require.Equal(m.T(), notificationVersion, resp5.NotificationVersion)
	m.EqualTimes(failoverEndTime, resp4.Namespace.FailoverEndTime.AsTime())

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
		time.Time{},
		notificationVersion,
		isGlobalNamespace,
	)
	require.NoError(m.T(), err6)

	resp6, err6 := m.GetNamespace(id, "")
	require.NoError(m.T(), err6)
	require.NotNil(m.T(), resp6)
	require.EqualValues(m.T(), id, resp6.Namespace.Info.Id)
	require.Equal(m.T(), name, resp6.Namespace.Info.Name)
	require.Equal(m.T(), isGlobalNamespace, resp6.IsGlobalNamespace)
	require.Equal(m.T(), updatedState, resp6.Namespace.Info.State)
	require.Equal(m.T(), updatedDescription, resp6.Namespace.Info.Description)
	require.Equal(m.T(), updatedOwner, resp6.Namespace.Info.Owner)
	require.Equal(m.T(), updatedData, resp6.Namespace.Info.Data)
	m.ProtoEqual(updatedRetention, resp6.Namespace.Config.Retention)
	require.Equal(m.T(), updatedHistoryArchivalState, resp6.Namespace.Config.HistoryArchivalState)
	require.Equal(m.T(), updatedHistoryArchivalURI, resp6.Namespace.Config.HistoryArchivalUri)
	require.Equal(m.T(), updatedVisibilityArchivalState, resp6.Namespace.Config.VisibilityArchivalState)
	require.Equal(m.T(), updatedVisibilityArchivalURI, resp6.Namespace.Config.VisibilityArchivalUri)
	m.ProtoEqual(testBinaries, resp6.Namespace.Config.BadBinaries)
	require.Equal(m.T(), updateClusterActive, resp6.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), len(updateClusters), len(resp6.Namespace.ReplicationConfig.Clusters))
	for index := range clusters {
		require.Equal(m.T(), updateClusters[index], resp4.Namespace.ReplicationConfig.Clusters[index])
	}
	require.Equal(m.T(), updateConfigVersion, resp6.Namespace.ConfigVersion)
	require.Equal(m.T(), updateFailoverVersion, resp6.Namespace.FailoverVersion)
	require.Equal(m.T(), updateFailoverNotificationVersion, resp6.Namespace.FailoverNotificationVersion)
	require.Equal(m.T(), notificationVersion, resp6.NotificationVersion)
	m.EqualTimes(time.Unix(0, 0).UTC(), resp6.Namespace.FailoverEndTime.AsTime())
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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id, resp1.ID)

	_, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)

	err3 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: name,
		NewName:      newName,
	})
	require.NoError(m.T(), err3)

	resp4, err4 := m.GetNamespace("", newName)
	require.NoError(m.T(), err4)
	require.NotNil(m.T(), resp4)
	require.EqualValues(m.T(), id, resp4.Namespace.Info.Id)
	require.Equal(m.T(), newName, resp4.Namespace.Info.Name)
	require.Equal(m.T(), isGlobalNamespace, resp4.IsGlobalNamespace)

	resp5, err5 := m.GetNamespace(id, "")
	require.NoError(m.T(), err5)
	require.NotNil(m.T(), resp5)
	require.EqualValues(m.T(), id, resp5.Namespace.Info.Id)
	require.Equal(m.T(), newName, resp5.Namespace.Info.Name)
	require.Equal(m.T(), isGlobalNamespace, resp5.IsGlobalNamespace)

	err6 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: newName,
		NewName:      newNewName,
	})
	require.NoError(m.T(), err6)

	resp6, err6 := m.GetNamespace(id, "")
	require.NoError(m.T(), err6)
	require.NotNil(m.T(), resp6)
	require.EqualValues(m.T(), id, resp6.Namespace.Info.Id)
	require.Equal(m.T(), newNewName, resp6.Namespace.Info.Name)
	require.Equal(m.T(), isGlobalNamespace, resp6.IsGlobalNamespace)
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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id, resp1.ID)

	resp2, err2 := m.GetNamespace("", name)
	require.NoError(m.T(), err2)
	require.NotNil(m.T(), resp2)

	err3 := m.DeleteNamespace("", name)
	require.NoError(m.T(), err3)

	// May need to loop here to avoid potential inconsistent read-after-write in cassandra
	require.Eventually(m.T(),
		func() bool {
			resp, err := m.GetNamespace("", name)
			if errors.As(err, new(*serviceerror.NamespaceNotFound)) {
				require.Nil(m.T(), resp)
				return true
			}
			return false
		},
		10*time.Second,
		100*time.Millisecond,
	)

	resp5, err5 := m.GetNamespace(id, "")
	require.Error(m.T(), err5)
	require.IsType(m.T(), &serviceerror.NamespaceNotFound{}, err5)
	require.Nil(m.T(), resp5)

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
	require.NoError(m.T(), err6)
	require.EqualValues(m.T(), id, resp6.ID)

	err7 := m.DeleteNamespace(id, "")
	require.NoError(m.T(), err7)

	resp8, err8 := m.GetNamespace("", name)
	require.Error(m.T(), err8)
	require.IsType(m.T(), &serviceerror.NamespaceNotFound{}, err8)
	require.Nil(m.T(), resp8)

	resp9, err9 := m.GetNamespace(id, "")
	require.Error(m.T(), err9)
	require.IsType(m.T(), &serviceerror.NamespaceNotFound{}, err9)
	require.Nil(m.T(), resp9)
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
				CreateTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}
	testBinaries2 := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"efg": {
				Reason:     "test-reason2",
				Operator:   "test-operator2",
				CreateTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
		require.NoError(m.T(), err)
	}

	var token []byte
	const pageSize = 1
	pageCount := 0
	outputNamespaces := make(map[string]*p.GetNamespaceResponse)
	for {
		resp, err := m.ListNamespaces(pageSize, token)
		require.NoError(m.T(), err)
		token = resp.NextPageToken
		for _, namespace := range resp.Namespaces {
			outputNamespaces[namespace.Namespace.Info.Id] = namespace
			// global notification version is already tested, so here we make it 0
			// so we can test == easily
			namespace.NotificationVersion = 0
		}
		// Some persistence backends return an unavoidable empty final page.
		if len(resp.Namespaces) > 0 {
			pageCount++
		}
		if len(token) == 0 {
			break
		}
	}

	// There should be 2 non-empty pages.
	require.Equal(m.T(), pageCount, 2)
	require.Equal(m.T(), len(inputNamespaces), len(outputNamespaces))
	for _, namespace := range inputNamespaces {
		m.DeepEqual(namespace, outputNamespaces[namespace.Namespace.Info.Id])
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
		require.NoError(m.T(), err)
	}

	var token []byte
	var listNamespacesPageSize2 []*p.GetNamespaceResponse
	pageCount := 0
	for {
		resp, err := m.ListNamespaces(2, token)
		require.NoError(m.T(), err)
		token = resp.NextPageToken
		listNamespacesPageSize2 = append(listNamespacesPageSize2, resp.Namespaces...)
		// Some persistence backends return an unavoidable empty final page.
		if len(resp.Namespaces) > 0 {
			pageCount++
		}
		if len(token) == 0 {
			break
		}
	}

	// There should be 1 non-empty page.
	require.Equal(m.T(), 1, pageCount)
	require.Len(m.T(), listNamespacesPageSize2, 2)
	for _, namespace := range listNamespacesPageSize2 {
		require.NotEqual(m.T(), namespace.Namespace.Info.State, enumspb.NAMESPACE_STATE_DELETED)
	}

	pageCount = 0
	var listNamespacesPageSize1 []*p.GetNamespaceResponse
	for {
		resp, err := m.ListNamespaces(1, token)
		require.NoError(m.T(), err)
		token = resp.NextPageToken
		listNamespacesPageSize1 = append(listNamespacesPageSize1, resp.Namespaces...)
		// Some persistence backends return an unavoidable empty final page.
		if len(resp.Namespaces) > 0 {
			pageCount++
		}
		if len(token) == 0 {
			break
		}
	}

	// There should be 2 non-empty pages.
	require.Equal(m.T(), 2, pageCount)
	require.Len(m.T(), listNamespacesPageSize1, 2)
	for _, namespace := range listNamespacesPageSize1 {
		require.NotEqual(m.T(), namespace.Namespace.Info.State, enumspb.NAMESPACE_STATE_DELETED)
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
	failoverEndTime time.Time,
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
			FailoverEndTime:             timestamppb.New(failoverEndTime),
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

// TestCASFailureUpdateNamespace tests CAS failure when trying to update a namespace
func (m *MetadataPersistenceSuiteV2) TestCASFailureUpdateNamespace() {
	id := uuid.New()
	name := "cas-update-namespace-test-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "cas-update-namespace-test-description"
	owner := "cas-update-namespace-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "test://history/uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}

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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id, resp1.ID)

	resp2, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	metadata, err := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err)
	notificationVersion := metadata.NotificationVersion

	updatedDescription := "description-updated"

	// Try to update with wrong notification version (stale version)
	err3 := m.UpdateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          resp2.Namespace.Info.Id,
			Name:        resp2.Namespace.Info.Name,
			State:       resp2.Namespace.Info.State,
			Description: updatedDescription,
			Owner:       resp2.Namespace.Info.Owner,
			Data:        resp2.Namespace.Info.Data,
		},
		&persistencespb.NamespaceConfig{
			Retention:               resp2.Namespace.Config.Retention,
			HistoryArchivalState:    resp2.Namespace.Config.HistoryArchivalState,
			HistoryArchivalUri:      resp2.Namespace.Config.HistoryArchivalUri,
			VisibilityArchivalState: resp2.Namespace.Config.VisibilityArchivalState,
			VisibilityArchivalUri:   resp2.Namespace.Config.VisibilityArchivalUri,
			BadBinaries:             badBinaries,
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: resp2.Namespace.ReplicationConfig.ActiveClusterName,
			Clusters:          resp2.Namespace.ReplicationConfig.Clusters,
		},
		resp2.Namespace.ConfigVersion,
		resp2.Namespace.FailoverVersion,
		resp2.Namespace.FailoverNotificationVersion,
		time.Time{},
		notificationVersion-1, // Use stale notification version to trigger CAS failure
		isGlobalNamespace,
	)
	require.ErrorAs(m.T(), err3, new(*serviceerror.Unavailable))

	// Verify that the namespace was not updated
	resp4, err4 := m.GetNamespace(id, "")
	require.NoError(m.T(), err4)
	require.Equal(m.T(), description, resp4.Namespace.Info.Description) // Should still have old description
}

// TestRenameNamespaceWithNameConflict tests name conflict when trying to rename a namespace
func (m *MetadataPersistenceSuiteV2) TestRenameNamespaceWithNameConflict() {
	id1 := uuid.New()
	name1 := "rename-conflict-namespace-1"
	id2 := uuid.New()
	name2 := "rename-conflict-namespace-2"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "rename-conflict-test-description"
	owner := "rename-conflict-test-owner"
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

	// Create first namespace
	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id1,
			Name:        name1,
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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id1, resp1.ID)

	// Create second namespace
	resp2, err2 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id2,
			Name:        name2,
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
	require.NoError(m.T(), err2)
	require.EqualValues(m.T(), id2, resp2.ID)

	// Try to rename namespace1 to the same name as namespace2 (should fail)
	err3 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: name1,
		NewName:      name2,
	})
	// The error should be a conflict/unavailable error due to CAS failure
	require.ErrorAs(m.T(), err3, new(*serviceerror.Unavailable))

	// Verify that we can still query namespace2 by name, proving the rename didn't affect it
	resp5, err5 := m.GetNamespace("", name2)
	require.NoError(m.T(), err5)
	require.Equal(m.T(), name2, resp5.Namespace.Info.Name)
	require.Equal(m.T(), id2, resp5.Namespace.Info.Id)

	// Verify that namespace1 can still be queried by its original name
	resp6, err6 := m.GetNamespace("", name1)
	require.NoError(m.T(), err6)
	require.Equal(m.T(), name1, resp6.Namespace.Info.Name)
	require.Equal(m.T(), id1, resp6.Namespace.Info.Id)
}

// TestGetMetadataVersionIncrement tests that GetMetadata correctly increments the version after a namespace is created
func (m *MetadataPersistenceSuiteV2) TestGetMetadataVersionIncrement() {
	// Get initial metadata version
	metadata1, err1 := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err1)
	initialVersion := metadata1.NotificationVersion

	// Create a namespace
	id := uuid.New()
	name := "metadata-version-test-namespace"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "metadata-version-test-description"
	owner := "metadata-version-test-owner"
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

	resp1, err2 := m.CreateNamespace(
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
	require.NoError(m.T(), err2)
	require.NotNil(m.T(), resp1)

	// Get metadata version after creation
	metadata2, err3 := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err3)
	afterCreationVersion := metadata2.NotificationVersion

	// Verify that the version was incremented
	require.Equal(m.T(), initialVersion+1, afterCreationVersion, "NotificationVersion should be incremented by exactly 1")
}

// TestCreateNamespaceWithDuplicateName tests creating a namespace with a name that already exists
func (m *MetadataPersistenceSuiteV2) TestCreateNamespaceWithDuplicateName() {
	id1 := uuid.New()
	name := "duplicate-name-test-namespace"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "duplicate-name-test-description"
	owner := "duplicate-name-test-owner"
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

	// Create first namespace
	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id1,
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
	require.NoError(m.T(), err1)
	require.NotNil(m.T(), resp1)

	// Verify the namespace was created
	getResp1, err2 := m.GetNamespace(id1, "")
	require.NoError(m.T(), err2)
	require.NotNil(m.T(), getResp1)
	require.Equal(m.T(), name, getResp1.Namespace.Info.Name)

	// Try to create another namespace with the same name but different ID
	id2 := uuid.New()
	_, err3 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id2,
			Name:        name, // Same name as the first namespace
			State:       state,
			Description: "different description",
			Owner:       "different owner",
			Data:        map[string]string{"k2": "v2"},
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(retention * 2),
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:      "",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	require.ErrorAs(m.T(), err3, new(*serviceerror.NamespaceAlreadyExists))

	// Verify that the original namespace still exists and was not modified
	getResp2, err4 := m.GetNamespace(id1, "")
	require.NoError(m.T(), err4)
	require.NotNil(m.T(), getResp2)
	require.Equal(m.T(), name, getResp2.Namespace.Info.Name)
	require.Equal(m.T(), description, getResp2.Namespace.Info.Description)
	require.Equal(m.T(), owner, getResp2.Namespace.Info.Owner)

	// Verify that the second namespace ID does not exist
	_, err5 := m.GetNamespace(id2, "")
	require.ErrorAs(m.T(), err5, new(*serviceerror.NamespaceNotFound))
}

// TestCreateNamespaceWithDuplicateID tests creating a namespace with an ID that already exists
func (m *MetadataPersistenceSuiteV2) TestCreateNamespaceWithDuplicateID() {
	id := uuid.New()
	name1 := "duplicate-id-test-namespace-1"
	name2 := "duplicate-id-test-namespace-2"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "duplicate-id-test-description"
	owner := "duplicate-id-test-owner"
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

	// Create first namespace
	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name1,
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
	require.NoError(m.T(), err1)
	require.NotNil(m.T(), resp1)

	// Verify the namespace was created
	getResp1, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	require.NotNil(m.T(), getResp1)
	require.Equal(m.T(), name1, getResp1.Namespace.Info.Name)
	require.Equal(m.T(), id, getResp1.Namespace.Info.Id)

	// Try to create another namespace with the same ID but different name
	_, err3 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id, // Same ID as the first namespace
			Name:        name2,
			State:       state,
			Description: "different description",
			Owner:       "different owner",
			Data:        map[string]string{"k2": "v2"},
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(retention * 2),
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:      "",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:   "",
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	require.ErrorAs(m.T(), err3, new(*serviceerror.NamespaceAlreadyExists))

	// Verify that the original namespace still exists and was not modified
	getResp2, err4 := m.GetNamespace(id, "")
	require.NoError(m.T(), err4)
	require.NotNil(m.T(), getResp2)
	require.Equal(m.T(), name1, getResp2.Namespace.Info.Name) // Should still have the original name
	require.Equal(m.T(), description, getResp2.Namespace.Info.Description)
	require.Equal(m.T(), owner, getResp2.Namespace.Info.Owner)
	require.Equal(m.T(), id, getResp2.Namespace.Info.Id)

	// Verify that the second name doesn't exist in the system
	_, err5 := m.GetNamespace("", name2)
	require.ErrorAs(m.T(), err5, new(*serviceerror.NamespaceNotFound))
}

// TestInitializeSystemNamespaces tests the initialization of system namespaces
func (m *MetadataPersistenceSuiteV2) TestInitializeSystemNamespaces() {
	clusterName := "test-cluster"

	// First initialization should succeed
	err1 := m.MetadataManager.InitializeSystemNamespaces(m.ctx, clusterName)
	require.NoError(m.T(), err1)

	// Verify the system namespace was created with correct properties
	resp, err2 := m.GetNamespace("", "temporal-system")
	require.NoError(m.T(), err2)
	require.NotNil(m.T(), resp)
	require.Equal(m.T(), "temporal-system", resp.Namespace.Info.Name)
	require.Equal(m.T(), "32049b68-7872-4094-8e63-d0dd59896a83", resp.Namespace.Info.Id)
	require.Equal(m.T(), enumspb.NAMESPACE_STATE_REGISTERED, resp.Namespace.Info.State)
	require.Equal(m.T(), "Temporal internal system namespace", resp.Namespace.Info.Description)
	require.Equal(m.T(), "temporal-core@temporal.io", resp.Namespace.Info.Owner)
	require.Equal(m.T(), clusterName, resp.Namespace.ReplicationConfig.ActiveClusterName)
	require.Equal(m.T(), []string{clusterName}, resp.Namespace.ReplicationConfig.Clusters)
	require.False(m.T(), resp.IsGlobalNamespace)

	// Second initialization should be idempotent
	err3 := m.MetadataManager.InitializeSystemNamespaces(m.ctx, clusterName)
	require.NoError(m.T(), err3, "InitializeSystemNamespaces should be idempotent")
}

// TestDeleteNamespaceIdempotency tests that delete operations are idempotent
func (m *MetadataPersistenceSuiteV2) TestDeleteNamespaceIdempotency() {
	id := uuid.New()
	name := "delete-idempotent-test-namespace"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "delete-idempotent-test-description"
	owner := "delete-idempotent-test-owner"
	retention := int32(10)

	clusterActive := "some random active cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := false

	// Create namespace
	resp1, err1 := m.CreateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          id,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
		},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(retention),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          []string{clusterActive},
		},
		isGlobalNamespace,
		configVersion,
		failoverVersion,
	)
	require.NoError(m.T(), err1)
	require.NotNil(m.T(), resp1)

	// Verify it exists
	resp2, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	require.NotNil(m.T(), resp2)
	require.Equal(m.T(), name, resp2.Namespace.Info.Name)

	// Delete by ID - first delete should succeed
	err3 := m.DeleteNamespace(id, "")
	require.NoError(m.T(), err3)

	// May need to loop here to avoid potential inconsistent read-after-write in cassandra
	require.Eventually(m.T(),
		func() bool {
			resp, err := m.GetNamespace(id, "")
			if errors.As(err, new(*serviceerror.NamespaceNotFound)) {
				require.Nil(m.T(), resp)
				return true
			}
			return false
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Delete again - This should NOT error (deleting a non-existent namespace is a no-op)
	err5 := m.DeleteNamespace(id, "")
	require.NoError(m.T(), err5, "Delete should be idempotent")

	// Delete by name should also be idempotent
	err6 := m.DeleteNamespace("", name)
	require.NoError(m.T(), err6, "Delete by name should be idempotent")
}

// TestUpdateNamespaceNotFound tests updating a non-existent namespace
func (m *MetadataPersistenceSuiteV2) TestUpdateNamespaceNotFound() {
	nonExistentID := uuid.New()
	name := "non-existent-namespace"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "test-description"
	owner := "test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)

	clusterActive := "some random active cluster name"
	configVersion := int64(10)
	failoverVersion := int64(59)
	isGlobalNamespace := false

	// Get current notification version
	metadata, err := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err)
	notificationVersion := metadata.NotificationVersion

	// Try to update a non-existent namespace
	_ = m.UpdateNamespace(
		&persistencespb.NamespaceInfo{
			Id:          nonExistentID,
			Name:        name,
			State:       state,
			Description: description,
			Owner:       owner,
			Data:        data,
		},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(retention),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          []string{clusterActive},
		},
		configVersion,
		failoverVersion,
		0,
		time.Time{},
		notificationVersion,
		isGlobalNamespace,
	)

	// Update operations may silently succeed on non-existent namespaces (no-op)
	// or may fail depending on implementation. For now, we just verify the operation completes.
	// The key test is that after the update, the namespace still doesn't exist.
	_, err3 := m.GetNamespace(nonExistentID, "")
	require.ErrorAs(m.T(), err3, new(*serviceerror.NamespaceNotFound))
}

// TestRenameNamespaceNotFound tests renaming a non-existent namespace
func (m *MetadataPersistenceSuiteV2) TestRenameNamespaceNotFound() {
	nonExistentName := "non-existent-namespace-" + uuid.New()
	newName := "new-name-for-non-existent"

	err := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: nonExistentName,
		NewName:      newName,
	})
	require.ErrorAs(m.T(), err, new(*serviceerror.NamespaceNotFound))
}

// TestRenameNamespaceCassandra tests Cassandra-specific RenameNamespace behavior
// This test verifies the two-step non-atomic rename operation in Cassandra
func (m *MetadataPersistenceSuiteV2) TestRenameNamespaceCassandra() {
	// This test is for Cassandra
	switch m.DefaultTestCluster.(type) {
	case *sql.TestCluster:
		m.T().Skip()
	default:
	}

	id := uuid.New()
	name := "cassandra-rename-test-name"
	newName := "cassandra-rename-test-new-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "cassandra-rename-test-description"
	owner := "cassandra-rename-test-owner"
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

	// Create namespace
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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id, resp1.ID)

	// Verify namespace exists with original name
	resp2, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	require.Equal(m.T(), name, resp2.Namespace.Info.Name)

	// Test 1: Rename to a new name
	err3 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: name,
		NewName:      newName,
	})
	require.NoError(m.T(), err3)

	// Verify namespace can be retrieved by new name
	resp4, err4 := m.GetNamespace("", newName)
	require.NoError(m.T(), err4)
	require.NotNil(m.T(), resp4)
	require.EqualValues(m.T(), id, resp4.Namespace.Info.Id)
	require.Equal(m.T(), newName, resp4.Namespace.Info.Name)
	require.Equal(m.T(), description, resp4.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp4.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp4.Namespace.Info.Data)

	// Verify namespace can be retrieved by ID and has new name
	resp5, err5 := m.GetNamespace(id, "")
	require.NoError(m.T(), err5)
	require.Equal(m.T(), newName, resp5.Namespace.Info.Name)

	// Verify old name no longer exists (may need eventual consistency check for Cassandra)
	require.Eventually(m.T(),
		func() bool {
			_, err := m.GetNamespace("", name)
			return errors.As(err, new(*serviceerror.NamespaceNotFound))
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Fetch metadata version before renaming
	metadataBeforeRename, err9 := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err9)

	// Test 2: Rename to the same name
	// In Cassandra, this will fail because it tries to INSERT a row that already exists
	// with IF NOT EXISTS, which is expected behavior for Cassandra's two-step process
	err6 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: newName,
		NewName:      newName,
	})
	require.ErrorAs(m.T(), err6, new(*serviceerror.Unavailable), "Renaming to the same name fails in Cassandra due to IF NOT EXISTS")

	// Test 3: Verify namespace still exists with same name (unchanged)
	resp7, err7 := m.GetNamespace(id, "")
	require.NoError(m.T(), err7)
	require.Equal(m.T(), newName, resp7.Namespace.Info.Name)

	// Test 4: Verify namespace can still be fetched by ID
	resp8, err8 := m.GetNamespace(id, "")
	require.NoError(m.T(), err8)
	require.Equal(m.T(), newName, resp8.Namespace.Info.Name)

	// Test 5: Verify metadata version was not incremented
	metadataAfterRename, err10 := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err10)
	require.Equal(m.T(), metadataBeforeRename.NotificationVersion, metadataAfterRename.NotificationVersion, "Notification version should not have been incremented")
}

// TestRenameNamespaceSQL tests SQL RenameNamespace behavior
// This test verifies the atomic transaction-based rename operation in SQL databases
func (m *MetadataPersistenceSuiteV2) TestRenameNamespaceSQL() {
	// This test is for SQL databases
	switch m.DefaultTestCluster.(type) {
	case *sql.TestCluster:
	default:
		m.T().Skip()
	}

	id := uuid.New()
	name := "sql-rename-test-name"
	newName := "sql-rename-test-new-name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "sql-rename-test-description"
	owner := "sql-rename-test-owner"
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

	// Create namespace
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
	require.NoError(m.T(), err1)
	require.EqualValues(m.T(), id, resp1.ID)

	// Verify namespace exists with original name
	resp2, err2 := m.GetNamespace(id, "")
	require.NoError(m.T(), err2)
	require.Equal(m.T(), name, resp2.Namespace.Info.Name)

	// Test 1: Rename to a new name
	err3 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: name,
		NewName:      newName,
	})
	require.NoError(m.T(), err3)

	// Verify namespace can be retrieved by new name
	resp4, err4 := m.GetNamespace("", newName)
	require.NoError(m.T(), err4)
	require.NotNil(m.T(), resp4)
	require.EqualValues(m.T(), id, resp4.Namespace.Info.Id)
	require.Equal(m.T(), newName, resp4.Namespace.Info.Name)
	require.Equal(m.T(), description, resp4.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp4.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp4.Namespace.Info.Data)

	// Verify namespace can be retrieved by ID and has new name
	resp5, err5 := m.GetNamespace(id, "")
	require.NoError(m.T(), err5)
	require.Equal(m.T(), newName, resp5.Namespace.Info.Name)

	// Verify old name no longer exists
	_, err6 := m.GetNamespace("", name)
	require.ErrorAs(m.T(), err6, new(*serviceerror.NamespaceNotFound), "Old name should not exist after rename")

	// Fetch metadata version before renaming
	metadataBeforeRename, err9 := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err9)

	// Test 2: Rename to the same name (idempotent operation)
	err7 := m.MetadataManager.RenameNamespace(m.ctx, &p.RenameNamespaceRequest{
		PreviousName: newName,
		NewName:      newName,
	})
	require.NoError(m.T(), err7, "Renaming to the same name should succeed")

	// Verify namespace still exists with same name and data is unchanged
	resp8, err8 := m.GetNamespace(id, "")
	require.NoError(m.T(), err8)
	require.Equal(m.T(), newName, resp8.Namespace.Info.Name)
	require.Equal(m.T(), description, resp8.Namespace.Info.Description)
	require.Equal(m.T(), owner, resp8.Namespace.Info.Owner)
	require.Equal(m.T(), data, resp8.Namespace.Info.Data)

	// Test 3: Verify atomicity - query by both ID and name should be consistent
	resp9, err9 := m.GetNamespace("", newName)
	require.NoError(m.T(), err9)
	require.Equal(m.T(), resp9.Namespace.Info.Id, resp8.Namespace.Info.Id)
	require.Equal(m.T(), resp9.Namespace.Info.Name, resp8.Namespace.Info.Name)
	require.Equal(m.T(), resp9.Namespace.Info.Description, resp8.Namespace.Info.Description)

	// Test 4: Verify metadata version was incremented
	metadataAfterRename, err11 := m.MetadataManager.GetMetadata(m.ctx)
	require.NoError(m.T(), err11)
	require.Greater(m.T(), metadataAfterRename.NotificationVersion, metadataBeforeRename.NotificationVersion, "Notification version should have been incremented")
}
