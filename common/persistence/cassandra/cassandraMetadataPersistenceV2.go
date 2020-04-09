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

package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cassandra"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/config"
)

const constNamespacePartition = 0
const namespaceMetadataRecordName = "temporal-namespace-metadata"

const (
	templateNamespaceInfoType = `{` +
		`id: ?, ` +
		`name: ?, ` +
		`status: ?, ` +
		`description: ?, ` +
		`owner_email: ?, ` +
		`data: ? ` +
		`}`

	templateNamespaceConfigType = `{` +
		`retention: ?, ` +
		`emit_metric: ?, ` +
		`archival_bucket: ?, ` +
		`archival_status: ?,` +
		`history_archival_status: ?, ` +
		`history_archival_uri: ?, ` +
		`visibility_archival_status: ?, ` +
		`visibility_archival_uri: ?, ` +
		`bad_binaries: ?,` +
		`bad_binaries_encoding: ?` +
		`}`

	templateNamespaceReplicationConfigType = `{` +
		`active_cluster_name: ?, ` +
		`clusters: ? ` +
		`}`

	templateCreateNamespaceQuery = `INSERT INTO namespaces (` +
		`id, namespace) ` +
		`VALUES(?, {name: ?}) IF NOT EXISTS`

	templateGetNamespaceQuery = `SELECT namespace.name ` +
		`FROM namespaces ` +
		`WHERE id = ?`

	templateDeleteNamespaceQuery = `DELETE FROM namespaces ` +
		`WHERE id = ?`

	templateCreateNamespaceByNameQueryWithinBatchV2 = `INSERT INTO namespaces_by_name_v2 (` +
		`namespaces_partition, name, namespace, config, replication_config, is_global_namespace, config_version, failover_version, failover_notification_version, notification_version) ` +
		`VALUES(?, ?, ` + templateNamespaceInfoType + `, ` + templateNamespaceConfigType + `, ` + templateNamespaceReplicationConfigType + `, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateGetNamespaceByNameQueryV2 = `SELECT namespace.id, namespace.name, namespace.status, namespace.description, ` +
		`namespace.owner_email, namespace.data, config.retention, config.emit_metric, ` +
		`config.archival_bucket, config.archival_status, ` +
		`config.history_archival_status, config.history_archival_uri, ` +
		`config.visibility_archival_status, config.visibility_archival_uri, ` +
		`config.bad_binaries, config.bad_binaries_encoding, ` +
		`replication_config.active_cluster_name, replication_config.clusters, ` +
		`is_global_namespace, ` +
		`config_version, ` +
		`failover_version, ` +
		`failover_notification_version, ` +
		`notification_version ` +
		`FROM namespaces_by_name_v2 ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ?`

	templateUpdateNamespaceByNameQueryWithinBatchV2 = `UPDATE namespaces_by_name_v2 ` +
		`SET namespace = ` + templateNamespaceInfoType + `, ` +
		`config = ` + templateNamespaceConfigType + `, ` +
		`replication_config = ` + templateNamespaceReplicationConfigType + `, ` +
		`config_version = ? ,` +
		`failover_version = ? ,` +
		`failover_notification_version = ? , ` +
		`notification_version = ? ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ?`

	templateGetMetadataQueryV2 = `SELECT notification_version ` +
		`FROM namespaces_by_name_v2 ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ? `

	templateUpdateMetadataQueryWithinBatchV2 = `UPDATE namespaces_by_name_v2 ` +
		`SET notification_version = ? ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ? ` +
		`IF notification_version = ? `

	templateDeleteNamespaceByNameQueryV2 = `DELETE FROM namespaces_by_name_v2 ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ?`

	templateListNamespaceQueryV2 = `SELECT name, namespace.id, namespace.name, namespace.status, namespace.description, ` +
		`namespace.owner_email, namespace.data, config.retention, config.emit_metric, ` +
		`config.archival_bucket, config.archival_status, ` +
		`config.history_archival_status, config.history_archival_uri, ` +
		`config.visibility_archival_status, config.visibility_archival_uri, ` +
		`config.bad_binaries, config.bad_binaries_encoding, ` +
		`replication_config.active_cluster_name, replication_config.clusters, ` +
		`is_global_namespace, ` +
		`config_version, ` +
		`failover_version, ` +
		`failover_notification_version, ` +
		`notification_version ` +
		`FROM namespaces_by_name_v2 ` +
		`WHERE namespaces_partition = ? `
)

type (
	cassandraMetadataPersistenceV2 struct {
		cassandraStore
		currentClusterName string
	}
)

// newMetadataPersistenceV2 is used to create an instance of HistoryManager implementation
func newMetadataPersistenceV2(cfg config.Cassandra, currentClusterName string, logger log.Logger) (p.MetadataStore, error) {
	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraMetadataPersistenceV2{
		cassandraStore:     cassandraStore{session: session, logger: logger},
		currentClusterName: currentClusterName,
	}, nil
}

// Close releases the resources held by this object
func (m *cassandraMetadataPersistenceV2) Close() {
	if m.session != nil {
		m.session.Close()
	}
}

// CreateNamespace create a namespace
// Cassandra does not support conditional updates across multiple tables.  For this reason we have to first insert into
// 'Namespaces' table and then do a conditional insert into namespaces_by_name table.  If the conditional write fails we
// delete the orphaned entry from namespaces table.  There is a chance delete entry could fail and we never delete the
// orphaned entry from namespaces table.  We might need a background job to delete those orphaned record.
func (m *cassandraMetadataPersistenceV2) CreateNamespace(request *p.InternalCreateNamespaceRequest) (*p.CreateNamespaceResponse, error) {
	query := m.session.Query(templateCreateNamespaceQuery, request.Info.ID, request.Info.Name)
	applied, err := query.MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateNamespace operation failed. Inserting into namespaces table. Error: %v", err))
	}
	if !applied {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateNamespace operation failed because of uuid collision."))
	}

	return m.CreateNamespaceInV2Table(request)
}

// CreateNamespaceInV2Table is the temporary function used by namespace v1 -> v2 migration
func (m *cassandraMetadataPersistenceV2) CreateNamespaceInV2Table(request *p.InternalCreateNamespaceRequest) (*p.CreateNamespaceResponse, error) {
	metadata, err := m.GetMetadata()
	if err != nil {
		return nil, err
	}

	batch := m.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateCreateNamespaceByNameQueryWithinBatchV2,
		constNamespacePartition,
		request.Info.Name,
		request.Info.ID,
		request.Info.Name,
		request.Info.Status,
		request.Info.Description,
		request.Info.OwnerEmail,
		request.Info.Data,
		request.Config.Retention,
		request.Config.EmitMetric,
		request.Config.ArchivalBucket,
		request.Config.ArchivalStatus,
		request.Config.HistoryArchivalStatus,
		request.Config.HistoryArchivalURI,
		request.Config.VisibilityArchivalStatus,
		request.Config.VisibilityArchivalURI,
		request.Config.BadBinaries.Data,
		string(request.Config.BadBinaries.GetEncoding()),
		request.ReplicationConfig.ActiveClusterName,
		p.SerializeClusterConfigs(request.ReplicationConfig.Clusters),
		request.IsGlobalNamespace,
		request.ConfigVersion,
		request.FailoverVersion,
		p.InitialFailoverNotificationVersion,
		metadata.NotificationVersion,
	)
	m.updateMetadataBatch(batch, metadata.NotificationVersion)

	previous := make(map[string]interface{})
	applied, iter, err := m.session.MapExecuteBatchCAS(batch, previous)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateNamespace operation failed. Inserting into namespaces_by_name_v2 table. Error: %v", err))
	}

	if !applied {
		// Namespace already exist.  Delete orphan namespace record before returning back to user
		if errDelete := m.session.Query(templateDeleteNamespaceQuery, request.Info.ID).Exec(); errDelete != nil {
			m.logger.Warn("Unable to delete orphan namespace record. Error", tag.Error(errDelete))
		}

		if namespace, ok := previous["namespace"].(map[string]interface{}); ok {
			msg := fmt.Sprintf("Namespace already exists.  NamespaceId: %v", namespace["id"])
			return nil, serviceerror.NewNamespaceAlreadyExists(msg)
		}

		return nil, serviceerror.NewNamespaceAlreadyExists(fmt.Sprintf("CreateNamespace operation failed because of conditional failure."))
	}

	return &p.CreateNamespaceResponse{ID: request.Info.ID}, nil
}

func (m *cassandraMetadataPersistenceV2) UpdateNamespace(request *p.InternalUpdateNamespaceRequest) error {
	batch := m.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateUpdateNamespaceByNameQueryWithinBatchV2,
		request.Info.ID,
		request.Info.Name,
		request.Info.Status,
		request.Info.Description,
		request.Info.OwnerEmail,
		request.Info.Data,
		request.Config.Retention,
		request.Config.EmitMetric,
		request.Config.ArchivalBucket,
		request.Config.ArchivalStatus,
		request.Config.HistoryArchivalStatus,
		request.Config.HistoryArchivalURI,
		request.Config.VisibilityArchivalStatus,
		request.Config.VisibilityArchivalURI,
		request.Config.BadBinaries.Data,
		string(request.Config.BadBinaries.GetEncoding()),
		request.ReplicationConfig.ActiveClusterName,
		p.SerializeClusterConfigs(request.ReplicationConfig.Clusters),
		request.ConfigVersion,
		request.FailoverVersion,
		request.FailoverNotificationVersion,
		request.NotificationVersion,
		constNamespacePartition,
		request.Info.Name,
	)
	m.updateMetadataBatch(batch, request.NotificationVersion)

	previous := make(map[string]interface{})
	applied, iter, err := m.session.MapExecuteBatchCAS(batch, previous)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("UpdateNamespace operation failed. Error: %v", err))
	}
	if !applied {
		return serviceerror.NewInternal(fmt.Sprintf("UpdateNamespace operation failed because of conditional failure."))
	}

	return nil
}

func (m *cassandraMetadataPersistenceV2) GetNamespace(request *p.GetNamespaceRequest) (*p.InternalGetNamespaceResponse, error) {
	var query *gocql.Query
	var err error
	info := &p.NamespaceInfo{}
	config := &p.InternalNamespaceConfig{}
	replicationConfig := &p.NamespaceReplicationConfig{}
	var replicationClusters []map[string]interface{}
	var failoverNotificationVersion int64
	var notificationVersion int64
	var failoverVersion int64
	var configVersion int64
	var isGlobalNamespace bool

	if len(request.ID) > 0 && len(request.Name) > 0 {
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name specified in request.")
	} else if len(request.ID) == 0 && len(request.Name) == 0 {
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name are empty.")
	}

	handleError := func(name, ID string, err error) error {
		identity := name
		if len(ID) > 0 {
			identity = ID
		}
		if err == gocql.ErrNotFound {
			return serviceerror.NewNotFound(fmt.Sprintf("Namespace %s does not exist.", identity))
		}
		return serviceerror.NewInternal(fmt.Sprintf("GetNamespace operation failed. Error %v", err))
	}

	namespace := request.Name
	if len(request.ID) > 0 {
		query = m.session.Query(templateGetNamespaceQuery, request.ID)
		err = query.Scan(&namespace)
		if err != nil {
			return nil, handleError(request.Name, request.ID, err)
		}
	}

	var badBinariesData []byte
	var badBinariesDataEncoding string

	query = m.session.Query(templateGetNamespaceByNameQueryV2, constNamespacePartition, namespace)
	err = query.Scan(
		&info.ID,
		&info.Name,
		&info.Status,
		&info.Description,
		&info.OwnerEmail,
		&info.Data,
		&config.Retention,
		&config.EmitMetric,
		&config.ArchivalBucket,
		&config.ArchivalStatus,
		&config.HistoryArchivalStatus,
		&config.HistoryArchivalURI,
		&config.VisibilityArchivalStatus,
		&config.VisibilityArchivalURI,
		&badBinariesData,
		&badBinariesDataEncoding,
		&replicationConfig.ActiveClusterName,
		&replicationClusters,
		&isGlobalNamespace,
		&configVersion,
		&failoverVersion,
		&failoverNotificationVersion,
		&notificationVersion,
	)

	if err != nil {
		return nil, handleError(request.Name, request.ID, err)
	}

	if info.Data == nil {
		info.Data = map[string]string{}
	}
	config.BadBinaries = p.NewDataBlob(badBinariesData, common.EncodingType(badBinariesDataEncoding))
	replicationConfig.ActiveClusterName = p.GetOrUseDefaultActiveCluster(m.currentClusterName, replicationConfig.ActiveClusterName)
	replicationConfig.Clusters = p.DeserializeClusterConfigs(replicationClusters)
	replicationConfig.Clusters = p.GetOrUseDefaultClusters(m.currentClusterName, replicationConfig.Clusters)

	return &p.InternalGetNamespaceResponse{
		Info:                        info,
		Config:                      config,
		ReplicationConfig:           replicationConfig,
		IsGlobalNamespace:           isGlobalNamespace,
		ConfigVersion:               configVersion,
		FailoverVersion:             failoverVersion,
		FailoverNotificationVersion: failoverNotificationVersion,
		NotificationVersion:         notificationVersion,
	}, nil
}

func (m *cassandraMetadataPersistenceV2) ListNamespaces(request *p.ListNamespacesRequest) (*p.InternalListNamespacesResponse, error) {
	var query *gocql.Query

	query = m.session.Query(templateListNamespaceQueryV2, constNamespacePartition)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("ListNamespaces operation failed.  Not able to create query iterator.")
	}

	var name string
	namespace := &p.InternalGetNamespaceResponse{
		Info:              &p.NamespaceInfo{},
		Config:            &p.InternalNamespaceConfig{},
		ReplicationConfig: &p.NamespaceReplicationConfig{},
	}
	var replicationClusters []map[string]interface{}
	var badBinariesData []byte
	var badBinariesDataEncoding string
	response := &p.InternalListNamespacesResponse{}
	for iter.Scan(
		&name,
		&namespace.Info.ID,
		&namespace.Info.Name,
		&namespace.Info.Status,
		&namespace.Info.Description,
		&namespace.Info.OwnerEmail,
		&namespace.Info.Data,
		&namespace.Config.Retention,
		&namespace.Config.EmitMetric,
		&namespace.Config.ArchivalBucket,
		&namespace.Config.ArchivalStatus,
		&namespace.Config.HistoryArchivalStatus,
		&namespace.Config.HistoryArchivalURI,
		&namespace.Config.VisibilityArchivalStatus,
		&namespace.Config.VisibilityArchivalURI,
		&badBinariesData,
		&badBinariesDataEncoding,
		&namespace.ReplicationConfig.ActiveClusterName,
		&replicationClusters,
		&namespace.IsGlobalNamespace,
		&namespace.ConfigVersion,
		&namespace.FailoverVersion,
		&namespace.FailoverNotificationVersion,
		&namespace.NotificationVersion,
	) {
		if name != namespaceMetadataRecordName {
			// do not include the metadata record
			if namespace.Info.Data == nil {
				namespace.Info.Data = map[string]string{}
			}
			namespace.Config.BadBinaries = p.NewDataBlob(badBinariesData, common.EncodingType(badBinariesDataEncoding))
			badBinariesData = []byte("")
			badBinariesDataEncoding = ""
			namespace.ReplicationConfig.ActiveClusterName = p.GetOrUseDefaultActiveCluster(m.currentClusterName, namespace.ReplicationConfig.ActiveClusterName)
			namespace.ReplicationConfig.Clusters = p.DeserializeClusterConfigs(replicationClusters)
			namespace.ReplicationConfig.Clusters = p.GetOrUseDefaultClusters(m.currentClusterName, namespace.ReplicationConfig.Clusters)
			response.Namespaces = append(response.Namespaces, namespace)
		}
		namespace = &p.InternalGetNamespaceResponse{
			Info:              &p.NamespaceInfo{},
			Config:            &p.InternalNamespaceConfig{},
			ReplicationConfig: &p.NamespaceReplicationConfig{},
		}
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListNamespaces operation failed. Error: %v", err))
	}

	return response, nil
}

func (m *cassandraMetadataPersistenceV2) DeleteNamespace(request *p.DeleteNamespaceRequest) error {
	var name string
	query := m.session.Query(templateGetNamespaceQuery, request.ID)
	err := query.Scan(&name)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}

	return m.deleteNamespace(name, request.ID)
}

func (m *cassandraMetadataPersistenceV2) DeleteNamespaceByName(request *p.DeleteNamespaceByNameRequest) error {
	var ID string
	query := m.session.Query(templateGetNamespaceByNameQueryV2, constNamespacePartition, request.Name)
	err := query.Scan(&ID, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}
	return m.deleteNamespace(request.Name, ID)
}

func (m *cassandraMetadataPersistenceV2) GetMetadata() (*p.GetMetadataResponse, error) {
	var notificationVersion int64
	query := m.session.Query(templateGetMetadataQueryV2, constNamespacePartition, namespaceMetadataRecordName)
	err := query.Scan(&notificationVersion)
	if err != nil {
		if err == gocql.ErrNotFound {
			// this error can be thrown in the very beginning,
			// i.e. when namespaces_by_name_v2 is initialized
			return &p.GetMetadataResponse{NotificationVersion: 0}, nil
		}
		return nil, err
	}
	return &p.GetMetadataResponse{NotificationVersion: notificationVersion}, nil
}

func (m *cassandraMetadataPersistenceV2) updateMetadataBatch(batch *gocql.Batch, notificationVersion int64) {
	var nextVersion int64 = 1
	var currentVersion *int64
	if notificationVersion > 0 {
		nextVersion = notificationVersion + 1
		currentVersion = &notificationVersion
	}
	batch.Query(templateUpdateMetadataQueryWithinBatchV2,
		nextVersion,
		constNamespacePartition,
		namespaceMetadataRecordName,
		currentVersion,
	)
}

func (m *cassandraMetadataPersistenceV2) deleteNamespace(name, ID string) error {
	query := m.session.Query(templateDeleteNamespaceByNameQueryV2, constNamespacePartition, name)
	if err := query.Exec(); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteNamespaceByName operation failed. Error %v", err))
	}

	query = m.session.Query(templateDeleteNamespaceQuery, ID)
	if err := query.Exec(); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteNamespace operation failed. Error %v", err))
	}

	return nil
}
