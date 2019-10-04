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

package cassandra

import (
	"fmt"

	"github.com/uber/cadence/common"

	"github.com/gocql/gocql"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

const constDomainPartition = 0
const domainMetadataRecordName = "cadence-domain-metadata"

const (
	templateDomainInfoType = `{` +
		`id: ?, ` +
		`name: ?, ` +
		`status: ?, ` +
		`description: ?, ` +
		`owner_email: ?, ` +
		`data: ? ` +
		`}`

	templateDomainConfigType = `{` +
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

	templateDomainReplicationConfigType = `{` +
		`active_cluster_name: ?, ` +
		`clusters: ? ` +
		`}`

	templateCreateDomainQuery = `INSERT INTO domains (` +
		`id, domain) ` +
		`VALUES(?, {name: ?}) IF NOT EXISTS`

	templateGetDomainQuery = `SELECT domain.name ` +
		`FROM domains ` +
		`WHERE id = ?`

	templateDeleteDomainQuery = `DELETE FROM domains ` +
		`WHERE id = ?`

	templateCreateDomainByNameQueryWithinBatchV2 = `INSERT INTO domains_by_name_v2 (` +
		`domains_partition, name, domain, config, replication_config, is_global_domain, config_version, failover_version, failover_notification_version, notification_version) ` +
		`VALUES(?, ?, ` + templateDomainInfoType + `, ` + templateDomainConfigType + `, ` + templateDomainReplicationConfigType + `, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateGetDomainByNameQueryV2 = `SELECT domain.id, domain.name, domain.status, domain.description, ` +
		`domain.owner_email, domain.data, config.retention, config.emit_metric, ` +
		`config.archival_bucket, config.archival_status, ` +
		`config.history_archival_status, config.history_archival_uri, ` +
		`config.visibility_archival_status, config.visibility_archival_uri, ` +
		`config.bad_binaries, config.bad_binaries_encoding, ` +
		`replication_config.active_cluster_name, replication_config.clusters, ` +
		`is_global_domain, ` +
		`config_version, ` +
		`failover_version, ` +
		`failover_notification_version, ` +
		`notification_version ` +
		`FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? ` +
		`and name = ?`

	templateUpdateDomainByNameQueryWithinBatchV2 = `UPDATE domains_by_name_v2 ` +
		`SET domain = ` + templateDomainInfoType + `, ` +
		`config = ` + templateDomainConfigType + `, ` +
		`replication_config = ` + templateDomainReplicationConfigType + `, ` +
		`config_version = ? ,` +
		`failover_version = ? ,` +
		`failover_notification_version = ? , ` +
		`notification_version = ? ` +
		`WHERE domains_partition = ? ` +
		`and name = ?`

	templateGetMetadataQueryV2 = `SELECT notification_version ` +
		`FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? ` +
		`and name = ? `

	templateUpdateMetadataQueryWithinBatchV2 = `UPDATE domains_by_name_v2 ` +
		`SET notification_version = ? ` +
		`WHERE domains_partition = ? ` +
		`and name = ? ` +
		`IF notification_version = ? `

	templateDeleteDomainByNameQueryV2 = `DELETE FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? ` +
		`and name = ?`

	templateListDomainQueryV2 = `SELECT name, domain.id, domain.name, domain.status, domain.description, ` +
		`domain.owner_email, domain.data, config.retention, config.emit_metric, ` +
		`config.archival_bucket, config.archival_status, ` +
		`config.history_archival_status, config.history_archival_uri, ` +
		`config.visibility_archival_status, config.visibility_archival_uri, ` +
		`config.bad_binaries, config.bad_binaries_encoding, ` +
		`replication_config.active_cluster_name, replication_config.clusters, ` +
		`is_global_domain, ` +
		`config_version, ` +
		`failover_version, ` +
		`failover_notification_version, ` +
		`notification_version ` +
		`FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? `
)

type (
	cassandraMetadataPersistenceV2 struct {
		cassandraStore
		currentClusterName string
	}
)

// newMetadataPersistenceV2 is used to create an instance of HistoryManager implementation
func newMetadataPersistenceV2(cfg config.Cassandra, currentClusterName string, logger log.Logger) (p.MetadataStore, error) {
	cluster := NewCassandraCluster(cfg.Hosts, cfg.Port, cfg.User, cfg.Password, cfg.Datacenter)
	cluster.Keyspace = cfg.Keyspace
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

// CreateDomain create a domain
// Cassandra does not support conditional updates across multiple tables.  For this reason we have to first insert into
// 'Domains' table and then do a conditional insert into domains_by_name table.  If the conditional write fails we
// delete the orphaned entry from domains table.  There is a chance delete entry could fail and we never delete the
// orphaned entry from domains table.  We might need a background job to delete those orphaned record.
func (m *cassandraMetadataPersistenceV2) CreateDomain(request *p.InternalCreateDomainRequest) (*p.CreateDomainResponse, error) {
	query := m.session.Query(templateCreateDomainQuery, request.Info.ID, request.Info.Name)
	applied, err := query.MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Inserting into domains table. Error: %v", err),
		}
	}
	if !applied {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed because of uuid collision."),
		}
	}

	return m.CreateDomainInV2Table(request)
}

// CreateDomainInV2Table is the temporary function used by domain v1 -> v2 migration
func (m *cassandraMetadataPersistenceV2) CreateDomainInV2Table(request *p.InternalCreateDomainRequest) (*p.CreateDomainResponse, error) {
	metadata, err := m.GetMetadata()
	if err != nil {
		return nil, err
	}

	batch := m.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateCreateDomainByNameQueryWithinBatchV2,
		constDomainPartition,
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
		request.IsGlobalDomain,
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
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Inserting into domains_by_name_v2 table. Error: %v", err),
		}
	}

	if !applied {
		// Domain already exist.  Delete orphan domain record before returning back to user
		if errDelete := m.session.Query(templateDeleteDomainQuery, request.Info.ID).Exec(); errDelete != nil {
			m.logger.Warn("Unable to delete orphan domain record. Error", tag.Error(errDelete))
		}

		if domain, ok := previous["domain"].(map[string]interface{}); ok {
			msg := fmt.Sprintf("Domain already exists.  DomainId: %v", domain["id"])
			return nil, &workflow.DomainAlreadyExistsError{
				Message: msg,
			}
		}

		return nil, &workflow.DomainAlreadyExistsError{
			Message: fmt.Sprintf("CreateDomain operation failed because of conditional failure."),
		}
	}

	return &p.CreateDomainResponse{ID: request.Info.ID}, nil
}

func (m *cassandraMetadataPersistenceV2) UpdateDomain(request *p.InternalUpdateDomainRequest) error {
	batch := m.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateUpdateDomainByNameQueryWithinBatchV2,
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
		constDomainPartition,
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
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Error: %v", err),
		}
	}
	if !applied {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed because of conditional failure."),
		}
	}

	return nil
}

func (m *cassandraMetadataPersistenceV2) GetDomain(request *p.GetDomainRequest) (*p.InternalGetDomainResponse, error) {
	var query *gocql.Query
	var err error
	info := &p.DomainInfo{}
	config := &p.InternalDomainConfig{}
	replicationConfig := &p.DomainReplicationConfig{}
	var replicationClusters []map[string]interface{}
	var failoverNotificationVersion int64
	var notificationVersion int64
	var failoverVersion int64
	var configVersion int64
	var isGlobalDomain bool

	if len(request.ID) > 0 && len(request.Name) > 0 {
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name specified in request.",
		}
	} else if len(request.ID) == 0 && len(request.Name) == 0 {
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name are empty.",
		}
	}

	handleError := func(name, ID string, err error) error {
		identity := name
		if len(ID) > 0 {
			identity = ID
		}
		if err == gocql.ErrNotFound {
			return &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Domain %s does not exist.", identity),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetDomain operation failed. Error %v", err),
		}
	}

	domainName := request.Name
	if len(request.ID) > 0 {
		query = m.session.Query(templateGetDomainQuery, request.ID)
		err = query.Scan(&domainName)
		if err != nil {
			return nil, handleError(request.Name, request.ID, err)
		}
	}

	var badBinariesData []byte
	var badBinariesDataEncoding string

	query = m.session.Query(templateGetDomainByNameQueryV2, constDomainPartition, domainName)
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
		&isGlobalDomain,
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

	return &p.InternalGetDomainResponse{
		Info:                        info,
		Config:                      config,
		ReplicationConfig:           replicationConfig,
		IsGlobalDomain:              isGlobalDomain,
		ConfigVersion:               configVersion,
		FailoverVersion:             failoverVersion,
		FailoverNotificationVersion: failoverNotificationVersion,
		NotificationVersion:         notificationVersion,
	}, nil
}

func (m *cassandraMetadataPersistenceV2) ListDomains(request *p.ListDomainsRequest) (*p.InternalListDomainsResponse, error) {
	var query *gocql.Query

	query = m.session.Query(templateListDomainQueryV2, constDomainPartition)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "ListDomains operation failed.  Not able to create query iterator.",
		}
	}

	var name string
	domain := &p.InternalGetDomainResponse{
		Info:              &p.DomainInfo{},
		Config:            &p.InternalDomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}
	var replicationClusters []map[string]interface{}
	var badBinariesData []byte
	var badBinariesDataEncoding string
	response := &p.InternalListDomainsResponse{}
	for iter.Scan(
		&name,
		&domain.Info.ID,
		&domain.Info.Name,
		&domain.Info.Status,
		&domain.Info.Description,
		&domain.Info.OwnerEmail,
		&domain.Info.Data,
		&domain.Config.Retention,
		&domain.Config.EmitMetric,
		&domain.Config.ArchivalBucket,
		&domain.Config.ArchivalStatus,
		&domain.Config.HistoryArchivalStatus,
		&domain.Config.HistoryArchivalURI,
		&domain.Config.VisibilityArchivalStatus,
		&domain.Config.VisibilityArchivalURI,
		&badBinariesData,
		&badBinariesDataEncoding,
		&domain.ReplicationConfig.ActiveClusterName,
		&replicationClusters,
		&domain.IsGlobalDomain,
		&domain.ConfigVersion,
		&domain.FailoverVersion,
		&domain.FailoverNotificationVersion,
		&domain.NotificationVersion,
	) {
		if name != domainMetadataRecordName {
			// do not include the metadata record
			if domain.Info.Data == nil {
				domain.Info.Data = map[string]string{}
			}
			domain.Config.BadBinaries = p.NewDataBlob(badBinariesData, common.EncodingType(badBinariesDataEncoding))
			badBinariesData = []byte("")
			badBinariesDataEncoding = ""
			domain.ReplicationConfig.ActiveClusterName = p.GetOrUseDefaultActiveCluster(m.currentClusterName, domain.ReplicationConfig.ActiveClusterName)
			domain.ReplicationConfig.Clusters = p.DeserializeClusterConfigs(replicationClusters)
			domain.ReplicationConfig.Clusters = p.GetOrUseDefaultClusters(m.currentClusterName, domain.ReplicationConfig.Clusters)
			response.Domains = append(response.Domains, domain)
		}
		domain = &p.InternalGetDomainResponse{
			Info:              &p.DomainInfo{},
			Config:            &p.InternalDomainConfig{},
			ReplicationConfig: &p.DomainReplicationConfig{},
		}
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListDomains operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (m *cassandraMetadataPersistenceV2) DeleteDomain(request *p.DeleteDomainRequest) error {
	var name string
	query := m.session.Query(templateGetDomainQuery, request.ID)
	err := query.Scan(&name)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}

	return m.deleteDomain(name, request.ID)
}

func (m *cassandraMetadataPersistenceV2) DeleteDomainByName(request *p.DeleteDomainByNameRequest) error {
	var ID string
	query := m.session.Query(templateGetDomainByNameQueryV2, constDomainPartition, request.Name)
	err := query.Scan(&ID, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}
	return m.deleteDomain(request.Name, ID)
}

func (m *cassandraMetadataPersistenceV2) GetMetadata() (*p.GetMetadataResponse, error) {
	var notificationVersion int64
	query := m.session.Query(templateGetMetadataQueryV2, constDomainPartition, domainMetadataRecordName)
	err := query.Scan(&notificationVersion)
	if err != nil {
		if err == gocql.ErrNotFound {
			// this error can be thrown in the very beginning,
			// i.e. when domains_by_name_v2 is initialized
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
		constDomainPartition,
		domainMetadataRecordName,
		currentVersion,
	)
}

func (m *cassandraMetadataPersistenceV2) deleteDomain(name, ID string) error {
	query := m.session.Query(templateDeleteDomainByNameQueryV2, constDomainPartition, name)
	if err := query.Exec(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteDomainByName operation failed. Error %v", err),
		}
	}

	query = m.session.Query(templateDeleteDomainQuery, ID)
	if err := query.Exec(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteDomain operation failed. Error %v", err),
		}
	}

	return nil
}
