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

package persistence

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

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
		`emit_metric: ?` +
		`}`

	templateDomainReplicationConfigType = `{` +
		`active_cluster_name: ?, ` +
		`clusters: ? ` +
		`}`

	templateCreateDomainQuery = `INSERT INTO domains (` +
		`id, domain) ` +
		`VALUES(?, {name: ?}) IF NOT EXISTS`

	templateCreateDomainByNameQuery = `INSERT INTO domains_by_name (` +
		`name, domain, config, replication_config, is_global_domain, config_version, failover_version) ` +
		`VALUES(?, ` + templateDomainInfoType + `, ` + templateDomainConfigType + `, ` + templateDomainReplicationConfigType + `, ?, ?, ?) IF NOT EXISTS`

	templateGetDomainQuery = `SELECT domain.name ` +
		`FROM domains ` +
		`WHERE id = ?`

	templateGetDomainByNameQuery = `SELECT domain.id, domain.name, domain.status, domain.description, ` +
		`domain.owner_email, domain.data, config.retention, config.emit_metric, ` +
		`replication_config.active_cluster_name, replication_config.clusters, ` +
		`is_global_domain, ` +
		`config_version, ` +
		`failover_version, ` +
		`db_version ` +
		`FROM domains_by_name ` +
		`WHERE name = ?`

	templateUpdateDomainByNameQuery = `UPDATE domains_by_name ` +
		`SET domain = ` + templateDomainInfoType + `, ` +
		`config = ` + templateDomainConfigType + `, ` +
		`replication_config = ` + templateDomainReplicationConfigType + `, ` +
		`config_version = ? ,` +
		`failover_version = ? ,` +
		`db_version = ? ` +
		`WHERE name = ? ` +
		`IF db_version = ? `

	templateDeleteDomainQuery = `DELETE FROM domains ` +
		`WHERE id = ?`

	templateDeleteDomainByNameQuery = `DELETE FROM domains_by_name ` +
		`WHERE name = ?`
)

type (
	cassandraMetadataPersistence struct {
		session            *gocql.Session
		currentClusterName string
		logger             bark.Logger
	}
)

// NewCassandraMetadataPersistence is used to create an instance of HistoryManager implementation
func NewCassandraMetadataPersistence(hosts string, port int, user, password, dc string, keyspace string,
	currentClusterName string, logger bark.Logger) (MetadataManager,
	error) {
	cluster := common.NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraMetadataPersistence{
		session:            session,
		currentClusterName: currentClusterName,
		logger:             logger,
	}, nil
}

// Close releases the resources held by this object
func (m *cassandraMetadataPersistence) Close() {
	if m.session != nil {
		m.session.Close()
	}
}

// Cassandra does not support conditional updates across multiple tables.  For this reason we have to first insert into
// 'Domains' table and then do a conditional insert into domains_by_name table.  If the conditional write fails we
// delete the orphaned entry from domains table.  There is a chance delete entry could fail and we never delete the
// orphaned entry from domains table.  We might need a background job to delete those orphaned record.
func (m *cassandraMetadataPersistence) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	query := m.session.Query(templateCreateDomainQuery, request.Info.ID, request.Info.Name)
	applied, err := query.ScanCAS()
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

	query = m.session.Query(templateCreateDomainByNameQuery,
		request.Info.Name,
		request.Info.ID,
		request.Info.Name,
		request.Info.Status,
		request.Info.Description,
		request.Info.OwnerEmail,
		request.Info.Data,
		request.Config.Retention,
		request.Config.EmitMetric,
		request.ReplicationConfig.ActiveClusterName,
		serializeClusterConfigs(request.ReplicationConfig.Clusters),
		request.IsGlobalDomain,
		request.ConfigVersion,
		request.FailoverVersion,
	)

	previous := make(map[string]interface{})
	applied, err = query.MapScanCAS(previous)

	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Inserting into domains_by_name table. Error: %v", err),
		}
	}

	if !applied {
		// Domain already exist.  Delete orphan domain record before returning back to user
		if errDelete := m.session.Query(templateDeleteDomainQuery, request.Info.ID).Exec(); errDelete != nil {
			m.logger.Warnf("Unable to delete orphan domain record. Error: %v", errDelete)
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

	return &CreateDomainResponse{ID: request.Info.ID}, nil
}

func (m *cassandraMetadataPersistence) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	var query *gocql.Query
	var err error
	info := &DomainInfo{}
	config := &DomainConfig{}
	replicationConfig := &DomainReplicationConfig{}
	var replicationClusters []map[string]interface{}
	var dbVersion int64
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

	query = m.session.Query(templateGetDomainByNameQuery, domainName)
	err = query.Scan(
		&info.ID,
		&info.Name,
		&info.Status,
		&info.Description,
		&info.OwnerEmail,
		&info.Data,
		&config.Retention,
		&config.EmitMetric,
		&replicationConfig.ActiveClusterName,
		&replicationClusters,
		&isGlobalDomain,
		&configVersion,
		&failoverVersion,
		&dbVersion,
	)

	if err != nil {
		return nil, handleError(request.Name, request.ID, err)
	}

	replicationConfig.ActiveClusterName = GetOrUseDefaultActiveCluster(m.currentClusterName, replicationConfig.ActiveClusterName)
	replicationConfig.Clusters = deserializeClusterConfigs(replicationClusters)
	replicationConfig.Clusters = GetOrUseDefaultClusters(m.currentClusterName, replicationConfig.Clusters)

	return &GetDomainResponse{
		Info:                info,
		Config:              config,
		ReplicationConfig:   replicationConfig,
		IsGlobalDomain:      isGlobalDomain,
		ConfigVersion:       configVersion,
		FailoverVersion:     failoverVersion,
		NotificationVersion: dbVersion,
	}, nil
}

func (m *cassandraMetadataPersistence) UpdateDomain(request *UpdateDomainRequest) error {
	var nextVersion int64 = 1
	var currentVersion *int64
	if request.NotificationVersion > 0 {
		nextVersion = request.NotificationVersion + 1
		currentVersion = &request.NotificationVersion
	}
	query := m.session.Query(templateUpdateDomainByNameQuery,
		request.Info.ID,
		request.Info.Name,
		request.Info.Status,
		request.Info.Description,
		request.Info.OwnerEmail,
		request.Info.Data,
		request.Config.Retention,
		request.Config.EmitMetric,
		request.ReplicationConfig.ActiveClusterName,
		serializeClusterConfigs(request.ReplicationConfig.Clusters),
		request.ConfigVersion,
		request.FailoverVersion,
		nextVersion,
		request.Info.Name,
		currentVersion,
	)

	applied, err := query.ScanCAS()
	if !applied {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation encounter concurrent write."),
		}
	}
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Error %v", err),
		}
	}

	return nil
}

func (m *cassandraMetadataPersistence) DeleteDomain(request *DeleteDomainRequest) error {
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

func (m *cassandraMetadataPersistence) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	var ID string
	query := m.session.Query(templateGetDomainByNameQuery, request.Name)
	err := query.Scan(&ID, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}
	return m.deleteDomain(request.Name, ID)
}

func (m *cassandraMetadataPersistence) ListDomain(request *ListDomainRequest) (*ListDomainResponse, error) {
	panic("cassandraMetadataPersistence do not support list domain operation.")
}

func (m *cassandraMetadataPersistence) GetMetadata() (*GetMetadataResponse, error) {
	panic("cassandraMetadataPersistence do not support get metadata operation.")
}

func (m *cassandraMetadataPersistence) deleteDomain(name, ID string) error {
	query := m.session.Query(templateDeleteDomainByNameQuery, name)
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

func serializeClusterConfigs(replicationConfigs []*ClusterReplicationConfig) []map[string]interface{} {
	seriaizedReplicationConfigs := []map[string]interface{}{}
	for index := range replicationConfigs {
		seriaizedReplicationConfigs = append(seriaizedReplicationConfigs, replicationConfigs[index].serialize())
	}
	return seriaizedReplicationConfigs
}

func deserializeClusterConfigs(replicationConfigs []map[string]interface{}) []*ClusterReplicationConfig {
	deseriaizedReplicationConfigs := []*ClusterReplicationConfig{}
	for index := range replicationConfigs {
		deseriaizedReplicationConfig := &ClusterReplicationConfig{}
		deseriaizedReplicationConfig.deserialize(replicationConfigs[index])
		deseriaizedReplicationConfigs = append(deseriaizedReplicationConfigs, deseriaizedReplicationConfig)
	}
	return deseriaizedReplicationConfigs
}
