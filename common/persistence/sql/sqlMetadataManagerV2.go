// Copyright (c) 2018 Uber Technologies, Inc.
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

package sql

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"

	"github.com/jmoiron/sqlx"
	"github.com/uber/cadence/common/service/config"
)

type (
	// Implements MetadataManager
	sqlMetadataManagerV2 struct {
		sqlStore
		activeClusterName string
	}

	domainCommon struct {
		// TODO Extracting the fields from DomainInfo since we don't support scanning into DomainInfo.Data
		ID          string
		Name        string
		Status      int
		Description string
		OwnerEmail  string
		Data        *[]byte

		persistence.DomainConfig
		// TODO Extracting the fields from DomainReplicationConfig since we don't currently support
		// TODO scanning into DomainReplicationConfig.Clusters
		//DomainReplicationConfig: *(request.ReplicationConfig),
		ActiveClusterName string
		Clusters          *[]byte
		ConfigVersion     int64
		FailoverVersion   int64
	}

	flatUpdateDomainRequest struct {
		domainCommon
		FailoverNotificationVersion int64
		NotificationVersion         int64
	}

	domainRow struct {
		domainCommon
		FailoverNotificationVersion int64
		NotificationVersion         int64
		IsGlobalDomain              bool
	}
)

const (
	createDomainSQLQuery = `INSERT INTO domains (
		id,
		name,
		retention, 
		emit_metric,
		config_version,
		status, 
		description, 
		owner_email,
		failover_version, 
		is_global_domain,
		active_cluster_name, 
		clusters, 
		notification_version,
		failover_notification_version,
		data
		)
		VALUES(
		:id,
		:name,
		:retention, 
		:emit_metric,
		:config_version,
		:status, 
		:description, 
		:owner_email,
		:failover_version, 
		:is_global_domain,
		:active_cluster_name, 
		:clusters,
		:notification_version,
		:failover_notification_version,
		:data
		)`

	getDomainPart = `SELECT
		id,
		retention, 
		emit_metric,
		config_version,
		name, 
		status, 
		description, 
		owner_email,
		failover_version, 
		is_global_domain,
		active_cluster_name, 
		clusters,
		notification_version,
		failover_notification_version,
		data
FROM domains
`
	getDomainByIDSQLQuery = getDomainPart +
		`WHERE id = :id`
	getDomainByNameSQLQuery = getDomainPart +
		`WHERE name = :name`

	updateDomainSQLQuery = `UPDATE domains
SET
		retention = :retention, 
		emit_metric = :emit_metric,
		config_version = :config_version,
		status = :status, 
		description = :description, 
		owner_email = :owner_email,
		failover_version = :failover_version, 
		active_cluster_name = :active_cluster_name,  
		clusters = :clusters,
		notification_version = :notification_version,
		failover_notification_version = :failover_notification_version,
		data = :data
WHERE
name = :name AND
id = :id`

	deleteDomainByIDSQLQuery   = `DELETE FROM domains WHERE id = :id`
	deleteDomainByNameSQLQuery = `DELETE FROM domains WHERE name = :name`

	listDomainsSQLQuery = getDomainPart

	getMetadataSQLQuery    = `SELECT notification_version FROM domain_metadata`
	lockMetadataSQLQuery   = `SELECT notification_version FROM domain_metadata FOR UPDATE`
	updateMetadataSQLQuery = `UPDATE domain_metadata
SET notification_version = :notification_version + 1 
WHERE notification_version = :notification_version`
)

func (m *sqlMetadataManagerV2) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

// newMetadataPersistenceV2 creates an instance of sqlMetadataManagerV2
func newMetadataPersistenceV2(cfg config.SQL, currentClusterName string,
	logger bark.Logger) (persistence.MetadataManager, error) {
	var db, err = newConnection(cfg)
	if err != nil {
		return nil, err
	}
	return &sqlMetadataManagerV2{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
		activeClusterName: currentClusterName,
	}, nil
}

func updateMetadata(tx *sqlx.Tx, oldNotificationVersion int64) error {
	result, err := tx.NamedExec(updateMetadataSQLQuery,
		struct {
			NotificationVersion int64
		}{oldNotificationVersion})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update domain metadata. Error: %v", err),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Could not verify whether domain metadata update occurred. Error: %v", err),
		}
	} else if rowsAffected != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update domain metadata. <>1 rows affected. Error: %v", err),
		}
	}

	return nil
}

func lockMetadata(tx *sqlx.Tx) error {
	var notificationVersion int
	err := tx.Get(&notificationVersion, lockMetadataSQLQuery)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock domain metadata. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlMetadataManagerV2) CreateDomain(request *persistence.CreateDomainRequest) (*persistence.CreateDomainResponse, error) {
	// Encode request.Info.Data
	data, err := gobSerialize(request.Info.Data)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Failed to encode DomainInfo.Data. Error: %v", err),
		}
	}

	// Encode request.ReplicationConfig.Clusters
	clusters, err := gobSerialize(persistence.SerializeClusterConfigs(request.ReplicationConfig.Clusters))
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Failed to encode ReplicationConfig.Clusters. Error: %v", err),
		}
	}

	metadata, err := m.GetMetadata()
	if err != nil {
		return nil, err
	}

	var resp *persistence.CreateDomainResponse
	err = m.txExecute("CreateDomain", func(tx *sqlx.Tx) error {
		if _, err1 := tx.NamedExec(createDomainSQLQuery, &domainRow{
			domainCommon: domainCommon{
				Name:        request.Info.Name,
				ID:          request.Info.ID,
				Status:      request.Info.Status,
				Description: request.Info.Description,
				OwnerEmail:  request.Info.OwnerEmail,
				Data:        &data,

				DomainConfig: *(request.Config),

				ActiveClusterName: request.ReplicationConfig.ActiveClusterName,
				Clusters:          &clusters,

				ConfigVersion:   request.ConfigVersion,
				FailoverVersion: request.FailoverVersion,
			},

			NotificationVersion:         metadata.NotificationVersion,
			FailoverNotificationVersion: persistence.InitialFailoverNotificationVersion,
			IsGlobalDomain:              request.IsGlobalDomain,
		}); err1 != nil {
			if sqlErr, ok := err1.(*mysql.MySQLError); ok && sqlErr.Number == ErrDupEntry {
				return &workflow.DomainAlreadyExistsError{
					Message: fmt.Sprintf("name: %v", request.Info.Name),
				}
			}
			return err1
		}
		if err1 := lockMetadata(tx); err1 != nil {
			return err1
		}
		if err1 := updateMetadata(tx, metadata.NotificationVersion); err1 != nil {
			return err1
		}
		resp = &persistence.CreateDomainResponse{ID: request.Info.ID}
		return nil
	})
	return resp, err
}

func (m *sqlMetadataManagerV2) GetDomain(request *persistence.GetDomainRequest) (*persistence.GetDomainResponse, error) {
	var err error
	var stmt *sqlx.NamedStmt

	if len(request.Name) > 0 {
		if len(request.ID) > 0 {
			return nil, &workflow.BadRequestError{
				Message: "GetDomain operation failed.  Both ID and Name specified in request.",
			}
		}
		stmt, err = m.db.PrepareNamed(getDomainByNameSQLQuery)
	} else if len(request.ID) > 0 {
		stmt, err = m.db.PrepareNamed(getDomainByIDSQLQuery)
	} else {
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name are empty.",
		}
	}

	if err != nil {
		return nil, err
	}

	var result domainRow
	if err = stmt.Get(&result, request); err != nil {
		switch err {
		case sql.ErrNoRows:
			// We did not return in the above for-loop because there were no rows.
			identity := request.Name
			if len(request.ID) > 0 {
				identity = request.ID
			}

			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Domain %s does not exist.", identity),
			}
		default:
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetDomain operation failed. Error %v", err),
			}

		}
	}

	response, err := m.domainRowToGetDomainResponse(&result)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (m *sqlMetadataManagerV2) domainRowToGetDomainResponse(result *domainRow) (*persistence.GetDomainResponse, error) {
	var data map[string]string
	if result.Data != nil {
		if err := gobDeserialize(*result.Data, &data); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("Error in deserializing DomainInfo.Data. Error: %v", err),
			}
		}
	}

	var clusters []map[string]interface{}
	if result.Clusters != nil {
		if err := gobDeserialize(*result.Clusters, &clusters); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("Error in deserializing ReplicationConfig.Clusters. Error: %v", err),
			}
		}
	}

	return &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID:          result.ID,
			Name:        result.Name,
			Status:      result.Status,
			Description: result.Description,
			OwnerEmail:  result.OwnerEmail,
			Data:        data,
		},
		Config: &result.DomainConfig,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: persistence.GetOrUseDefaultActiveCluster(m.activeClusterName, result.ActiveClusterName),
			Clusters:          persistence.GetOrUseDefaultClusters(m.activeClusterName, persistence.DeserializeClusterConfigs(clusters)),
		},
		IsGlobalDomain:              result.IsGlobalDomain,
		FailoverVersion:             result.FailoverVersion,
		ConfigVersion:               result.ConfigVersion,
		NotificationVersion:         result.NotificationVersion,
		FailoverNotificationVersion: result.FailoverNotificationVersion,
	}, nil
}

func (m *sqlMetadataManagerV2) UpdateDomain(request *persistence.UpdateDomainRequest) error {
	clusters, err := gobSerialize(persistence.SerializeClusterConfigs(request.ReplicationConfig.Clusters))
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Failed to encode ReplicationConfig.Clusters. Value: %v", request.ReplicationConfig.Clusters),
		}
	}

	data, err := gobSerialize(request.Info.Data)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Failed to encode DomainInfo.Data. Value: %v", request.Info.Data),
		}
	}

	return m.txExecute("UpdateDomain", func(tx *sqlx.Tx) error {
		result, err := tx.NamedExec(updateDomainSQLQuery, &flatUpdateDomainRequest{
			domainCommon: domainCommon{
				Name:        request.Info.Name,
				ID:          request.Info.ID,
				Status:      request.Info.Status,
				Description: request.Info.Description,
				OwnerEmail:  request.Info.OwnerEmail,
				Data:        &data,

				DomainConfig: *(request.Config),

				ActiveClusterName: request.ReplicationConfig.ActiveClusterName,
				Clusters:          &clusters,
				ConfigVersion:     request.ConfigVersion,
				FailoverVersion:   request.FailoverVersion,
			},
			FailoverNotificationVersion: request.FailoverNotificationVersion,
			NotificationVersion:         request.NotificationVersion,
		})
		if err != nil {
			return err
		}
		noRowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected error: %v", err)
		}
		if noRowsAffected != 1 {
			return fmt.Errorf("%v rows updated instead of one", noRowsAffected)
		}
		if err := lockMetadata(tx); err != nil {
			return err
		}
		return updateMetadata(tx, request.NotificationVersion)
	})
}

func (m *sqlMetadataManagerV2) DeleteDomain(request *persistence.DeleteDomainRequest) error {
	return m.txExecute("DeleteDomain", func(tx *sqlx.Tx) error {
		_, err := tx.NamedExec(deleteDomainByIDSQLQuery, request)
		return err
	})
}

func (m *sqlMetadataManagerV2) DeleteDomainByName(request *persistence.DeleteDomainByNameRequest) error {
	return m.txExecute("DeleteDomainByName", func(tx *sqlx.Tx) error {
		_, err := m.db.NamedExec(deleteDomainByNameSQLQuery, request)
		return err
	})
}

func (m *sqlMetadataManagerV2) GetMetadata() (*persistence.GetMetadataResponse, error) {
	var notificationVersion int64
	row := m.db.QueryRow(getMetadataSQLQuery)
	if err := row.Scan(&notificationVersion); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetMetadata operation failed. Error: %v", err),
		}
	}
	return &persistence.GetMetadataResponse{NotificationVersion: notificationVersion}, nil
}

func (m *sqlMetadataManagerV2) ListDomains(request *persistence.ListDomainsRequest) (*persistence.ListDomainsResponse, error) {
	rows, err := m.db.Queryx(listDomainsSQLQuery)
	if err != nil {
		if err == sql.ErrNoRows {
			return &persistence.ListDomainsResponse{}, nil
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListDomains operation failed. Failed to get domain rows. Error: %v", err),
		}
	}

	var domains []*persistence.GetDomainResponse

	for i := 0; rows.Next(); i++ {
		var row domainRow
		if err := rows.StructScan(&row); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("ListDomains operation failed. Failed to scan domain row. Error: %v", err),
			}
		}

		resp, err := m.domainRowToGetDomainResponse(&row)
		if err != nil {
			return nil, err
		}

		domains = append(domains, resp)
	}

	if err := rows.Err(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListDomains operation failed. Failed while iterating through domain rows. Error: %v", err),
		}
	}

	return &persistence.ListDomainsResponse{
		Domains: domains,
	}, nil
}
