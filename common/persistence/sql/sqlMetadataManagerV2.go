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

	"github.com/uber/cadence/common/persistence/sql/storage"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
	"github.com/uber/cadence/common/service/config"
)

type sqlMetadataManagerV2 struct {
	sqlStore
	activeClusterName string
}

// newMetadataPersistenceV2 creates an instance of sqlMetadataManagerV2
func newMetadataPersistenceV2(cfg config.SQL, currentClusterName string,
	logger bark.Logger) (persistence.MetadataManager, error) {
	var db, err = storage.NewSQLDB(&cfg)
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

func updateMetadata(tx sqldb.Tx, oldNotificationVersion int64) error {
	result, err := tx.UpdateDomainMetadata(&sqldb.DomainMetadataRow{NotificationVersion: oldNotificationVersion})
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

func lockMetadata(tx sqldb.Tx) error {
	err := tx.LockDomainMetadata()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock domain metadata. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlMetadataManagerV2) CreateDomain(request *persistence.CreateDomainRequest) (*persistence.CreateDomainResponse, error) {
	data, err := gobSerialize(request.Info.Data)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Failed to encode DomainInfo.Data. Error: %v", err),
		}
	}

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
	err = m.txExecute("CreateDomain", func(tx sqldb.Tx) error {
		if _, err1 := tx.InsertIntoDomain(&sqldb.DomainRow{
			Name:                        request.Info.Name,
			ID:                          request.Info.ID,
			Status:                      request.Info.Status,
			Description:                 request.Info.Description,
			OwnerEmail:                  request.Info.OwnerEmail,
			Data:                        data,
			Retention:                   int(request.Config.Retention),
			EmitMetric:                  request.Config.EmitMetric,
			ArchivalBucket:              request.Config.ArchivalBucket,
			ArchivalStatus:              int(request.Config.ArchivalStatus),
			ActiveClusterName:           request.ReplicationConfig.ActiveClusterName,
			Clusters:                    clusters,
			ConfigVersion:               request.ConfigVersion,
			FailoverVersion:             request.FailoverVersion,
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
	filter := &sqldb.DomainFilter{}
	switch {
	case request.Name != "" && request.ID != "":
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name specified in request.",
		}
	case request.Name != "":
		filter.Name = &request.Name
	case request.ID != "":
		filter.ID = &request.ID
	default:
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name are empty.",
		}
	}

	rows, err := m.db.SelectFromDomain(filter)
	if err != nil {
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

	response, err := m.domainRowToGetDomainResponse(&rows[0])
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (m *sqlMetadataManagerV2) domainRowToGetDomainResponse(row *sqldb.DomainRow) (*persistence.GetDomainResponse, error) {
	var data map[string]string
	if row.Data != nil {
		if err := gobDeserialize(row.Data, &data); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("Error in deserializing DomainInfo.Data. Error: %v", err),
			}
		}
	}

	var clusters []map[string]interface{}
	if row.Clusters != nil {
		if err := gobDeserialize(row.Clusters, &clusters); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("Error in deserializing ReplicationConfig.Clusters. Error: %v", err),
			}
		}
	}

	return &persistence.GetDomainResponse{
		TableVersion: persistence.DomainTableVersionV2,
		Info: &persistence.DomainInfo{
			ID:          row.ID,
			Name:        row.Name,
			Status:      row.Status,
			Description: row.Description,
			OwnerEmail:  row.OwnerEmail,
			Data:        data,
		},
		Config: &persistence.DomainConfig{
			Retention:      int32(row.Retention),
			EmitMetric:     row.EmitMetric,
			ArchivalBucket: row.ArchivalBucket,
			ArchivalStatus: workflow.ArchivalStatus(row.ArchivalStatus),
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: persistence.GetOrUseDefaultActiveCluster(m.activeClusterName, row.ActiveClusterName),
			Clusters:          persistence.GetOrUseDefaultClusters(m.activeClusterName, persistence.DeserializeClusterConfigs(clusters)),
		},
		IsGlobalDomain:              row.IsGlobalDomain,
		FailoverVersion:             row.FailoverVersion,
		ConfigVersion:               row.ConfigVersion,
		NotificationVersion:         row.NotificationVersion,
		FailoverNotificationVersion: row.FailoverNotificationVersion,
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

	return m.txExecute("UpdateDomain", func(tx sqldb.Tx) error {
		result, err := tx.UpdateDomain(&sqldb.DomainRow{
			Name:                        request.Info.Name,
			ID:                          request.Info.ID,
			Status:                      request.Info.Status,
			Description:                 request.Info.Description,
			OwnerEmail:                  request.Info.OwnerEmail,
			Data:                        data,
			Retention:                   int(request.Config.Retention),
			EmitMetric:                  request.Config.EmitMetric,
			ArchivalBucket:              request.Config.ArchivalBucket,
			ArchivalStatus:              int(request.Config.ArchivalStatus),
			ActiveClusterName:           request.ReplicationConfig.ActiveClusterName,
			Clusters:                    clusters,
			ConfigVersion:               request.ConfigVersion,
			FailoverVersion:             request.FailoverVersion,
			NotificationVersion:         request.NotificationVersion,
			FailoverNotificationVersion: request.FailoverNotificationVersion,
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
	return m.txExecute("DeleteDomain", func(tx sqldb.Tx) error {
		_, err := tx.DeleteFromDomain(&sqldb.DomainFilter{ID: &request.ID})
		return err
	})
}

func (m *sqlMetadataManagerV2) DeleteDomainByName(request *persistence.DeleteDomainByNameRequest) error {
	return m.txExecute("DeleteDomainByName", func(tx sqldb.Tx) error {
		_, err := tx.DeleteFromDomain(&sqldb.DomainFilter{Name: &request.Name})
		return err
	})
}

func (m *sqlMetadataManagerV2) GetMetadata() (*persistence.GetMetadataResponse, error) {
	row, err := m.db.SelectFromDomainMetadata()
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetMetadata operation failed. Error: %v", err),
		}
	}
	return &persistence.GetMetadataResponse{NotificationVersion: row.NotificationVersion}, nil
}

func (m *sqlMetadataManagerV2) ListDomains(request *persistence.ListDomainsRequest) (*persistence.ListDomainsResponse, error) {
	rows, err := m.db.SelectFromDomain(&sqldb.DomainFilter{})
	if err != nil {
		if err == sql.ErrNoRows {
			return &persistence.ListDomainsResponse{}, nil
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListDomains operation failed. Failed to get domain rows. Error: %v", err),
		}
	}

	var domains []*persistence.GetDomainResponse
	for _, row := range rows {
		resp, err := m.domainRowToGetDomainResponse(&row)
		if err != nil {
			return nil, err
		}
		domains = append(domains, resp)
	}

	return &persistence.ListDomainsResponse{Domains: domains}, nil
}
