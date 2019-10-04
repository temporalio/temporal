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
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

type sqlMetadataManagerV2 struct {
	sqlStore
	activeClusterName string
}

// newMetadataPersistenceV2 creates an instance of sqlMetadataManagerV2
func newMetadataPersistenceV2(db sqldb.Interface, currentClusterName string,
	logger log.Logger) (persistence.MetadataStore, error) {
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

func (m *sqlMetadataManagerV2) CreateDomain(request *persistence.InternalCreateDomainRequest) (*persistence.CreateDomainResponse, error) {
	metadata, err := m.GetMetadata()
	if err != nil {
		return nil, err
	}

	clusters := make([]string, len(request.ReplicationConfig.Clusters))
	for i := range clusters {
		clusters[i] = request.ReplicationConfig.Clusters[i].ClusterName
	}

	var badBinaries []byte
	var badBinariesEncoding *string
	if request.Config.BadBinaries != nil {
		badBinaries = request.Config.BadBinaries.Data
		badBinariesEncoding = common.StringPtr(string(request.Config.BadBinaries.GetEncoding()))
	}
	domainInfo := &sqlblobs.DomainInfo{
		Status:                      common.Int32Ptr(int32(request.Info.Status)),
		Description:                 &request.Info.Description,
		Owner:                       &request.Info.OwnerEmail,
		Data:                        request.Info.Data,
		RetentionDays:               common.Int16Ptr(int16(request.Config.Retention)),
		EmitMetric:                  &request.Config.EmitMetric,
		ArchivalBucket:              &request.Config.ArchivalBucket,
		ArchivalStatus:              common.Int16Ptr(int16(request.Config.ArchivalStatus)),
		HistoryArchivalStatus:       common.Int16Ptr(int16(request.Config.HistoryArchivalStatus)),
		HistoryArchivalURI:          &request.Config.HistoryArchivalURI,
		VisibilityArchivalStatus:    common.Int16Ptr(int16(request.Config.VisibilityArchivalStatus)),
		VisibilityArchivalURI:       &request.Config.VisibilityArchivalURI,
		ActiveClusterName:           &request.ReplicationConfig.ActiveClusterName,
		Clusters:                    clusters,
		ConfigVersion:               common.Int64Ptr(request.ConfigVersion),
		FailoverVersion:             common.Int64Ptr(request.FailoverVersion),
		NotificationVersion:         common.Int64Ptr(metadata.NotificationVersion),
		FailoverNotificationVersion: common.Int64Ptr(persistence.InitialFailoverNotificationVersion),
		BadBinaries:                 badBinaries,
		BadBinariesEncoding:         badBinariesEncoding,
	}

	blob, err := domainInfoToBlob(domainInfo)
	if err != nil {
		return nil, err
	}

	var resp *persistence.CreateDomainResponse
	err = m.txExecute("CreateDomain", func(tx sqldb.Tx) error {
		if _, err1 := tx.InsertIntoDomain(&sqldb.DomainRow{
			Name:         request.Info.Name,
			ID:           sqldb.MustParseUUID(request.Info.ID),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
			IsGlobal:     request.IsGlobalDomain,
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

func (m *sqlMetadataManagerV2) GetDomain(request *persistence.GetDomainRequest) (*persistence.InternalGetDomainResponse, error) {
	filter := &sqldb.DomainFilter{}
	switch {
	case request.Name != "" && request.ID != "":
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name specified in request.",
		}
	case request.Name != "":
		filter.Name = &request.Name
	case request.ID != "":
		filter.ID = sqldb.UUIDPtr(sqldb.MustParseUUID(request.ID))
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

func (m *sqlMetadataManagerV2) domainRowToGetDomainResponse(row *sqldb.DomainRow) (*persistence.InternalGetDomainResponse, error) {
	domainInfo, err := domainInfoFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	clusters := make([]*persistence.ClusterReplicationConfig, len(domainInfo.Clusters))
	for i := range domainInfo.Clusters {
		clusters[i] = &persistence.ClusterReplicationConfig{ClusterName: domainInfo.Clusters[i]}
	}

	var badBinaries *persistence.DataBlob
	if domainInfo.BadBinaries != nil {
		badBinaries = persistence.NewDataBlob(domainInfo.BadBinaries, common.EncodingType(*domainInfo.BadBinariesEncoding))
	}

	return &persistence.InternalGetDomainResponse{
		Info: &persistence.DomainInfo{
			ID:          row.ID.String(),
			Name:        row.Name,
			Status:      int(domainInfo.GetStatus()),
			Description: domainInfo.GetDescription(),
			OwnerEmail:  domainInfo.GetOwner(),
			Data:        domainInfo.GetData(),
		},
		Config: &persistence.InternalDomainConfig{
			Retention:                int32(domainInfo.GetRetentionDays()),
			EmitMetric:               domainInfo.GetEmitMetric(),
			ArchivalBucket:           domainInfo.GetArchivalBucket(),
			ArchivalStatus:           workflow.ArchivalStatus(domainInfo.GetArchivalStatus()),
			HistoryArchivalStatus:    workflow.ArchivalStatus(domainInfo.GetHistoryArchivalStatus()),
			HistoryArchivalURI:       domainInfo.GetHistoryArchivalURI(),
			VisibilityArchivalStatus: workflow.ArchivalStatus(domainInfo.GetVisibilityArchivalStatus()),
			VisibilityArchivalURI:    domainInfo.GetVisibilityArchivalURI(),
			BadBinaries:              badBinaries,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: persistence.GetOrUseDefaultActiveCluster(m.activeClusterName, domainInfo.GetActiveClusterName()),
			Clusters:          persistence.GetOrUseDefaultClusters(m.activeClusterName, clusters),
		},
		IsGlobalDomain:              row.IsGlobal,
		FailoverVersion:             domainInfo.GetFailoverVersion(),
		ConfigVersion:               domainInfo.GetConfigVersion(),
		NotificationVersion:         domainInfo.GetNotificationVersion(),
		FailoverNotificationVersion: domainInfo.GetFailoverNotificationVersion(),
	}, nil
}

func (m *sqlMetadataManagerV2) UpdateDomain(request *persistence.InternalUpdateDomainRequest) error {
	clusters := make([]string, len(request.ReplicationConfig.Clusters))
	for i := range clusters {
		clusters[i] = request.ReplicationConfig.Clusters[i].ClusterName
	}

	var badBinaries []byte
	var badBinariesEncoding *string
	if request.Config.BadBinaries != nil {
		badBinaries = request.Config.BadBinaries.Data
		badBinariesEncoding = common.StringPtr(string(request.Config.BadBinaries.GetEncoding()))
	}
	domainInfo := &sqlblobs.DomainInfo{
		Status:                      common.Int32Ptr(int32(request.Info.Status)),
		Description:                 &request.Info.Description,
		Owner:                       &request.Info.OwnerEmail,
		Data:                        request.Info.Data,
		RetentionDays:               common.Int16Ptr(int16(request.Config.Retention)),
		EmitMetric:                  &request.Config.EmitMetric,
		ArchivalBucket:              &request.Config.ArchivalBucket,
		ArchivalStatus:              common.Int16Ptr(int16(request.Config.ArchivalStatus)),
		HistoryArchivalStatus:       common.Int16Ptr(int16(request.Config.HistoryArchivalStatus)),
		HistoryArchivalURI:          &request.Config.HistoryArchivalURI,
		VisibilityArchivalStatus:    common.Int16Ptr(int16(request.Config.VisibilityArchivalStatus)),
		VisibilityArchivalURI:       &request.Config.VisibilityArchivalURI,
		ActiveClusterName:           &request.ReplicationConfig.ActiveClusterName,
		Clusters:                    clusters,
		ConfigVersion:               common.Int64Ptr(request.ConfigVersion),
		FailoverVersion:             common.Int64Ptr(request.FailoverVersion),
		NotificationVersion:         common.Int64Ptr(request.NotificationVersion),
		FailoverNotificationVersion: common.Int64Ptr(request.FailoverNotificationVersion),
		BadBinaries:                 badBinaries,
		BadBinariesEncoding:         badBinariesEncoding,
	}

	blob, err := domainInfoToBlob(domainInfo)
	if err != nil {
		return err
	}

	return m.txExecute("UpdateDomain", func(tx sqldb.Tx) error {
		result, err := tx.UpdateDomain(&sqldb.DomainRow{
			Name:         request.Info.Name,
			ID:           sqldb.MustParseUUID(request.Info.ID),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
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
		_, err := tx.DeleteFromDomain(&sqldb.DomainFilter{ID: sqldb.UUIDPtr(sqldb.MustParseUUID(request.ID))})
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

func (m *sqlMetadataManagerV2) ListDomains(request *persistence.ListDomainsRequest) (*persistence.InternalListDomainsResponse, error) {
	var pageToken *sqldb.UUID
	if request.NextPageToken != nil {
		token := sqldb.UUID(request.NextPageToken)
		pageToken = &token
	}
	rows, err := m.db.SelectFromDomain(&sqldb.DomainFilter{
		GreaterThanID: pageToken,
		PageSize:      &request.PageSize,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return &persistence.InternalListDomainsResponse{}, nil
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListDomains operation failed. Failed to get domain rows. Error: %v", err),
		}
	}

	var domains []*persistence.InternalGetDomainResponse
	for _, row := range rows {
		resp, err := m.domainRowToGetDomainResponse(&row)
		if err != nil {
			return nil, err
		}
		domains = append(domains, resp)
	}

	resp := &persistence.InternalListDomainsResponse{Domains: domains}
	if len(rows) >= request.PageSize {
		resp.NextPageToken = rows[len(rows)-1].ID
	}

	return resp, nil
}
