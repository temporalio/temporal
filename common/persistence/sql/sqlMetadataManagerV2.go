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

	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
	"github.com/temporalio/temporal/common/primitives"
)

type sqlMetadataManagerV2 struct {
	sqlStore
	activeClusterName string
}

// newMetadataPersistenceV2 creates an instance of sqlMetadataManagerV2
func newMetadataPersistenceV2(db sqlplugin.DB, currentClusterName string,
	logger log.Logger) (persistence.MetadataStore, error) {
	return &sqlMetadataManagerV2{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
		activeClusterName: currentClusterName,
	}, nil
}

func updateMetadata(tx sqlplugin.Tx, oldNotificationVersion int64) error {
	result, err := tx.UpdateDomainMetadata(&sqlplugin.DomainMetadataRow{NotificationVersion: oldNotificationVersion})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update domain metadata. Error: %v", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Could not verify whether domain metadata update occurred. Error: %v", err))
	} else if rowsAffected != 1 {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update domain metadata. <>1 rows affected. Error: %v", err))
	}

	return nil
}

func lockMetadata(tx sqlplugin.Tx) error {
	err := tx.LockDomainMetadata()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to lock domain metadata. Error: %v", err))
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
	var badBinariesEncoding string
	if request.Config.BadBinaries != nil {
		badBinaries = request.Config.BadBinaries.Data
		badBinariesEncoding = string(request.Config.BadBinaries.GetEncoding())
	}
	domainInfo := &persistenceblobs.DomainInfo{
		Status:                      int32(request.Info.Status),
		Description:                 request.Info.Description,
		Owner:                       request.Info.OwnerEmail,
		Data:                        request.Info.Data,
		RetentionDays:               request.Config.Retention,
		EmitMetric:                  request.Config.EmitMetric,
		ArchivalBucket:              request.Config.ArchivalBucket,
		ArchivalStatus:              int32(request.Config.ArchivalStatus),
		HistoryArchivalStatus:       int32(request.Config.HistoryArchivalStatus),
		HistoryArchivalURI:          request.Config.HistoryArchivalURI,
		VisibilityArchivalStatus:    int32(request.Config.VisibilityArchivalStatus),
		VisibilityArchivalURI:       request.Config.VisibilityArchivalURI,
		ActiveClusterName:           request.ReplicationConfig.ActiveClusterName,
		Clusters:                    clusters,
		ConfigVersion:               request.ConfigVersion,
		FailoverVersion:             request.FailoverVersion,
		NotificationVersion:         metadata.NotificationVersion,
		FailoverNotificationVersion: persistence.InitialFailoverNotificationVersion,
		BadBinaries:                 badBinaries,
		BadBinariesEncoding:         badBinariesEncoding,
	}

	blob, err := serialization.DomainInfoToBlob(domainInfo)
	if err != nil {
		return nil, err
	}

	var resp *persistence.CreateDomainResponse
	err = m.txExecute("CreateDomain", func(tx sqlplugin.Tx) error {
		if _, err1 := tx.InsertIntoDomain(&sqlplugin.DomainRow{
			Name:         request.Info.Name,
			ID:           primitives.MustParseUUID(request.Info.ID),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
			IsGlobal:     request.IsGlobalDomain,
		}); err1 != nil {
			if m.db.IsDupEntryError(err1) {
				return serviceerror.NewDomainAlreadyExists(fmt.Sprintf("name: %v", request.Info.Name))
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
	filter := &sqlplugin.DomainFilter{}
	switch {
	case request.Name != "" && request.ID != "":
		return nil, serviceerror.NewInvalidArgument("GetDomain operation failed.  Both ID and Name specified in request.")
	case request.Name != "":
		filter.Name = &request.Name
	case request.ID != "":
		filter.ID = primitives.UUIDPtr(primitives.MustParseUUID(request.ID))
	default:
		return nil, serviceerror.NewInvalidArgument("GetDomain operation failed.  Both ID and Name are empty.")
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

			return nil, serviceerror.NewNotFound(fmt.Sprintf("Domain %s does not exist.", identity))
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("GetDomain operation failed. Error %v", err))
		}
	}

	response, err := m.domainRowToGetDomainResponse(&rows[0])
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (m *sqlMetadataManagerV2) domainRowToGetDomainResponse(row *sqlplugin.DomainRow) (*persistence.InternalGetDomainResponse, error) {
	domainInfo, err := serialization.DomainInfoFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	clusters := make([]*persistence.ClusterReplicationConfig, len(domainInfo.Clusters))
	for i := range domainInfo.Clusters {
		clusters[i] = &persistence.ClusterReplicationConfig{ClusterName: domainInfo.Clusters[i]}
	}

	var badBinaries *serialization.DataBlob
	if domainInfo.BadBinaries != nil {
		badBinaries = persistence.NewDataBlob(domainInfo.BadBinaries, common.EncodingType(domainInfo.BadBinariesEncoding))
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
			Retention:                domainInfo.GetRetentionDays(),
			EmitMetric:               domainInfo.GetEmitMetric(),
			ArchivalBucket:           domainInfo.GetArchivalBucket(),
			ArchivalStatus:           enums.ArchivalStatus(domainInfo.GetArchivalStatus()),
			HistoryArchivalStatus:    enums.ArchivalStatus(domainInfo.GetHistoryArchivalStatus()),
			HistoryArchivalURI:       domainInfo.GetHistoryArchivalURI(),
			VisibilityArchivalStatus: enums.ArchivalStatus(domainInfo.GetVisibilityArchivalStatus()),
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
	var badBinariesEncoding string
	if request.Config.BadBinaries != nil {
		badBinaries = request.Config.BadBinaries.Data
		badBinariesEncoding = string(request.Config.BadBinaries.GetEncoding())
	}
	domainInfo := &persistenceblobs.DomainInfo{
		Status:                      int32(request.Info.Status),
		Description:                 request.Info.Description,
		Owner:                       request.Info.OwnerEmail,
		Data:                        request.Info.Data,
		RetentionDays:               request.Config.Retention,
		EmitMetric:                  request.Config.EmitMetric,
		ArchivalBucket:              request.Config.ArchivalBucket,
		ArchivalStatus:              int32(request.Config.ArchivalStatus),
		HistoryArchivalStatus:       int32(request.Config.HistoryArchivalStatus),
		HistoryArchivalURI:          request.Config.HistoryArchivalURI,
		VisibilityArchivalStatus:    int32(request.Config.VisibilityArchivalStatus),
		VisibilityArchivalURI:       request.Config.VisibilityArchivalURI,
		ActiveClusterName:           request.ReplicationConfig.ActiveClusterName,
		Clusters:                    clusters,
		ConfigVersion:               request.ConfigVersion,
		FailoverVersion:             request.FailoverVersion,
		NotificationVersion:         request.NotificationVersion,
		FailoverNotificationVersion: request.FailoverNotificationVersion,
		BadBinaries:                 badBinaries,
		BadBinariesEncoding:         badBinariesEncoding,
	}

	blob, err := serialization.DomainInfoToBlob(domainInfo)
	if err != nil {
		return err
	}

	return m.txExecute("UpdateDomain", func(tx sqlplugin.Tx) error {
		result, err := tx.UpdateDomain(&sqlplugin.DomainRow{
			Name:         request.Info.Name,
			ID:           primitives.MustParseUUID(request.Info.ID),
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
	return m.txExecute("DeleteDomain", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteFromDomain(&sqlplugin.DomainFilter{ID: primitives.UUIDPtr(primitives.MustParseUUID(request.ID))})
		return err
	})
}

func (m *sqlMetadataManagerV2) DeleteDomainByName(request *persistence.DeleteDomainByNameRequest) error {
	return m.txExecute("DeleteDomainByName", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteFromDomain(&sqlplugin.DomainFilter{Name: &request.Name})
		return err
	})
}

func (m *sqlMetadataManagerV2) GetMetadata() (*persistence.GetMetadataResponse, error) {
	row, err := m.db.SelectFromDomainMetadata()
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetMetadata operation failed. Error: %v", err))
	}
	return &persistence.GetMetadataResponse{NotificationVersion: row.NotificationVersion}, nil
}

func (m *sqlMetadataManagerV2) ListDomains(request *persistence.ListDomainsRequest) (*persistence.InternalListDomainsResponse, error) {
	var pageToken *primitives.UUID
	if request.NextPageToken != nil {
		token := primitives.UUID(request.NextPageToken)
		pageToken = &token
	}
	rows, err := m.db.SelectFromDomain(&sqlplugin.DomainFilter{
		GreaterThanID: pageToken,
		PageSize:      &request.PageSize,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return &persistence.InternalListDomainsResponse{}, nil
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListDomains operation failed. Failed to get domain rows. Error: %v", err))
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
