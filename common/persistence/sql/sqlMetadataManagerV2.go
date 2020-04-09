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

package sql

import (
	"database/sql"
	"fmt"

	namespacepb "go.temporal.io/temporal-proto/namespace"
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
	result, err := tx.UpdateNamespaceMetadata(&sqlplugin.NamespaceMetadataRow{NotificationVersion: oldNotificationVersion})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update namespace metadata. Error: %v", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Could not verify whether namespace metadata update occurred. Error: %v", err))
	} else if rowsAffected != 1 {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update namespace metadata. <>1 rows affected. Error: %v", err))
	}

	return nil
}

func lockMetadata(tx sqlplugin.Tx) error {
	err := tx.LockNamespaceMetadata()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to lock namespace metadata. Error: %v", err))
	}
	return nil
}

func (m *sqlMetadataManagerV2) CreateNamespace(request *persistence.InternalCreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
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
	namespaceInfo := &persistenceblobs.NamespaceInfo{
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

	blob, err := serialization.NamespaceInfoToBlob(namespaceInfo)
	if err != nil {
		return nil, err
	}

	var resp *persistence.CreateNamespaceResponse
	err = m.txExecute("CreateNamespace", func(tx sqlplugin.Tx) error {
		if _, err1 := tx.InsertIntoNamespace(&sqlplugin.NamespaceRow{
			Name:         request.Info.Name,
			ID:           primitives.MustParseUUID(request.Info.ID),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
			IsGlobal:     request.IsGlobalNamespace,
		}); err1 != nil {
			if m.db.IsDupEntryError(err1) {
				return serviceerror.NewNamespaceAlreadyExists(fmt.Sprintf("name: %v", request.Info.Name))
			}
			return err1
		}
		if err1 := lockMetadata(tx); err1 != nil {
			return err1
		}
		if err1 := updateMetadata(tx, metadata.NotificationVersion); err1 != nil {
			return err1
		}
		resp = &persistence.CreateNamespaceResponse{ID: request.Info.ID}
		return nil
	})
	return resp, err
}

func (m *sqlMetadataManagerV2) GetNamespace(request *persistence.GetNamespaceRequest) (*persistence.InternalGetNamespaceResponse, error) {
	filter := &sqlplugin.NamespaceFilter{}
	switch {
	case request.Name != "" && request.ID != "":
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name specified in request.")
	case request.Name != "":
		filter.Name = &request.Name
	case request.ID != "":
		filter.ID = primitives.UUIDPtr(primitives.MustParseUUID(request.ID))
	default:
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name are empty.")
	}

	rows, err := m.db.SelectFromNamespace(filter)
	if err != nil {
		switch err {
		case sql.ErrNoRows:
			// We did not return in the above for-loop because there were no rows.
			identity := request.Name
			if len(request.ID) > 0 {
				identity = request.ID
			}

			return nil, serviceerror.NewNotFound(fmt.Sprintf("Namespace %s does not exist.", identity))
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("GetNamespace operation failed. Error %v", err))
		}
	}

	response, err := m.namespaceRowToGetNamespaceResponse(&rows[0])
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (m *sqlMetadataManagerV2) namespaceRowToGetNamespaceResponse(row *sqlplugin.NamespaceRow) (*persistence.InternalGetNamespaceResponse, error) {
	namespaceInfo, err := serialization.NamespaceInfoFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	clusters := make([]*persistence.ClusterReplicationConfig, len(namespaceInfo.Clusters))
	for i := range namespaceInfo.Clusters {
		clusters[i] = &persistence.ClusterReplicationConfig{ClusterName: namespaceInfo.Clusters[i]}
	}

	var badBinaries *serialization.DataBlob
	if namespaceInfo.BadBinaries != nil {
		badBinaries = persistence.NewDataBlob(namespaceInfo.BadBinaries, common.EncodingType(namespaceInfo.BadBinariesEncoding))
	}

	return &persistence.InternalGetNamespaceResponse{
		Info: &persistence.NamespaceInfo{
			ID:          row.ID.String(),
			Name:        row.Name,
			Status:      int(namespaceInfo.GetStatus()),
			Description: namespaceInfo.GetDescription(),
			OwnerEmail:  namespaceInfo.GetOwner(),
			Data:        namespaceInfo.GetData(),
		},
		Config: &persistence.InternalNamespaceConfig{
			Retention:                namespaceInfo.GetRetentionDays(),
			EmitMetric:               namespaceInfo.GetEmitMetric(),
			ArchivalBucket:           namespaceInfo.GetArchivalBucket(),
			ArchivalStatus:           namespacepb.ArchivalStatus(namespaceInfo.GetArchivalStatus()),
			HistoryArchivalStatus:    namespacepb.ArchivalStatus(namespaceInfo.GetHistoryArchivalStatus()),
			HistoryArchivalURI:       namespaceInfo.GetHistoryArchivalURI(),
			VisibilityArchivalStatus: namespacepb.ArchivalStatus(namespaceInfo.GetVisibilityArchivalStatus()),
			VisibilityArchivalURI:    namespaceInfo.GetVisibilityArchivalURI(),
			BadBinaries:              badBinaries,
		},
		ReplicationConfig: &persistence.NamespaceReplicationConfig{
			ActiveClusterName: persistence.GetOrUseDefaultActiveCluster(m.activeClusterName, namespaceInfo.GetActiveClusterName()),
			Clusters:          persistence.GetOrUseDefaultClusters(m.activeClusterName, clusters),
		},
		IsGlobalNamespace:           row.IsGlobal,
		FailoverVersion:             namespaceInfo.GetFailoverVersion(),
		ConfigVersion:               namespaceInfo.GetConfigVersion(),
		NotificationVersion:         namespaceInfo.GetNotificationVersion(),
		FailoverNotificationVersion: namespaceInfo.GetFailoverNotificationVersion(),
	}, nil
}

func (m *sqlMetadataManagerV2) UpdateNamespace(request *persistence.InternalUpdateNamespaceRequest) error {
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
	namespaceInfo := &persistenceblobs.NamespaceInfo{
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

	blob, err := serialization.NamespaceInfoToBlob(namespaceInfo)
	if err != nil {
		return err
	}

	return m.txExecute("UpdateNamespace", func(tx sqlplugin.Tx) error {
		result, err := tx.UpdateNamespace(&sqlplugin.NamespaceRow{
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

func (m *sqlMetadataManagerV2) DeleteNamespace(request *persistence.DeleteNamespaceRequest) error {
	return m.txExecute("DeleteNamespace", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteFromNamespace(&sqlplugin.NamespaceFilter{ID: primitives.UUIDPtr(primitives.MustParseUUID(request.ID))})
		return err
	})
}

func (m *sqlMetadataManagerV2) DeleteNamespaceByName(request *persistence.DeleteNamespaceByNameRequest) error {
	return m.txExecute("DeleteNamespaceByName", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteFromNamespace(&sqlplugin.NamespaceFilter{Name: &request.Name})
		return err
	})
}

func (m *sqlMetadataManagerV2) GetMetadata() (*persistence.GetMetadataResponse, error) {
	row, err := m.db.SelectFromNamespaceMetadata()
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetMetadata operation failed. Error: %v", err))
	}
	return &persistence.GetMetadataResponse{NotificationVersion: row.NotificationVersion}, nil
}

func (m *sqlMetadataManagerV2) ListNamespaces(request *persistence.ListNamespacesRequest) (*persistence.InternalListNamespacesResponse, error) {
	var pageToken *primitives.UUID
	if request.NextPageToken != nil {
		token := primitives.UUID(request.NextPageToken)
		pageToken = &token
	}
	rows, err := m.db.SelectFromNamespace(&sqlplugin.NamespaceFilter{
		GreaterThanID: pageToken,
		PageSize:      &request.PageSize,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return &persistence.InternalListNamespacesResponse{}, nil
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListNamespaces operation failed. Failed to get namespace rows. Error: %v", err))
	}

	var namespaces []*persistence.InternalGetNamespaceResponse
	for _, row := range rows {
		resp, err := m.namespaceRowToGetNamespaceResponse(&row)
		if err != nil {
			return nil, err
		}
		namespaces = append(namespaces, resp)
	}

	resp := &persistence.InternalListNamespacesResponse{Namespaces: namespaces}
	if len(rows) >= request.PageSize {
		resp.NextPageToken = rows[len(rows)-1].ID
	}

	return resp, nil
}
