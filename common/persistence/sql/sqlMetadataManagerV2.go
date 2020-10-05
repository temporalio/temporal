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

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
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

func lockMetadata(tx sqlplugin.Tx) (*sqlplugin.NamespaceMetadataRow, error) {
	row, err := tx.LockNamespaceMetadata()
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to lock namespace metadata. Error: %v", err))
	}
	return row, nil
}

func (m *sqlMetadataManagerV2) CreateNamespace(request *persistence.InternalCreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
	idBytes, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return nil, err
	}

	var resp *persistence.CreateNamespaceResponse
	err = m.txExecute("CreateNamespace", func(tx sqlplugin.Tx) error {
		metadata, err := lockMetadata(tx)
		if err != nil {
			return err
		}
		if _, err := tx.InsertIntoNamespace(&sqlplugin.NamespaceRow{
			Name:                request.Name,
			ID:                  idBytes,
			Data:                request.Namespace.Data,
			DataEncoding:        request.Namespace.Encoding.String(),
			IsGlobal:            request.IsGlobal,
			NotificationVersion: metadata.NotificationVersion,
		}); err != nil {
			if m.db.IsDupEntryError(err) {
				return serviceerror.NewNamespaceAlreadyExists(fmt.Sprintf("name: %v", request.Name))
			}
			return err
		}
		if err := updateMetadata(tx, metadata.NotificationVersion); err != nil {
			return err
		}
		resp = &persistence.CreateNamespaceResponse{ID: request.ID}
		return nil
	})
	return resp, err
}

func (m *sqlMetadataManagerV2) GetNamespace(request *persistence.GetNamespaceRequest) (*persistence.InternalGetNamespaceResponse, error) {
	idBytes, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return nil, err
	}
	filter := &sqlplugin.NamespaceFilter{}
	switch {
	case request.Name != "" && request.ID != "":
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name specified in request.")
	case request.Name != "":
		filter.Name = &request.Name
	case len(request.ID) != 0:
		filter.ID = &idBytes
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
	return &persistence.InternalGetNamespaceResponse{
		Namespace:           persistence.NewDataBlob(row.Data, row.DataEncoding),
		IsGlobal:            row.IsGlobal,
		NotificationVersion: row.NotificationVersion,
	}, nil
}

func (m *sqlMetadataManagerV2) UpdateNamespace(request *persistence.InternalUpdateNamespaceRequest) error {
	idBytes, err := primitives.ParseUUID(request.Id)
	if err != nil {
		return err
	}

	return m.txExecute("UpdateNamespace", func(tx sqlplugin.Tx) error {
		metadata, err := lockMetadata(tx)
		if err != nil {
			return err
		}
		if metadata.NotificationVersion != request.NotificationVersion {
			return fmt.Errorf(
				"conditional update error: expect: %v, actual: %v",
				request.NotificationVersion,
				metadata.NotificationVersion,
			)
		}
		result, err := tx.UpdateNamespace(&sqlplugin.NamespaceRow{
			Name:                request.Name,
			ID:                  idBytes,
			Data:                request.Namespace.Data,
			DataEncoding:        request.Namespace.Encoding.String(),
			NotificationVersion: request.NotificationVersion,
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
		return updateMetadata(tx, metadata.NotificationVersion)
	})
}

func (m *sqlMetadataManagerV2) DeleteNamespace(request *persistence.DeleteNamespaceRequest) error {
	idBytes, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return err
	}

	return m.txExecute("DeleteNamespace", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteFromNamespace(&sqlplugin.NamespaceFilter{ID: &idBytes})
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
