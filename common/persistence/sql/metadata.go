package sql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type sqlMetadataManagerV2 struct {
	SqlStore
	activeClusterName string
}

// newMetadataPersistenceV2 creates an instance of sqlMetadataManagerV2
func newMetadataPersistenceV2(
	db sqlplugin.DB,
	currentClusterName string,
	logger log.Logger,
) (persistence.MetadataStore, error) {
	return &sqlMetadataManagerV2{
		SqlStore:          NewSqlStore(db, logger),
		activeClusterName: currentClusterName,
	}, nil
}

func (m *sqlMetadataManagerV2) CreateNamespace(
	ctx context.Context,
	request *persistence.InternalCreateNamespaceRequest,
) (*persistence.CreateNamespaceResponse, error) {
	idBytes, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return nil, err
	}

	var resp *persistence.CreateNamespaceResponse
	err = m.txExecute(ctx, "CreateNamespace", func(tx sqlplugin.Tx) error {
		metadata, err := lockMetadata(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.InsertIntoNamespace(ctx, &sqlplugin.NamespaceRow{
			Name:                request.Name,
			ID:                  idBytes,
			Data:                request.Namespace.Data,
			DataEncoding:        request.Namespace.EncodingType.String(),
			IsGlobal:            request.IsGlobal,
			NotificationVersion: metadata.NotificationVersion,
		}); err != nil {
			if m.DB.IsDupEntryError(err) {
				return serviceerror.NewNamespaceAlreadyExistsf("name: %v", request.Name)
			}
			return err
		}
		if err := updateMetadata(ctx,
			tx,
			metadata.NotificationVersion,
		); err != nil {
			return err
		}
		resp = &persistence.CreateNamespaceResponse{ID: request.ID}
		return nil
	})
	return resp, err
}

func (m *sqlMetadataManagerV2) GetNamespace(
	ctx context.Context,
	request *persistence.GetNamespaceRequest,
) (*persistence.InternalGetNamespaceResponse, error) {
	idBytes, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return nil, err
	}
	filter := sqlplugin.NamespaceFilter{}
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

	rows, err := m.DB.SelectFromNamespace(ctx, filter)
	if err != nil {
		switch err {
		case sql.ErrNoRows:
			// We did not return in the above for-loop because there were no rows.
			identity := request.Name
			if len(request.ID) > 0 {
				identity = request.ID
			}

			return nil, serviceerror.NewNamespaceNotFound(identity)
		default:
			return nil, serviceerror.NewUnavailablef("GetNamespace operation failed. Error %v", err)
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

func (m *sqlMetadataManagerV2) UpdateNamespace(
	ctx context.Context,
	request *persistence.InternalUpdateNamespaceRequest,
) error {
	return m.updateNamespace(ctx, request, "UpdateNamespace")
}

func (m *sqlMetadataManagerV2) RenameNamespace(
	ctx context.Context,
	request *persistence.InternalRenameNamespaceRequest,
) error {
	return m.updateNamespace(ctx, request.InternalUpdateNamespaceRequest, "RenameNamespace")
}

func (m *sqlMetadataManagerV2) updateNamespace(
	ctx context.Context,
	request *persistence.InternalUpdateNamespaceRequest,
	operationName string,
) error {
	idBytes, err := primitives.ParseUUID(request.Id)
	if err != nil {
		return err
	}

	return m.txExecute(ctx, operationName, func(tx sqlplugin.Tx) error {
		metadata, err := lockMetadata(ctx, tx)
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
		result, err := tx.UpdateNamespace(ctx, &sqlplugin.NamespaceRow{
			Name:                request.Name,
			ID:                  idBytes,
			Data:                request.Namespace.Data,
			DataEncoding:        request.Namespace.EncodingType.String(),
			NotificationVersion: request.NotificationVersion,
			IsGlobal:            request.IsGlobal,
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
		return updateMetadata(ctx, tx, metadata.NotificationVersion)
	})
}

func (m *sqlMetadataManagerV2) DeleteNamespace(
	ctx context.Context,
	request *persistence.DeleteNamespaceRequest,
) error {
	idBytes, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return err
	}

	return m.txExecute(ctx, "DeleteNamespace", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteFromNamespace(ctx, sqlplugin.NamespaceFilter{
			ID: &idBytes,
		})
		return err
	})
}

func (m *sqlMetadataManagerV2) DeleteNamespaceByName(
	ctx context.Context,
	request *persistence.DeleteNamespaceByNameRequest,
) error {
	return m.txExecute(ctx, "DeleteNamespaceByName", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteFromNamespace(ctx, sqlplugin.NamespaceFilter{
			Name: &request.Name,
		})
		return err
	})
}

func (m *sqlMetadataManagerV2) GetMetadata(
	ctx context.Context,
) (*persistence.GetMetadataResponse, error) {
	row, err := m.DB.SelectFromNamespaceMetadata(ctx)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetMetadata operation failed. Error: %v", err)
	}
	return &persistence.GetMetadataResponse{NotificationVersion: row.NotificationVersion}, nil
}

func (m *sqlMetadataManagerV2) ListNamespaces(
	ctx context.Context,
	request *persistence.InternalListNamespacesRequest,
) (*persistence.InternalListNamespacesResponse, error) {
	var pageToken *primitives.UUID
	if request.NextPageToken != nil {
		token := primitives.UUID(request.NextPageToken)
		pageToken = &token
	}
	rows, err := m.DB.SelectFromNamespace(ctx, sqlplugin.NamespaceFilter{
		GreaterThanID: pageToken,
		PageSize:      &request.PageSize,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return &persistence.InternalListNamespacesResponse{}, nil
		}
		return nil, serviceerror.NewUnavailablef("ListNamespaces operation failed. Failed to get namespace rows. Error: %v", err)
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

func updateMetadata(
	ctx context.Context,
	tx sqlplugin.Tx,
	oldNotificationVersion int64,
) error {
	result, err := tx.UpdateNamespaceMetadata(ctx, &sqlplugin.NamespaceMetadataRow{
		NotificationVersion: oldNotificationVersion,
	})
	if err != nil {
		return serviceerror.NewUnavailablef("Failed to update namespace metadata. Error: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailablef("Could not verify whether namespace metadata update occurred. Error: %v", err)
	} else if rowsAffected != 1 {
		return serviceerror.NewUnavailablef("Failed to update namespace metadata. <>1 rows affected. Error: %v", err)
	}

	return nil
}

func lockMetadata(
	ctx context.Context,
	tx sqlplugin.Tx,
) (*sqlplugin.NamespaceMetadataRow, error) {
	row, err := tx.LockNamespaceMetadata(ctx)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("Failed to lock namespace metadata. Error: %v", err)
	}
	return row, nil
}
