package sql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type userDataStore struct {
	SqlStore
}

func (uds *userDataStore) GetTaskQueueUserData(ctx context.Context, request *persistence.GetTaskQueueUserDataRequest) (*persistence.InternalGetTaskQueueUserDataResponse, error) {
	namespaceID, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternalf("failed to parse namespace ID as UUID: %v", err)
	}
	response, err := uds.DB.GetTaskQueueUserData(ctx, &sqlplugin.GetTaskQueueUserDataRequest{
		NamespaceID:   namespaceID,
		TaskQueueName: request.TaskQueue,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFoundf("task queue user data not found for %v.%v", request.NamespaceID, request.TaskQueue)
		}
		return nil, err
	}
	return &persistence.InternalGetTaskQueueUserDataResponse{
		Version:  response.Version,
		UserData: persistence.NewDataBlob(response.Data, response.DataEncoding),
	}, nil
}

// nolint:revive,cognitive-complexity // moving old code
func (uds *userDataStore) UpdateTaskQueueUserData(ctx context.Context, request *persistence.InternalUpdateTaskQueueUserDataRequest) error {
	namespaceID, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return serviceerror.NewInternalf("failed to parse namespace ID as UUID: %v", err)
	}
	err = uds.txExecute(ctx, "UpdateTaskQueueUserData", func(tx sqlplugin.Tx) error {
		for taskQueue, update := range request.Updates {
			err := tx.UpdateTaskQueueUserData(ctx, &sqlplugin.UpdateTaskQueueDataRequest{
				NamespaceID:   namespaceID,
				TaskQueueName: taskQueue,
				Data:          update.UserData.Data,
				DataEncoding:  update.UserData.EncodingType.String(),
				Version:       update.Version,
			})
			// note these are in a transaction: if one fails the others will be rolled back
			if uds.DB.IsDupEntryError(err) {
				err = &persistence.ConditionFailedError{Msg: err.Error()}
			}
			if persistence.IsConflictErr(err) && update.Conflicting != nil {
				*update.Conflicting = true
			}
			if err != nil {
				return err
			}
			if len(update.BuildIdsAdded) > 0 {
				err = tx.AddToBuildIdToTaskQueueMapping(ctx, sqlplugin.AddToBuildIdToTaskQueueMapping{
					NamespaceID:   namespaceID,
					TaskQueueName: taskQueue,
					BuildIds:      update.BuildIdsAdded,
				})
				if err != nil {
					return err
				}
			}
			if len(update.BuildIdsRemoved) > 0 {
				err = tx.RemoveFromBuildIdToTaskQueueMapping(ctx, sqlplugin.RemoveFromBuildIdToTaskQueueMapping{
					NamespaceID:   namespaceID,
					TaskQueueName: taskQueue,
					BuildIds:      update.BuildIdsRemoved,
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	// only set Applied if the whole transaction succeeded
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = err == nil
		}
	}
	return err
}

func (uds *userDataStore) ListTaskQueueUserDataEntries(ctx context.Context, request *persistence.ListTaskQueueUserDataEntriesRequest) (*persistence.InternalListTaskQueueUserDataEntriesResponse, error) {
	namespaceID, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	lastQueueName := ""
	if len(request.NextPageToken) != 0 {
		token, err := deserializePageTokenJson[userDataListNextPageToken](request.NextPageToken)
		if err != nil {
			return nil, err
		}
		lastQueueName = token.LastTaskQueueName
	}

	rows, err := uds.DB.ListTaskQueueUserDataEntries(ctx, &sqlplugin.ListTaskQueueUserDataEntriesRequest{
		NamespaceID:       namespaceID,
		LastTaskQueueName: lastQueueName,
		Limit:             request.PageSize,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailablef("ListTaskQueueUserDataEntries operation failed. Failed to get rows. Error: %v", err)
	}

	var nextPageToken []byte
	if len(rows) == request.PageSize {
		nextPageToken, err = serializePageTokenJson(&userDataListNextPageToken{LastTaskQueueName: rows[request.PageSize-1].TaskQueueName})
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
	}
	entries := make([]persistence.InternalTaskQueueUserDataEntry, len(rows))
	for i, row := range rows {
		entries[i].TaskQueue = rows[i].TaskQueueName
		entries[i].Data = persistence.NewDataBlob(row.Data, row.DataEncoding)
		entries[i].Version = rows[i].Version
	}
	response := &persistence.InternalListTaskQueueUserDataEntriesResponse{
		Entries:       entries,
		NextPageToken: nextPageToken,
	}

	return response, nil
}

func (uds *userDataStore) GetTaskQueuesByBuildId(ctx context.Context, request *persistence.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	namespaceID, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	return uds.DB.GetTaskQueuesByBuildId(ctx, &sqlplugin.GetTaskQueuesByBuildIdRequest{NamespaceID: namespaceID, BuildID: request.BuildID})
}

func (uds *userDataStore) CountTaskQueuesByBuildId(ctx context.Context, request *persistence.CountTaskQueuesByBuildIdRequest) (int, error) {
	namespaceID, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return 0, serviceerror.NewInternal(err.Error())
	}
	return uds.DB.CountTaskQueuesByBuildId(ctx, &sqlplugin.CountTaskQueuesByBuildIdRequest{NamespaceID: namespaceID, BuildID: request.BuildID})
}

func (uds *userDataStore) txExecute(ctx context.Context, operation string, f func(tx sqlplugin.Tx) error) error {
	tx, err := uds.DB.BeginTx(ctx)
	if err != nil {
		return serviceerror.NewUnavailablef("%s failed. Failed to start transaction. Error: %v", operation, err)
	}
	err = f(tx)
	if err != nil {
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			uds.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}

		switch err.(type) {
		case *persistence.ConditionFailedError,
			*persistence.CurrentWorkflowConditionFailedError,
			*persistence.WorkflowConditionFailedError,
			*serviceerror.NamespaceAlreadyExists,
			*persistence.ShardOwnershipLostError,
			*serviceerror.Unavailable,
			*serviceerror.NotFound:
			return err
		default:
			return serviceerror.NewUnavailablef("%v: %v", operation, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return serviceerror.NewUnavailablef("%s operation failed. Failed to commit transaction. Error: %v", operation, err)
	}
	return nil
}
