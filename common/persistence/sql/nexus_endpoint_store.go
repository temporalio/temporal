package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

var (
	emptyID = make([]byte, 0)
)

type (
	sqlNexusEndpointStore struct {
		SqlStore
	}

	listEndpointsNextPageToken struct {
		LastID []byte
	}
)

func NewSqlNexusEndpointStore(
	db sqlplugin.DB,
	logger log.Logger,
) (p.NexusEndpointStore, error) {
	return &sqlNexusEndpointStore{
		SqlStore: NewSqlStore(db, logger),
	}, nil
}

func (s *sqlNexusEndpointStore) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *p.InternalCreateOrUpdateNexusEndpointRequest,
) error {
	id, retErr := primitives.ParseUUID(request.Endpoint.ID)
	if retErr != nil {
		return serviceerror.NewInternalf("unable to parse endpoint ID as UUID: %v", retErr)
	}

	retErr = s.txExecute(ctx, "CreateOrUpdateNexusEndpoint", func(tx sqlplugin.Tx) error {
		// Upsert table version row
		var result sql.Result
		var err error
		if request.LastKnownTableVersion == 0 {
			result, err = tx.InitializeNexusEndpointsTableVersion(ctx)
		} else {
			result, err = tx.IncrementNexusEndpointsTableVersion(ctx, request.LastKnownTableVersion)
		}

		err = checkUpdateResult(result, err, p.ErrNexusTableVersionConflict)
		if s.DB.IsDupEntryError(err) {
			return &p.ConditionFailedError{Msg: err.Error()}
		}
		if err != nil {
			s.logger.Error("error during CreateOrUpdateNexusEndpoint", tag.Error(err))
			return err
		}

		// Upsert Nexus endpoint row
		row := sqlplugin.NexusEndpointsRow{
			ID:           id,
			Version:      request.Endpoint.Version,
			Data:         request.Endpoint.Data.Data,
			DataEncoding: request.Endpoint.Data.EncodingType.String(),
		}
		if request.Endpoint.Version == 0 {
			result, err = tx.InsertIntoNexusEndpoints(ctx, &row)
		} else {
			result, err = tx.UpdateNexusEndpoint(ctx, &row)
		}
		err = checkUpdateResult(result, err, p.ErrNexusEndpointVersionConflict)
		if s.DB.IsDupEntryError(err) {
			return p.ErrNexusEndpointVersionConflict
		}

		return err
	})
	return retErr
}

func (s *sqlNexusEndpointStore) GetNexusEndpoint(
	ctx context.Context,
	request *p.GetNexusEndpointRequest,
) (*p.InternalNexusEndpoint, error) {
	id, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return nil, serviceerror.NewInternalf("unable to parse endpoint ID as UUID: %v", err)
	}

	row, err := s.DB.GetNexusEndpointByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, serviceerror.NewNotFoundf("Nexus endpoint with ID `%v` not found", request.ID)
		}
		s.logger.Error(fmt.Sprintf("error getting Nexus endpoint with ID %v", request.ID), tag.Error(err))
		return nil, serviceerror.NewUnavailable(err.Error())
	}

	return &p.InternalNexusEndpoint{
		ID:      request.ID,
		Version: row.Version,
		Data:    p.NewDataBlob(row.Data, row.DataEncoding),
	}, nil
}

func (s *sqlNexusEndpointStore) ListNexusEndpoints(
	ctx context.Context,
	request *p.ListNexusEndpointsRequest,
) (*p.InternalListNexusEndpointsResponse, error) {
	lastID := emptyID
	if len(request.NextPageToken) > 0 {
		token, err := deserializePageTokenJson[listEndpointsNextPageToken](request.NextPageToken)
		if err != nil {
			return nil, err
		}
		lastID = token.LastID
	}

	var response p.InternalListNexusEndpointsResponse
	var rows []sqlplugin.NexusEndpointsRow
	retErr := s.txExecute(ctx, "ListNexusEndpoints", func(tx sqlplugin.Tx) error {
		curTableVersion, err := tx.GetNexusEndpointsTableVersion(ctx)
		if err != nil {
			return err
		}
		response.TableVersion = curTableVersion
		if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != curTableVersion {
			return p.ErrNexusTableVersionConflict
		}

		if request.PageSize > 0 {
			// PageSize could be zero when fetching just the table version.
			rows, err = tx.ListNexusEndpoints(ctx, &sqlplugin.ListNexusEndpointsRequest{
				LastID: lastID,
				Limit:  request.PageSize,
			})
		}

		return err
	})

	if retErr != nil {
		return &response, retErr
	}

	var nextPageToken []byte
	if len(rows) > 0 && len(rows) == request.PageSize {
		// len(rows) could be zero when fetching just the table version.
		nextPageToken, retErr = serializePageTokenJson(&listEndpointsNextPageToken{
			LastID: rows[request.PageSize-1].ID,
		})
		if retErr != nil {
			s.logger.Error("error serializing next page token during ListNexusEndpoints", tag.Error(retErr))
			return nil, serviceerror.NewInternal(retErr.Error())
		}
	}
	response.NextPageToken = nextPageToken

	response.Endpoints = make([]p.InternalNexusEndpoint, len(rows))
	for i, row := range rows {
		response.Endpoints[i].ID = primitives.UUIDString(row.ID)
		response.Endpoints[i].Version = row.Version
		response.Endpoints[i].Data = p.NewDataBlob(row.Data, row.DataEncoding)
	}

	return &response, retErr
}

func (s *sqlNexusEndpointStore) DeleteNexusEndpoint(
	ctx context.Context,
	request *p.DeleteNexusEndpointRequest,
) error {
	id, retErr := primitives.ParseUUID(request.ID)
	if retErr != nil {
		return serviceerror.NewInternalf("unable to parse endpoint ID as UUID: %v", retErr)
	}

	retErr = s.txExecute(ctx, "DeleteNexusEndpoint", func(tx sqlplugin.Tx) error {
		result, err := tx.IncrementNexusEndpointsTableVersion(ctx, request.LastKnownTableVersion)
		err = checkUpdateResult(result, err, p.ErrNexusTableVersionConflict)
		if err != nil {
			s.logger.Error("error incrementing Nexus endpoints table version during DeleteNexusEndpoint call", tag.Error(err))
			return serviceerror.NewInternal(err.Error())
		}

		result, err = tx.DeleteFromNexusEndpoints(ctx, id)
		if err != nil {
			s.logger.Error("DeleteNexusEndpoint operation failed", tag.Error(err))
			return serviceerror.NewUnavailable(err.Error())
		}

		nRows, err := result.RowsAffected()
		if err != nil {
			s.logger.Error("error getting RowsAffected during DeleteNexusEndpoint", tag.Error(err))
			return serviceerror.NewUnavailablef("rowsAffected returned error: %v", err)
		}
		if nRows != 1 {
			return serviceerror.NewNotFoundf("nexus endpoint not found for ID: %v", request.ID)
		}

		return nil
	})
	return retErr
}

func checkUpdateResult(result sql.Result, pluginErr error, conflictErr error) error {
	if pluginErr != nil {
		return pluginErr
	}

	nRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nRows != 1 {
		return conflictErr
	}
	return nil
}
