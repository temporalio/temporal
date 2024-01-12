// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type (
	sqlNexusIncomingServiceStore struct {
		SqlStore
	}

	listIncomingServicesNextPageToken struct {
		LastServiceID []byte
	}
)

func NewSqlNexusIncomingServiceStore(
	db sqlplugin.DB,
	logger log.Logger,
) (p.NexusServiceStore, error) {
	return &sqlNexusIncomingServiceStore{
		SqlStore: NewSqlStore(db, logger),
	}, nil
}

func (s *sqlNexusIncomingServiceStore) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *p.InternalCreateOrUpdateNexusIncomingServiceRequest,
) error {
	serviceID, retErr := primitives.ParseUUID(request.Service.ServiceID)
	if retErr != nil {
		return serviceerror.NewInternal(fmt.Sprintf("unable to parse service ID as UUID: %v", retErr))
	}

	retErr = s.txExecute(ctx, "CreateOrUpdateNexusIncomingService", func(tx sqlplugin.Tx) error {
		// Upsert table version row
		var err error
		if request.LastKnownTableVersion == 0 {
			err = tx.InitializeNexusIncomingServicesTableVersion(ctx)
		} else {
			err = tx.IncrementNexusIncomingServicesTableVersion(ctx, request.LastKnownTableVersion)
		}
		if s.Db.IsDupEntryError(err) {
			return &p.ConditionFailedError{Msg: err.Error()}
		}
		if err != nil {
			s.logger.Error("error during CreateOrUpdateNexusIncomingService", tag.Error(err))
			return err
		}

		// Upsert Nexus incoming service row
		row := sqlplugin.NexusIncomingServicesRow{
			ServiceID:    serviceID,
			Version:      request.Service.Version,
			Data:         request.Service.Data.Data,
			DataEncoding: request.Service.Data.EncodingType.String(),
		}
		if request.Service.Version == 0 {
			err = tx.InsertIntoNexusIncomingServices(ctx, &row)
		} else {
			err = tx.UpdateNexusIncomingService(ctx, &row)
		}
		if s.Db.IsDupEntryError(err) {
			return p.ErrNexusIncomingServiceVersionConflict
		}

		return err
	})
	return retErr
}

func (s *sqlNexusIncomingServiceStore) ListNexusIncomingServices(
	ctx context.Context,
	request *p.InternalListNexusIncomingServicesRequest,
) (*p.InternalListNexusIncomingServicesResponse, error) {
	lastServiceID := make([]byte, 0)
	if len(request.NextPageToken) > 0 {
		token, err := deserializePageTokenJson[listIncomingServicesNextPageToken](request.NextPageToken)
		if err != nil {
			return nil, err
		}
		lastServiceID = token.LastServiceID
	}

	resp, err := s.Db.ListNexusIncomingServices(ctx, &sqlplugin.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		LastServiceID:         lastServiceID,
		Limit:                 request.PageSize,
	})
	var curTableVersion int64
	if resp != nil {
		curTableVersion = resp.CurrentTableVersion
	}
	if err != nil {
		if errors.Is(err, p.ErrNexusIncomingServiceVersionConflict) {
			// On table version conflict, return current table version and appropriate error
			return &p.InternalListNexusIncomingServicesResponse{TableVersion: curTableVersion}, err
		}
		// For all other errors, operation failed so return Unavailable error
		s.logger.Error("ListNexusIncomingServices operation failed", tag.Error(err))
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListNexusIncmoingServices operation failed: %v", err))
	}

	var nextPageToken []byte
	if len(resp.Entries) == request.PageSize {
		nextPageToken, err = serializePageTokenJson(&listIncomingServicesNextPageToken{
			LastServiceID: resp.Entries[request.PageSize-1].ServiceID,
		})
		if err != nil {
			s.logger.Error("error serializing next page token during ListNexusIncomingServices", tag.Error(err))
			return nil, serviceerror.NewInternal(err.Error())
		}
	}

	services := make([]p.InternalNexusIncomingService, len(resp.Entries))
	for i, entry := range resp.Entries {
		services[i].ServiceID = primitives.UUIDString(entry.ServiceID)
		services[i].Version = entry.Version
		services[i].Data = p.NewDataBlob(entry.Data, entry.DataEncoding)
	}

	return &p.InternalListNexusIncomingServicesResponse{
		TableVersion:  curTableVersion,
		Services:      services,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *sqlNexusIncomingServiceStore) DeleteNexusIncomingService(
	ctx context.Context,
	request *p.InternalDeleteNexusIncomingServiceRequest,
) error {
	serviceID, retErr := primitives.ParseUUID(request.ServiceID)
	if retErr != nil {
		return serviceerror.NewInternal(fmt.Sprintf("unable to parse service ID as UUID: %v", retErr))
	}

	retErr = s.txExecute(ctx, "DeleteNexusIncomingService", func(tx sqlplugin.Tx) error {
		err := tx.IncrementNexusIncomingServicesTableVersion(ctx, request.LastKnownTableVersion)
		if err != nil {
			s.logger.Error("error incrementing Nexus incoming services table version during DeleteNexusIncomingService call", tag.Error(err))
			return serviceerror.NewInternal(err.Error())
		}

		result, err := tx.DeleteFromNexusIncomingServices(ctx, serviceID)
		if err != nil {
			s.logger.Error("DeleteNexusIncomingService operation failed", tag.Error(err))
			return serviceerror.NewUnavailable(err.Error())
		}

		nRows, err := result.RowsAffected()
		if err != nil {
			s.logger.Error("error getting RowsAffected during DeleteNexusIncomingService", tag.Error(err))
			return serviceerror.NewUnavailable(fmt.Sprintf("rowsAffected returned error: %v", err))
		}
		if nRows != 1 {
			return p.ErrNexusIncomingServiceNotFound
		}

		return nil
	})
	return retErr
}
