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

package mysql

import (
	"context"
	"database/sql"
	"errors"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createIncomingServicesTableVersionQry    = `INSERT INTO nexus_services_table_versions(service_type, version) VALUES(0, 1)`
	incrementIncomingServicesTableVersionQry = `UPDATE nexus_services_table_versions SET version = ? WHERE service_type = 0 AND version = ?`
	getIncomingServicesTableVersionQry       = `SELECT version FROM nexus_services_table_versions WHERE service_type = 0 LIMIT 1`

	createIncomingServiceQry = `INSERT INTO nexus_incoming_services(service_id, data, data_encoding, version) VALUES (?, ?, ?, 1)`
	updateIncomingServiceQry = `UPDATE nexus_incoming_services SET data = ?, data_encoding = ?, version = ? WHERE service_id = ? AND version = ?`
	deleteIncomingServiceQry = `DELETE FROM nexus_incoming_services WHERE service_id = ?`
	getIncomingServicesQry   = `SELECT service_id, data, data_encoding, version FROM nexus_incoming_services WHERE service_id > ? LIMIT ?`
)

func (mdb *db) InitializeNexusIncomingServicesTableVersion(ctx context.Context) error {
	result, err := mdb.conn.ExecContext(ctx, createIncomingServicesTableVersionQry)
	if err != nil {
		return err
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nRows != 1 {
		return persistence.ErrNexusTableVersionConflict
	}
	return nil
}

func (mdb *db) IncrementNexusIncomingServicesTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) error {
	result, err := mdb.conn.ExecContext(ctx, incrementIncomingServicesTableVersionQry, lastKnownTableVersion+1, lastKnownTableVersion)
	if err != nil {
		return err
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nRows != 1 {
		return persistence.ErrNexusTableVersionConflict
	}
	return nil
}

func (mdb *db) GetNexusIncomingServicesTableVersion(ctx context.Context) (int64, error) {
	version := int64(-1)
	err := mdb.conn.GetContext(ctx, &version, getIncomingServicesTableVersionQry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (mdb *db) InsertIntoNexusIncomingServices(
	ctx context.Context,
	row *sqlplugin.NexusIncomingServicesRow,
) error {
	result, err := mdb.conn.ExecContext(
		ctx,
		createIncomingServiceQry,
		row.ServiceID,
		row.Data,
		row.DataEncoding)
	if err != nil {
		return err
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nRows != 1 {
		return persistence.ErrNexusTableVersionConflict
	}
	return err
}

func (mdb *db) UpdateNexusIncomingService(
	ctx context.Context,
	row *sqlplugin.NexusIncomingServicesRow,
) error {
	result, err := mdb.conn.ExecContext(
		ctx,
		updateIncomingServiceQry,
		row.Data,
		row.DataEncoding,
		row.Version+1,
		row.ServiceID,
		row.Version)
	if err != nil {
		return err
	}

	nRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nRows != 1 {
		return persistence.ErrNexusIncomingServiceVersionConflict
	}

	return nil
}

func (mdb *db) DeleteFromNexusIncomingServices(
	ctx context.Context,
	serviceID []byte,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx, deleteIncomingServiceQry, serviceID)
}

func (mdb *db) ListNexusIncomingServices(
	ctx context.Context,
	request *sqlplugin.ListNexusIncomingServicesRequest,
) ([]sqlplugin.NexusIncomingServicesRow, error) {
	var rows []sqlplugin.NexusIncomingServicesRow
	err := mdb.conn.SelectContext(ctx, &rows, getIncomingServicesQry, request.LastServiceID, request.Limit)
	return rows, err
}
