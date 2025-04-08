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

package tidb

import (
	"context"
	"database/sql"
	"errors"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createIncomingServicesTableVersionQry    = `INSERT INTO nexus_incoming_services_partition_status(version) VALUES(1)`
	incrementIncomingServicesTableVersionQry = `UPDATE nexus_incoming_services_partition_status SET version = ? WHERE version = ?`
	getIncomingServicesTableVersionQry       = `SELECT version FROM nexus_incoming_services_partition_status`

	createIncomingServiceQry  = `INSERT INTO nexus_incoming_services(service_id, data, data_encoding, version) VALUES (?, ?, ?, 1)`
	updateIncomingServiceQry  = `UPDATE nexus_incoming_services SET data = ?, data_encoding = ?, version = ? WHERE service_id = ? AND version = ?`
	deleteIncomingServiceQry  = `DELETE FROM nexus_incoming_services WHERE service_id = ?`
	getIncomingServiceByIdQry = `SELECT service_id, data, data_encoding, version FROM nexus_incoming_services WHERE service_id = ?`
	getIncomingServicesQry    = `SELECT service_id, data, data_encoding, version FROM nexus_incoming_services WHERE service_id > ? LIMIT ?`
)

func (tidb *db) InitializeNexusIncomingServicesTableVersion(ctx context.Context) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx, createIncomingServicesTableVersionQry)
}

func (tidb *db) IncrementNexusIncomingServicesTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx, incrementIncomingServicesTableVersionQry, lastKnownTableVersion+1, lastKnownTableVersion)
}

func (tidb *db) GetNexusIncomingServicesTableVersion(ctx context.Context) (int64, error) {
	var version int64
	err := tidb.conn.GetContext(ctx, &version, getIncomingServicesTableVersionQry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (tidb *db) InsertIntoNexusIncomingServices(
	ctx context.Context,
	row *sqlplugin.NexusIncomingServicesRow,
) (sql.Result, error) {
	return tidb.conn.ExecContext(
		ctx,
		createIncomingServiceQry,
		row.ServiceID,
		row.Data,
		row.DataEncoding)
}

func (tidb *db) UpdateNexusIncomingService(
	ctx context.Context,
	row *sqlplugin.NexusIncomingServicesRow,
) (sql.Result, error) {
	return tidb.conn.ExecContext(
		ctx,
		updateIncomingServiceQry,
		row.Data,
		row.DataEncoding,
		row.Version+1,
		row.ServiceID,
		row.Version)
}

func (tidb *db) DeleteFromNexusIncomingServices(
	ctx context.Context,
	serviceID []byte,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx, deleteIncomingServiceQry, serviceID)
}

func (tidb *db) GetNexusIncomingServiceByID(
	ctx context.Context,
	serviceID []byte,
) (*sqlplugin.NexusIncomingServicesRow, error) {
	var row sqlplugin.NexusIncomingServicesRow
	err := tidb.conn.GetContext(ctx, &row, getIncomingServiceByIdQry, serviceID)
	return &row, err
}

func (tidb *db) ListNexusIncomingServices(
	ctx context.Context,
	request *sqlplugin.ListNexusIncomingServicesRequest,
) ([]sqlplugin.NexusIncomingServicesRow, error) {
	var rows []sqlplugin.NexusIncomingServicesRow
	err := tidb.conn.SelectContext(ctx, &rows, getIncomingServicesQry, request.LastServiceID, request.Limit)
	return rows, err
}
