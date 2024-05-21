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

package postgresql

import (
	"context"
	"database/sql"
	"errors"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createEndpointsTableVersionQry    = `INSERT INTO nexus_endpoints_partition_status(version) VALUES(1)`
	incrementEndpointsTableVersionQry = `UPDATE nexus_endpoints_partition_status SET version = $1 WHERE version = $2`
	getEndpointsTableVersionQry       = `SELECT version FROM nexus_endpoints_partition_status`

	createEndpointQry  = `INSERT INTO nexus_endpoints(id, data, data_encoding, version) VALUES ($1, $2, $3, 1)`
	updateEndpointQry  = `UPDATE nexus_endpoints SET data = $1, data_encoding = $2, version = $3 WHERE id = $4 AND version = $5`
	deleteEndpointQry  = `DELETE FROM nexus_endpoints WHERE id = $1`
	getEndpointByIdQry = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id = $1`
	getEndpointsQry    = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id > $1 ORDER BY id LIMIT $2`
)

func (pdb *db) InitializeNexusEndpointsTableVersion(ctx context.Context) (sql.Result, error) {
	return pdb.ExecContext(ctx, createEndpointsTableVersionQry)
}

func (pdb *db) IncrementNexusEndpointsTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, incrementEndpointsTableVersionQry, lastKnownTableVersion+1, lastKnownTableVersion)
}

func (pdb *db) GetNexusEndpointsTableVersion(
	ctx context.Context,
) (int64, error) {
	var version int64
	err := pdb.GetContext(ctx, &version, getEndpointsTableVersionQry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (pdb *db) InsertIntoNexusEndpoints(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	return pdb.ExecContext(
		ctx,
		createEndpointQry,
		row.ID,
		row.Data,
		row.DataEncoding)
}

func (pdb *db) UpdateNexusEndpoint(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	return pdb.ExecContext(
		ctx,
		updateEndpointQry,
		row.Data,
		row.DataEncoding,
		row.Version+1,
		row.ID,
		row.Version)
}

func (pdb *db) DeleteFromNexusEndpoints(
	ctx context.Context,
	id []byte,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, deleteEndpointQry, id)
}

func (pdb *db) GetNexusEndpointByID(
	ctx context.Context,
	id []byte,
) (*sqlplugin.NexusEndpointsRow, error) {
	var row sqlplugin.NexusEndpointsRow
	err := pdb.GetContext(ctx, &row, getEndpointByIdQry, id)
	return &row, err
}

func (pdb *db) ListNexusEndpoints(
	ctx context.Context,
	request *sqlplugin.ListNexusEndpointsRequest,
) ([]sqlplugin.NexusEndpointsRow, error) {
	var rows []sqlplugin.NexusEndpointsRow
	err := pdb.SelectContext(ctx, &rows, getEndpointsQry, request.LastID, request.Limit)
	return rows, err
}
