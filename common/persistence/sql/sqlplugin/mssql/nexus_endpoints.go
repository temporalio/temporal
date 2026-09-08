package mssql

import (
	"context"
	"database/sql"
	"errors"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// The nexus_endpoints_partition_status table is restricted to a single row
	// (id = 0, applied by column DEFAULT and CHECK constraint), so the insert
	// only supplies the version.
	createEndpointsTableVersionQry    = `INSERT INTO nexus_endpoints_partition_status(version) VALUES(1)`
	incrementEndpointsTableVersionQry = `UPDATE nexus_endpoints_partition_status SET version = ? WHERE version = ?`
	getEndpointsTableVersionQry       = `SELECT version FROM nexus_endpoints_partition_status`

	createEndpointQry  = `INSERT INTO nexus_endpoints(id, data, data_encoding, version) VALUES (?, ?, ?, 1)`
	updateEndpointQry  = `UPDATE nexus_endpoints SET data = ?, data_encoding = ?, version = ? WHERE id = ? AND version = ?`
	deleteEndpointQry  = `DELETE FROM nexus_endpoints WHERE id = ?`
	getEndpointByIdQry = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id = ?`
	getEndpointsQry    = `SELECT TOP (?) id, data, data_encoding, version FROM nexus_endpoints WHERE id > ? ORDER BY id`
)

func (mdb *db) InitializeNexusEndpointsTableVersion(ctx context.Context) (sql.Result, error) {
	return mdb.ExecContext(ctx, createEndpointsTableVersionQry)
}

func (mdb *db) IncrementNexusEndpointsTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) (sql.Result, error) {
	return mdb.ExecContext(ctx, incrementEndpointsTableVersionQry, lastKnownTableVersion+1, lastKnownTableVersion)
}

func (mdb *db) GetNexusEndpointsTableVersion(
	ctx context.Context,
) (int64, error) {
	var version int64
	err := mdb.GetContext(ctx, &version, getEndpointsTableVersionQry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (mdb *db) InsertIntoNexusEndpoints(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	return mdb.ExecContext(
		ctx,
		createEndpointQry,
		row.ID,
		row.Data,
		row.DataEncoding)
}

func (mdb *db) UpdateNexusEndpoint(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	return mdb.ExecContext(
		ctx,
		updateEndpointQry,
		row.Data,
		row.DataEncoding,
		row.Version+1,
		row.ID,
		row.Version)
}

func (mdb *db) DeleteFromNexusEndpoints(
	ctx context.Context,
	id []byte,
) (sql.Result, error) {
	return mdb.ExecContext(ctx, deleteEndpointQry, id)
}

func (mdb *db) GetNexusEndpointByID(
	ctx context.Context,
	id []byte,
) (*sqlplugin.NexusEndpointsRow, error) {
	var row sqlplugin.NexusEndpointsRow
	err := mdb.GetContext(ctx, &row, getEndpointByIdQry, id)
	return &row, err
}

func (mdb *db) ListNexusEndpoints(
	ctx context.Context,
	request *sqlplugin.ListNexusEndpointsRequest,
) ([]sqlplugin.NexusEndpointsRow, error) {
	var rows []sqlplugin.NexusEndpointsRow
	err := mdb.SelectContext(ctx, &rows, getEndpointsQry, request.Limit, request.LastID)
	return rows, err
}
