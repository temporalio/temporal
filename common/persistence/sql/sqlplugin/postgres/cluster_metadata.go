// Copyright (c) 2020 Temporal Technologies, Inc.
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
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR

package postgres

import (
	"database/sql"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const constMetadataPartition = 0
const (
	// ****** CLUSTER_METADATA TABLE ******
	// One time only for the immutable data, so just use idempotent insert that does nothing if a record exists
	// This particular query requires PostgreSQL 9.5, PostgreSQL 9.4 comes out of LTS on February 13, 2020
	// QueryInfo: https://wiki.postgresql.org/wiki/What%27s_new_in_PostgreSQL_9.5#INSERT_..._ON_CONFLICT_DO_NOTHING.2FUPDATE_.28.22UPSERT.22.29
	insertClusterMetadataOneTimeOnlyQry = `INSERT INTO 
cluster_metadata (metadata_partition, immutable_data, immutable_data_encoding)
VALUES($1, $2, $3)
ON CONFLICT DO NOTHING`

	getImmutableClusterMetadataQry = `SELECT immutable_data, immutable_data_encoding FROM 
cluster_metadata WHERE metadata_partition = $1`

	// ****** CLUSTER_MEMBERSHIP TABLE ******
	templateUpsertActiveClusterMembership = `INSERT INTO
cluster_membership (host_id, rpc_address, session_start, last_heartbeat, record_expiry)
VALUES($1, $2, $3, $4, $5)
ON CONFLICT(host_id)
DO UPDATE SET 
host_id = $1, rpc_address = $2, session_start = $3, last_heartbeat = $4, record_expiry = $5`

	templatePruneStaleClusterMembership = `DELETE FROM
cluster_membership 
WHERE host_id = ANY(ARRAY(
SELECT host_id FROM cluster_membership WHERE record_expiry < $1 LIMIT $2))`

	templateGetActiveClusterMembership = `SELECT host_id, rpc_address, session_start, last_heartbeat, record_expiry 
FROM cluster_membership 
WHERE last_heartbeat > $1 AND record_expiry > $2`
)

// Does not follow traditional lock, select, read, insert as we only expect a single row.
func (pdb *db) InsertIfNotExistsIntoClusterMetadata(row *sqlplugin.ClusterMetadataRow) (sql.Result, error) {
	return pdb.conn.Exec(insertClusterMetadataOneTimeOnlyQry,
		constMetadataPartition,
		row.ImmutableData,
		row.ImmutableDataEncoding)
}

func (pdb *db) GetClusterMetadata() (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := pdb.conn.Get(&row, getImmutableClusterMetadataQry, constMetadataPartition)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (pdb *db) UpsertClusterMembership(row *sqlplugin.ClusterMembershipRow) (sql.Result, error) {
	return pdb.conn.Exec(templateUpsertActiveClusterMembership,
		row.HostID,
		row.RPCAddress,
		row.SessionStart,
		row.LastHeartbeat,
		row.RecordExpiry)
}

func (pdb *db) GetActiveClusterMembers(filter *sqlplugin.ClusterMembershipFilter) ([]sqlplugin.ClusterMembershipRow, error) {
	var rows []sqlplugin.ClusterMembershipRow
	err := pdb.conn.Select(&rows,
		templateGetActiveClusterMembership,
		filter.HeartbeatSince,
		filter.RecordExpiryCutoff)

	if err != nil {
		return nil, err
	}

	convertedRows := make([]sqlplugin.ClusterMembershipRow, 0, len(rows))
	for _, r := range rows {
		r.SessionStart = r.SessionStart.UTC()
		convertedRows = append(convertedRows, r)
	}
	return convertedRows, err
}

func (pdb *db) PruneClusterMembership(filter *sqlplugin.PruneClusterMembershipFilter) (sql.Result, error) {
	return pdb.conn.Exec(templatePruneStaleClusterMembership,
		filter.PruneRecordsBefore,
		filter.MaxRecordsAffected)
}
