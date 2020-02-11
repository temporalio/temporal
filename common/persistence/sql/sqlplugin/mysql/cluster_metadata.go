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
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package mysql

import (
	"database/sql"
	"strings"

	p "github.com/temporalio/temporal/common/persistence"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const constMetadataPartition = 0
const (
	// ****** CLUSTER_METADATA TABLE ******
	// One time only for the immutable data, so lets just use insert ignore
	insertClusterMetadataOneTimeOnlyQry = `INSERT IGNORE INTO 
cluster_metadata (metadata_partition, immutable_data, immutable_data_encoding)
VALUES(?, ?, ?)`

	getImmutableClusterMetadataQry = `SELECT immutable_data, immutable_data_encoding FROM 
cluster_metadata WHERE metadata_partition = ?`

	// ****** CLUSTER_MEMBERSHIP TABLE ******
	templateUpsertActiveClusterMembership = `REPLACE INTO
cluster_membership (host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry)
VALUES(?, ?, ?, ?, ?, ?, ?)`

	templatePruneStaleClusterMembership = `DELETE FROM
cluster_membership 
WHERE record_expiry < ? LIMIT ?`

	templateGetClusterMembership = `SELECT host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry, insertion_order FROM
cluster_membership`

	// ClusterMembership WHERE Suffixes
	templateWithRoleSuffix           = ` AND role = ?`
	templateWithHeartbeatSinceSuffix = ` AND last_heartbeat > ?`
	templateWithRecordExpirySuffix   = ` AND record_expiry > ?`
	templateWithRPCAddressSuffix     = ` AND rpc_address = ?`
	templateWithHostIDSuffix         = ` AND host_id = ?`
	templateWithSessionStartSuffix   = ` AND session_start > ?`
	templateWithInsertionOrderSuffix = ` AND insertion_order > ?`

	// Generic SELECT Suffixes
	templateWithLimitSuffix            = ` LIMIT ?`
	templateWithOrderByInsertionSuffix = ` ORDER BY insertion_order ASC`
)

// Does not follow traditional lock, select, read, insert as we only expect a single row.
func (mdb *db) InsertIfNotExistsIntoClusterMetadata(row *sqlplugin.ClusterMetadataRow) (sql.Result, error) {
	return mdb.conn.Exec(insertClusterMetadataOneTimeOnlyQry,
		constMetadataPartition,
		row.ImmutableData,
		row.ImmutableDataEncoding)
}

func (mdb *db) GetClusterMetadata() (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := mdb.conn.Get(&row, getImmutableClusterMetadataQry, constMetadataPartition)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (mdb *db) UpsertClusterMembership(row *sqlplugin.ClusterMembershipRow) (sql.Result, error) {
	return mdb.conn.Exec(templateUpsertActiveClusterMembership,
		row.HostID,
		row.RPCAddress,
		row.RPCPort,
		row.Role,
		mdb.converter.ToMySQLDateTime(row.SessionStart),
		mdb.converter.ToMySQLDateTime(row.LastHeartbeat),
		mdb.converter.ToMySQLDateTime(row.RecordExpiry))
}

func (mdb *db) GetClusterMembers(filter *sqlplugin.ClusterMembershipFilter) ([]sqlplugin.ClusterMembershipRow, error) {
	var queryString strings.Builder
	var operands []interface{}
	queryString.WriteString(templateGetClusterMembership)

	if filter.HostIDEquals != nil {
		queryString.WriteString(templateWithHostIDSuffix)
		operands = append(operands, filter.HostIDEquals)
	}

	if filter.RPCAddressEquals != "" {
		queryString.WriteString(templateWithRPCAddressSuffix)
		operands = append(operands, filter.RPCAddressEquals)
	}

	if filter.RoleEquals != p.All {
		queryString.WriteString(templateWithRoleSuffix)
		operands = append(operands, filter.RoleEquals)
	}

	if !filter.LastHeartbeatAfter.IsZero() {
		queryString.WriteString(templateWithHeartbeatSinceSuffix)
		operands = append(operands, filter.LastHeartbeatAfter)
	}

	if !filter.RecordExpiryAfter.IsZero() {
		queryString.WriteString(templateWithRecordExpirySuffix)
		operands = append(operands, filter.RecordExpiryAfter)
	}

	if !filter.SessionStartedAfter.IsZero() {
		queryString.WriteString(templateWithSessionStartSuffix)
		operands = append(operands, filter.SessionStartedAfter)
	}

	if filter.InsertionOrderGreaterThan > 0 {
		queryString.WriteString(templateWithInsertionOrderSuffix)
		operands = append(operands, filter.InsertionOrderGreaterThan)
	}

	queryString.WriteString(templateWithOrderByInsertionSuffix)

	if filter.MaxRecordCount > 0 {
		queryString.WriteString(templateWithLimitSuffix)
		operands = append(operands, filter.MaxRecordCount)
	}

	// All suffixes start with AND, replace the first occurrence with WHERE
	compiledQryString := strings.Replace(queryString.String(), " AND ", " WHERE ", 1)

	var rows []sqlplugin.ClusterMembershipRow
	err := mdb.conn.Select(&rows,
		compiledQryString,
		operands...)
	if err != nil {
		return nil, err
	}
	return rows, err
}

func (mdb *db) PruneClusterMembership(filter *sqlplugin.PruneClusterMembershipFilter) (sql.Result, error) {
	return mdb.conn.Exec(templatePruneStaleClusterMembership,
		mdb.converter.ToMySQLDateTime(filter.PruneRecordsBefore),
		filter.MaxRecordsAffected)
}
