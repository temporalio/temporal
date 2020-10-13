// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"database/sql"
	"strconv"
	"strings"

	p "go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const constMetadataPartition = 0
const constMembershipPartition = 0
const (
	// ****** CLUSTER_METADATA TABLE ******
	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	insertClusterMetadataQry = `INSERT INTO cluster_metadata (metadata_partition, data, data_encoding, immutable_data, immutable_data_encoding, version) VALUES($1, $2, $3, $4, $5, $6)`

	updateClusterMetadataQry = `UPDATE cluster_metadata SET data = $1, data_encoding = $2, version = $3 WHERE metadata_partition = $4`

	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	getClusterMetadataQry = `SELECT data, data_encoding, immutable_data, immutable_data_encoding, version FROM cluster_metadata WHERE metadata_partition = $1`

	writeLockGetClusterMetadataQry = getClusterMetadataQry + ` FOR UPDATE`

	// ****** CLUSTER_MEMBERSHIP TABLE ******
	templateUpsertActiveClusterMembership = `INSERT INTO
cluster_membership (membership_partition, host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry)
VALUES($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT(membership_partition, host_id)
DO UPDATE SET 
membership_partition = $1, host_id = $2, rpc_address = $3, rpc_port = $4, role = $5, session_start = $6, last_heartbeat = $7, record_expiry = $8`

	templatePruneStaleClusterMembership = `DELETE FROM
cluster_membership 
WHERE host_id = ANY(ARRAY(
SELECT host_id FROM cluster_membership WHERE membership_partition = $1 AND record_expiry < $2 LIMIT $3))`

	templateGetClusterMembership = `SELECT host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry FROM
cluster_membership WHERE membership_partition = $`

	// ClusterMembership WHERE Suffixes
	templateWithRoleSuffix           = ` AND role = $`
	templateWithHeartbeatSinceSuffix = ` AND last_heartbeat > $`
	templateWithRecordExpirySuffix   = ` AND record_expiry > $`
	templateWithRPCAddressSuffix     = ` AND rpc_address = $`
	templateWithHostIDSuffix         = ` AND host_id = $`
	templateWithHostIDGreaterSuffix  = ` AND host_id > $`
	templateWithSessionStartSuffix   = ` AND session_start >= $`

	// Generic SELECT Suffixes
	templateWithLimitSuffix               = ` LIMIT $`
	templateWithOrderBySessionStartSuffix = ` ORDER BY membership_partition ASC, host_id ASC`
)

func (pdb *db) SaveClusterMetadata(row *sqlplugin.ClusterMetadataRow) (sql.Result, error) {
	if row.Version == 0 {
		// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
		pdb.conn.Exec(insertClusterMetadataQry, constMetadataPartition, row.Data, row.DataEncoding, row.Data, row.DataEncoding, 1)
	}
	return pdb.conn.Exec(updateClusterMetadataQry, row.Data, row.DataEncoding, row.Version+1, constMetadataPartition)
}

func (pdb *db) GetClusterMetadata() (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := pdb.conn.Get(&row, getClusterMetadataQry, constMetadataPartition)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (pdb *db) WriteLockGetClusterMetadata() (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := pdb.conn.Get(&row, writeLockGetClusterMetadataQry, constMetadataPartition)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (pdb *db) UpsertClusterMembership(row *sqlplugin.ClusterMembershipRow) (sql.Result, error) {
	return pdb.conn.Exec(templateUpsertActiveClusterMembership,
		constMembershipPartition,
		row.HostID,
		row.RPCAddress,
		row.RPCPort,
		row.Role,
		row.SessionStart,
		row.LastHeartbeat,
		row.RecordExpiry)
}

func (pdb *db) GetClusterMembers(filter *sqlplugin.ClusterMembershipFilter) ([]sqlplugin.ClusterMembershipRow, error) {
	var queryString strings.Builder
	var operands []interface{}
	queryString.WriteString(templateGetClusterMembership)
	operands = append(operands, constMembershipPartition)
	queryString.WriteString(strconv.Itoa(len(operands)))

	if filter.HostIDEquals != nil {
		queryString.WriteString(templateWithHostIDSuffix)
		operands = append(operands, filter.HostIDEquals)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	if filter.RPCAddressEquals != "" {
		queryString.WriteString(templateWithRPCAddressSuffix)
		operands = append(operands, filter.RPCAddressEquals)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	if filter.RoleEquals != p.All {
		queryString.WriteString(templateWithRoleSuffix)
		operands = append(operands, filter.RoleEquals)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	if !filter.LastHeartbeatAfter.IsZero() {
		queryString.WriteString(templateWithHeartbeatSinceSuffix)
		operands = append(operands, filter.LastHeartbeatAfter)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	if !filter.RecordExpiryAfter.IsZero() {
		queryString.WriteString(templateWithRecordExpirySuffix)
		operands = append(operands, filter.RecordExpiryAfter)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	if !filter.SessionStartedAfter.IsZero() {
		queryString.WriteString(templateWithSessionStartSuffix)
		operands = append(operands, filter.SessionStartedAfter)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	if filter.HostIDGreaterThan != nil {
		queryString.WriteString(templateWithHostIDGreaterSuffix)
		operands = append(operands, filter.HostIDGreaterThan)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	queryString.WriteString(templateWithOrderBySessionStartSuffix)

	if filter.MaxRecordCount > 0 {
		queryString.WriteString(templateWithLimitSuffix)
		operands = append(operands, filter.MaxRecordCount)
		queryString.WriteString(strconv.Itoa(len(operands)))
	}

	compiledQryString := queryString.String()

	var rows []sqlplugin.ClusterMembershipRow
	err := pdb.conn.Select(&rows,
		compiledQryString,
		operands...)

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
		constMembershipPartition,
		filter.PruneRecordsBefore,
		filter.MaxRecordsAffected)
}
