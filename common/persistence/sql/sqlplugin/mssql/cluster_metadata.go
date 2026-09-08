package mssql

import (
	"context"
	"database/sql"
	"strings"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const constMetadataPartition = 0
const constMembershipPartition = 0
const (
	// ****** CLUSTER_METADATA_INFO TABLE ******
	insertClusterMetadataQry = `INSERT INTO cluster_metadata_info (metadata_partition, cluster_name, data, data_encoding, version) VALUES(?, ?, ?, ?, ?)`

	updateClusterMetadataQry = `UPDATE cluster_metadata_info SET data = ?, data_encoding = ?, version = ? WHERE metadata_partition = ? AND cluster_name = ?`

	getClusterMetadataBase      = `SELECT data, data_encoding, version FROM cluster_metadata_info `
	listClusterMetadataQry      = getClusterMetadataBase + `WHERE metadata_partition = ? ORDER BY cluster_name OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY`
	listClusterMetadataRangeQry = getClusterMetadataBase + `WHERE metadata_partition = ? AND cluster_name > ? ORDER BY cluster_name OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY`
	getClusterMetadataQry       = getClusterMetadataBase + `WHERE metadata_partition = ? AND cluster_name = ?`

	// FOR UPDATE equivalent: UPDLOCK holds the row lock until the enclosing
	// transaction ends. The hint must follow the table name, so this query is
	// spelled out rather than derived from getClusterMetadataQry.
	writeLockGetClusterMetadataQry = `SELECT data, data_encoding, version FROM cluster_metadata_info WITH (UPDLOCK, ROWLOCK) WHERE metadata_partition = ? AND cluster_name = ?`

	deleteClusterMetadataQry = `DELETE FROM cluster_metadata_info WHERE metadata_partition = ? AND cluster_name = ?`

	// ****** CLUSTER_MEMBERSHIP TABLE ******
	// Like mysql's ON DUPLICATE KEY UPDATE, only the session_start,
	// last_heartbeat and record_expiry columns change on conflict.
	templateUpsertActiveClusterMembership = `MERGE cluster_membership WITH (HOLDLOCK) AS target
USING (SELECT ? AS membership_partition, ? AS host_id, ? AS rpc_address, ? AS rpc_port, ? AS role, ? AS session_start, ? AS last_heartbeat, ? AS record_expiry) AS source
ON target.membership_partition = source.membership_partition AND target.host_id = source.host_id
WHEN MATCHED THEN UPDATE SET session_start = source.session_start, last_heartbeat = source.last_heartbeat, record_expiry = source.record_expiry
WHEN NOT MATCHED THEN INSERT (membership_partition, host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry)
VALUES (source.membership_partition, source.host_id, source.rpc_address, source.rpc_port, source.role, source.session_start, source.last_heartbeat, source.record_expiry);`

	templatePruneStaleClusterMembership = `DELETE FROM
cluster_membership
WHERE membership_partition = ? AND record_expiry < ?`

	templateGetClusterMembership = `SELECT host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry FROM
cluster_membership WHERE membership_partition = ?`

	// ClusterMembership WHERE Suffixes
	templateWithRoleSuffix           = ` AND role = ?`
	templateWithHeartbeatSinceSuffix = ` AND last_heartbeat > ?`
	templateWithRecordExpirySuffix   = ` AND record_expiry > ?`
	templateWithRPCAddressSuffix     = ` AND rpc_address = ?`
	templateWithHostIDSuffix         = ` AND host_id = ?`
	templateWithHostIDGreaterSuffix  = ` AND host_id > ?`
	templateWithSessionStartSuffix   = ` AND session_start >= ?`

	// Generic SELECT Suffixes. OFFSET/FETCH is T-SQL's LIMIT; it requires an
	// ORDER BY, which templateWithOrderBySessionStartSuffix always provides.
	templateWithLimitSuffix               = ` OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY`
	templateWithOrderBySessionStartSuffix = ` ORDER BY membership_partition ASC, host_id ASC`
)

func (mdb *db) SaveClusterMetadata(
	ctx context.Context,
	row *sqlplugin.ClusterMetadataRow,
) (sql.Result, error) {
	if row.Version == 0 {
		return mdb.ExecContext(ctx,
			insertClusterMetadataQry,
			constMetadataPartition,
			row.ClusterName,
			row.Data,
			row.DataEncoding,
			1,
		)
	}
	return mdb.ExecContext(ctx,
		updateClusterMetadataQry,
		row.Data,
		row.DataEncoding,
		row.Version+1,
		constMetadataPartition,
		row.ClusterName,
	)
}

func (mdb *db) ListClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) ([]sqlplugin.ClusterMetadataRow, error) {
	var err error
	var rows []sqlplugin.ClusterMetadataRow
	switch {
	case len(filter.ClusterName) != 0:
		err = mdb.SelectContext(ctx,
			&rows,
			listClusterMetadataRangeQry,
			constMetadataPartition,
			filter.ClusterName,
			filter.PageSize,
		)
	default:
		err = mdb.SelectContext(ctx,
			&rows,
			listClusterMetadataQry,
			constMetadataPartition,
			filter.PageSize,
		)
	}
	return rows, err
}

func (mdb *db) GetClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := mdb.GetContext(ctx,
		&row,
		getClusterMetadataQry,
		constMetadataPartition,
		filter.ClusterName,
	)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (mdb *db) DeleteClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) (sql.Result, error) {

	return mdb.ExecContext(ctx,
		deleteClusterMetadataQry,
		constMetadataPartition,
		filter.ClusterName,
	)
}

func (mdb *db) WriteLockGetClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := mdb.GetContext(ctx,
		&row,
		writeLockGetClusterMetadataQry,
		constMetadataPartition,
		filter.ClusterName,
	)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (mdb *db) UpsertClusterMembership(
	ctx context.Context,
	row *sqlplugin.ClusterMembershipRow,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		templateUpsertActiveClusterMembership,
		constMembershipPartition,
		row.HostID,
		row.RPCAddress,
		row.RPCPort,
		row.Role,
		mdb.converter.ToMSSQLDateTime(row.SessionStart),
		mdb.converter.ToMSSQLDateTime(row.LastHeartbeat),
		mdb.converter.ToMSSQLDateTime(row.RecordExpiry))
}

func (mdb *db) GetClusterMembers(
	ctx context.Context,
	filter *sqlplugin.ClusterMembershipFilter,
) ([]sqlplugin.ClusterMembershipRow, error) {
	var queryString strings.Builder
	var operands []any
	queryString.WriteString(templateGetClusterMembership)
	operands = append(operands, constMembershipPartition)

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

	if filter.HostIDGreaterThan != nil {
		queryString.WriteString(templateWithHostIDGreaterSuffix)
		operands = append(operands, filter.HostIDGreaterThan)
	}

	queryString.WriteString(templateWithOrderBySessionStartSuffix)

	if filter.MaxRecordCount > 0 {
		queryString.WriteString(templateWithLimitSuffix)
		operands = append(operands, filter.MaxRecordCount)
	}

	compiledQryString := queryString.String()

	var rows []sqlplugin.ClusterMembershipRow
	if err := mdb.SelectContext(ctx,
		&rows,
		compiledQryString,
		operands...,
	); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].SessionStart = mdb.converter.FromMSSQLDateTime(rows[i].SessionStart)
		rows[i].LastHeartbeat = mdb.converter.FromMSSQLDateTime(rows[i].LastHeartbeat)
		rows[i].RecordExpiry = mdb.converter.FromMSSQLDateTime(rows[i].RecordExpiry)
	}
	return rows, nil
}

func (mdb *db) PruneClusterMembership(
	ctx context.Context,
	filter *sqlplugin.PruneClusterMembershipFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		templatePruneStaleClusterMembership,
		constMembershipPartition,
		mdb.converter.ToMSSQLDateTime(filter.PruneRecordsBefore),
	)
}
