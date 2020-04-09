package postgres

import (
	"database/sql"
	"errors"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const (
	createNamespaceQuery = `INSERT INTO 
 namespaces (id, name, is_global, data, data_encoding)
 VALUES($1, $2, $3, $4, $5)`

	updateNamespaceQuery = `UPDATE namespaces 
 SET name = $1, data = $2, data_encoding = $3
 WHERE shard_id=54321 AND id = $4`

	getNamespacePart = `SELECT id, name, is_global, data, data_encoding FROM namespaces`

	getNamespaceByIDQuery   = getNamespacePart + ` WHERE shard_id=$1 AND id = $2`
	getNamespaceByNameQuery = getNamespacePart + ` WHERE shard_id=$1 AND name = $2`

	listNamespacesQuery      = getNamespacePart + ` WHERE shard_id=$1 ORDER BY id LIMIT $2`
	listNamespacesRangeQuery = getNamespacePart + ` WHERE shard_id=$1 AND id > $2 ORDER BY id LIMIT $3`

	deleteNamespaceByIDQuery   = `DELETE FROM namespaces WHERE shard_id=$1 AND id = $2`
	deleteNamespaceByNameQuery = `DELETE FROM namespaces WHERE shard_id=$1 AND name = $2`

	getNamespaceMetadataQuery    = `SELECT notification_version FROM namespace_metadata`
	lockNamespaceMetadataQuery   = `SELECT notification_version FROM namespace_metadata FOR UPDATE`
	updateNamespaceMetadataQuery = `UPDATE namespace_metadata SET notification_version = $1 WHERE notification_version = $2`
)

const (
	shardID = 54321
)

var errMissingArgs = errors.New("missing one or more args for API")

// InsertIntoNamespace inserts a single row into namespaces table
func (pdb *db) InsertIntoNamespace(row *sqlplugin.NamespaceRow) (sql.Result, error) {
	return pdb.conn.Exec(createNamespaceQuery, row.ID, row.Name, row.IsGlobal, row.Data, row.DataEncoding)
}

// UpdateNamespace updates a single row in namespaces table
func (pdb *db) UpdateNamespace(row *sqlplugin.NamespaceRow) (sql.Result, error) {
	return pdb.conn.Exec(updateNamespaceQuery, row.Name, row.Data, row.DataEncoding, row.ID)
}

// SelectFromNamespace reads one or more rows from namespaces table
func (pdb *db) SelectFromNamespace(filter *sqlplugin.NamespaceFilter) ([]sqlplugin.NamespaceRow, error) {
	switch {
	case filter.ID != nil || filter.Name != nil:
		return pdb.selectFromNamespace(filter)
	case filter.PageSize != nil && *filter.PageSize > 0:
		return pdb.selectAllFromNamespace(filter)
	default:
		return nil, errMissingArgs
	}
}

func (pdb *db) selectFromNamespace(filter *sqlplugin.NamespaceFilter) ([]sqlplugin.NamespaceRow, error) {
	var err error
	var row sqlplugin.NamespaceRow
	switch {
	case filter.ID != nil:
		err = pdb.conn.Get(&row, getNamespaceByIDQuery, shardID, *filter.ID)
	case filter.Name != nil:
		err = pdb.conn.Get(&row, getNamespaceByNameQuery, shardID, *filter.Name)
	}
	if err != nil {
		return nil, err
	}
	return []sqlplugin.NamespaceRow{row}, err
}

func (pdb *db) selectAllFromNamespace(filter *sqlplugin.NamespaceFilter) ([]sqlplugin.NamespaceRow, error) {
	var err error
	var rows []sqlplugin.NamespaceRow
	switch {
	case filter.GreaterThanID != nil:
		err = pdb.conn.Select(&rows, listNamespacesRangeQuery, shardID, *filter.GreaterThanID, *filter.PageSize)
	default:
		err = pdb.conn.Select(&rows, listNamespacesQuery, shardID, filter.PageSize)
	}
	return rows, err
}

// DeleteFromNamespace deletes a single row in namespaces table
func (pdb *db) DeleteFromNamespace(filter *sqlplugin.NamespaceFilter) (sql.Result, error) {
	var err error
	var result sql.Result
	switch {
	case filter.ID != nil:
		result, err = pdb.conn.Exec(deleteNamespaceByIDQuery, shardID, filter.ID)
	default:
		result, err = pdb.conn.Exec(deleteNamespaceByNameQuery, shardID, filter.Name)
	}
	return result, err
}

// LockNamespaceMetadata acquires a write lock on a single row in namespace_metadata table
func (pdb *db) LockNamespaceMetadata() error {
	var row sqlplugin.NamespaceMetadataRow
	err := pdb.conn.Get(&row.NotificationVersion, lockNamespaceMetadataQuery)
	return err
}

// SelectFromNamespaceMetadata reads a single row in namespace_metadata table
func (pdb *db) SelectFromNamespaceMetadata() (*sqlplugin.NamespaceMetadataRow, error) {
	var row sqlplugin.NamespaceMetadataRow
	err := pdb.conn.Get(&row.NotificationVersion, getNamespaceMetadataQuery)
	return &row, err
}

// UpdateNamespaceMetadata updates a single row in namespace_metadata table
func (pdb *db) UpdateNamespaceMetadata(row *sqlplugin.NamespaceMetadataRow) (sql.Result, error) {
	return pdb.conn.Exec(updateNamespaceMetadataQuery, row.NotificationVersion+1, row.NotificationVersion)
}
