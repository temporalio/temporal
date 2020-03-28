// Copyright (c) 2017 Uber Technologies, Inc.
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
	"errors"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const (
	createNamespaceQuery = `INSERT INTO 
 namespaces (id, name, is_global, data, data_encoding)
 VALUES(?, ?, ?, ?, ?)`

	updateNamespaceQuery = `UPDATE namespaces 
 SET name = ?, data = ?, data_encoding = ?
 WHERE shard_id=54321 AND id = ?`

	getNamespacePart = `SELECT id, name, is_global, data, data_encoding FROM namespaces`

	getNamespaceByIDQuery   = getNamespacePart + ` WHERE shard_id=? AND id = ?`
	getNamespaceByNameQuery = getNamespacePart + ` WHERE shard_id=? AND name = ?`

	listNamespacesQuery      = getNamespacePart + ` WHERE shard_id=? ORDER BY id LIMIT ?`
	listNamespacesRangeQuery = getNamespacePart + ` WHERE shard_id=? AND id > ? ORDER BY id LIMIT ?`

	deleteNamespaceByIDQuery   = `DELETE FROM namespaces WHERE shard_id=? AND id = ?`
	deleteNamespaceByNameQuery = `DELETE FROM namespaces WHERE shard_id=? AND name = ?`

	getNamespaceMetadataQuery    = `SELECT notification_version FROM namespace_metadata`
	lockNamespaceMetadataQuery   = `SELECT notification_version FROM namespace_metadata FOR UPDATE`
	updateNamespaceMetadataQuery = `UPDATE namespace_metadata SET notification_version = ? WHERE notification_version = ?`
)

const (
	shardID = 54321
)

var errMissingArgs = errors.New("missing one or more args for API")

// InsertIntoNamespace inserts a single row into namespaces table
func (mdb *db) InsertIntoNamespace(row *sqlplugin.NamespaceRow) (sql.Result, error) {
	return mdb.conn.Exec(createNamespaceQuery, row.ID, row.Name, row.IsGlobal, row.Data, row.DataEncoding)
}

// UpdateNamespace updates a single row in namespaces table
func (mdb *db) UpdateNamespace(row *sqlplugin.NamespaceRow) (sql.Result, error) {
	return mdb.conn.Exec(updateNamespaceQuery, row.Name, row.Data, row.DataEncoding, row.ID)
}

// SelectFromNamespace reads one or more rows from namespaces table
func (mdb *db) SelectFromNamespace(filter *sqlplugin.NamespaceFilter) ([]sqlplugin.NamespaceRow, error) {
	switch {
	case filter.ID != nil || filter.Name != nil:
		return mdb.selectFromNamespace(filter)
	case filter.PageSize != nil && *filter.PageSize > 0:
		return mdb.selectAllFromNamespace(filter)
	default:
		return nil, errMissingArgs
	}
}

func (mdb *db) selectFromNamespace(filter *sqlplugin.NamespaceFilter) ([]sqlplugin.NamespaceRow, error) {
	var err error
	var row sqlplugin.NamespaceRow
	switch {
	case filter.ID != nil:
		err = mdb.conn.Get(&row, getNamespaceByIDQuery, shardID, *filter.ID)
	case filter.Name != nil:
		err = mdb.conn.Get(&row, getNamespaceByNameQuery, shardID, *filter.Name)
	}
	if err != nil {
		return nil, err
	}
	return []sqlplugin.NamespaceRow{row}, err
}

func (mdb *db) selectAllFromNamespace(filter *sqlplugin.NamespaceFilter) ([]sqlplugin.NamespaceRow, error) {
	var err error
	var rows []sqlplugin.NamespaceRow
	switch {
	case filter.GreaterThanID != nil:
		err = mdb.conn.Select(&rows, listNamespacesRangeQuery, shardID, *filter.GreaterThanID, *filter.PageSize)
	default:
		err = mdb.conn.Select(&rows, listNamespacesQuery, shardID, filter.PageSize)
	}
	return rows, err
}

// DeleteFromNamespace deletes a single row in namespaces table
func (mdb *db) DeleteFromNamespace(filter *sqlplugin.NamespaceFilter) (sql.Result, error) {
	var err error
	var result sql.Result
	switch {
	case filter.ID != nil:
		result, err = mdb.conn.Exec(deleteNamespaceByIDQuery, shardID, filter.ID)
	default:
		result, err = mdb.conn.Exec(deleteNamespaceByNameQuery, shardID, filter.Name)
	}
	return result, err
}

// LockNamespaceMetadata acquires a write lock on a single row in namespace_metadata table
func (mdb *db) LockNamespaceMetadata() error {
	var row sqlplugin.NamespaceMetadataRow
	err := mdb.conn.Get(&row.NotificationVersion, lockNamespaceMetadataQuery)
	return err
}

// SelectFromNamespaceMetadata reads a single row in namespace_metadata table
func (mdb *db) SelectFromNamespaceMetadata() (*sqlplugin.NamespaceMetadataRow, error) {
	var row sqlplugin.NamespaceMetadataRow
	err := mdb.conn.Get(&row.NotificationVersion, getNamespaceMetadataQuery)
	return &row, err
}

// UpdateNamespaceMetadata updates a single row in namespace_metadata table
func (mdb *db) UpdateNamespaceMetadata(row *sqlplugin.NamespaceMetadataRow) (sql.Result, error) {
	return mdb.conn.Exec(updateNamespaceMetadataQuery, row.NotificationVersion+1, row.NotificationVersion)
}
