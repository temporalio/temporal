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

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	createDomainQuery = `INSERT INTO 
 domains (id, name, is_global, data, data_encoding)
 VALUES(?, ?, ?, ?, ?)`

	updateDomainQuery = `UPDATE domains 
 SET name = ?, data = ?, data_encoding = ?
 WHERE shard_id=54321 AND id = ?`

	getDomainPart = `SELECT id, name, is_global, data, data_encoding FROM domains`

	getDomainByIDQuery   = getDomainPart + ` WHERE shard_id=? AND id = ?`
	getDomainByNameQuery = getDomainPart + ` WHERE shard_id=? AND name = ?`

	listDomainsQuery      = getDomainPart + ` WHERE shard_id=? ORDER BY id LIMIT ?`
	listDomainsRangeQuery = getDomainPart + ` WHERE shard_id=? AND id > ? ORDER BY id LIMIT ?`

	deleteDomainByIDQuery   = `DELETE FROM domains WHERE shard_id=? AND id = ?`
	deleteDomainByNameQuery = `DELETE FROM domains WHERE shard_id=? AND name = ?`

	getDomainMetadataQuery    = `SELECT notification_version FROM domain_metadata`
	lockDomainMetadataQuery   = `SELECT notification_version FROM domain_metadata FOR UPDATE`
	updateDomainMetadataQuery = `UPDATE domain_metadata SET notification_version = ? WHERE notification_version = ?`
)

const (
	shardID = 54321
)

var errMissingArgs = errors.New("missing one or more args for API")

// InsertIntoDomain inserts a single row into domains table
func (mdb *db) InsertIntoDomain(row *sqlplugin.DomainRow) (sql.Result, error) {
	return mdb.conn.Exec(createDomainQuery, row.ID, row.Name, row.IsGlobal, row.Data, row.DataEncoding)
}

// UpdateDomain updates a single row in domains table
func (mdb *db) UpdateDomain(row *sqlplugin.DomainRow) (sql.Result, error) {
	return mdb.conn.Exec(updateDomainQuery, row.Name, row.Data, row.DataEncoding, row.ID)
}

// SelectFromDomain reads one or more rows from domains table
func (mdb *db) SelectFromDomain(filter *sqlplugin.DomainFilter) ([]sqlplugin.DomainRow, error) {
	switch {
	case filter.ID != nil || filter.Name != nil:
		return mdb.selectFromDomain(filter)
	case filter.PageSize != nil && *filter.PageSize > 0:
		return mdb.selectAllFromDomain(filter)
	default:
		return nil, errMissingArgs
	}
}

func (mdb *db) selectFromDomain(filter *sqlplugin.DomainFilter) ([]sqlplugin.DomainRow, error) {
	var err error
	var row sqlplugin.DomainRow
	switch {
	case filter.ID != nil:
		err = mdb.conn.Get(&row, getDomainByIDQuery, shardID, *filter.ID)
	case filter.Name != nil:
		err = mdb.conn.Get(&row, getDomainByNameQuery, shardID, *filter.Name)
	}
	if err != nil {
		return nil, err
	}
	return []sqlplugin.DomainRow{row}, err
}

func (mdb *db) selectAllFromDomain(filter *sqlplugin.DomainFilter) ([]sqlplugin.DomainRow, error) {
	var err error
	var rows []sqlplugin.DomainRow
	switch {
	case filter.GreaterThanID != nil:
		err = mdb.conn.Select(&rows, listDomainsRangeQuery, shardID, *filter.GreaterThanID, *filter.PageSize)
	default:
		err = mdb.conn.Select(&rows, listDomainsQuery, shardID, filter.PageSize)
	}
	return rows, err
}

// DeleteFromDomain deletes a single row in domains table
func (mdb *db) DeleteFromDomain(filter *sqlplugin.DomainFilter) (sql.Result, error) {
	var err error
	var result sql.Result
	switch {
	case filter.ID != nil:
		result, err = mdb.conn.Exec(deleteDomainByIDQuery, shardID, filter.ID)
	default:
		result, err = mdb.conn.Exec(deleteDomainByNameQuery, shardID, filter.Name)
	}
	return result, err
}

// LockDomainMetadata acquires a write lock on a single row in domain_metadata table
func (mdb *db) LockDomainMetadata() error {
	var row sqlplugin.DomainMetadataRow
	err := mdb.conn.Get(&row.NotificationVersion, lockDomainMetadataQuery)
	return err
}

// SelectFromDomainMetadata reads a single row in domain_metadata table
func (mdb *db) SelectFromDomainMetadata() (*sqlplugin.DomainMetadataRow, error) {
	var row sqlplugin.DomainMetadataRow
	err := mdb.conn.Get(&row.NotificationVersion, getDomainMetadataQuery)
	return &row, err
}

// UpdateDomainMetadata updates a single row in domain_metadata table
func (mdb *db) UpdateDomainMetadata(row *sqlplugin.DomainMetadataRow) (sql.Result, error) {
	return mdb.conn.Exec(updateDomainMetadataQuery, row.NotificationVersion+1, row.NotificationVersion)
}
