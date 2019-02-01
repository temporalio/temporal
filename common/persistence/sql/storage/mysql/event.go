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

	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

const (
	addEventsQry = `INSERT INTO events (` +
		`domain_id,workflow_id,run_id,first_event_id,batch_version,range_id,tx_id,data,data_encoding)` +
		`VALUES (:domain_id,:workflow_id,:run_id,:first_event_id,:batch_version,:range_id,:tx_id,:data,:data_encoding);`

	updateEventsQry = `UPDATE events ` +
		`SET batch_version = :batch_version, range_id = :range_id, tx_id = :tx_id, data = :data, data_encoding = :data_encoding ` +
		`WHERE domain_id = :domain_id AND workflow_id = :workflow_id AND run_id = :run_id AND first_event_id = :first_event_id`

	getEventsQry = `SELECT first_event_id, batch_version, data, data_encoding ` +
		`FROM events ` +
		`WHERE domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id >= ? AND first_event_id < ? ` +
		`ORDER BY first_event_id LIMIT ?`

	deleteEventsQry = `DELETE FROM events WHERE domain_id = ? AND workflow_id = ? AND run_id = ?`

	lockEventQry = `SELECT range_id, tx_id FROM events ` +
		`WHERE domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id = ? ` +
		`FOR UPDATE`
)

// InsertIntoEvents inserts a row into events table
func (mdb *DB) InsertIntoEvents(row *sqldb.EventsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(addEventsQry, row)
}

// UpdateEvents updates a row in events table
func (mdb *DB) UpdateEvents(row *sqldb.EventsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(updateEventsQry, row)
}

// SelectFromEvents reads one or more rows from events table
func (mdb *DB) SelectFromEvents(filter *sqldb.EventsFilter) ([]sqldb.EventsRow, error) {
	var rows []sqldb.EventsRow
	err := mdb.conn.Select(&rows, getEventsQry,
		filter.DomainID, filter.WorkflowID, filter.RunID, *filter.FirstEventID, *filter.NextEventID, *filter.PageSize)
	return rows, err
}

// DeleteFromEvents deletes one or more rows from events table
func (mdb *DB) DeleteFromEvents(filter *sqldb.EventsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteEventsQry, filter.DomainID, filter.WorkflowID, filter.RunID)
}

// LockEvents acquires a write lock on a single row in events table
func (mdb *DB) LockEvents(filter *sqldb.EventsFilter) (*sqldb.EventsRow, error) {
	var row sqldb.EventsRow
	err := mdb.conn.Get(&row, lockEventQry, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.FirstEventID)
	return &row, err
}
