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

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const constMetadataPartition = 0
const (
	// One time only for the immutable data, so lets just use insert ignore
	insertClusterMetadataOneTimeOnlyQry = `INSERT IGNORE INTO 
cluster_metadata (metadata_partition, immutable_data, immutable_data_encoding)
VALUES(?, ?, ?)`

	getImmutableClusterMetadataQry = `SELECT immutable_data, immutable_data_encoding FROM 
cluster_metadata WHERE metadata_partition = ?`
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
