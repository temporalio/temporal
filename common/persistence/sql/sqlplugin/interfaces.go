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

package sqlplugin

import (
	"database/sql"

	"go.temporal.io/server/common/service/config"
)

type (
	// Plugin defines the interface for any SQL database that needs to implement
	Plugin interface {
		CreateDB(cfg *config.SQL) (DB, error)
		CreateAdminDB(cfg *config.SQL) (AdminDB, error)
	}

	// tableCRUD defines the API for interacting with the database tables
	tableCRUD interface {
		clusterMetadata
		Namespace
		visibility
		queue

		MatchingTask
		MatchingTaskQueue

		HistoryShard
		historyEvent
		historyExecution
		HistoryExecutionBuffer
		HistoryExecutionActivity
		HistoryExecutionChildWorkflow
		HistoryExecutionTimer
		HistoryExecutionRequestCancel
		HistoryExecutionSignal
		HistoryExecutionSignalRequest
		historyTransferTask
		historyTimerTask
		historyReplicationTask
	}

	// adminCRUD defines admin operations for CLI and test suites
	adminCRUD interface {
		CreateSchemaVersionTables() error
		ReadSchemaVersion(database string) (string, error)
		UpdateSchemaVersion(database string, newVersion string, minCompatibleVersion string) error
		WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error
		ListTables(database string) ([]string, error)
		DropTable(table string) error
		DropAllTables(database string) error
		CreateDatabase(database string) error
		DropDatabase(database string) error
		Exec(stmt string, args ...interface{}) error
	}

	// Tx defines the API for a SQL transaction
	Tx interface {
		tableCRUD
		Commit() error
		Rollback() error
	}

	// DB defines the API for regular SQL operations of a Temporal server
	DB interface {
		tableCRUD

		BeginTx() (Tx, error)
		PluginName() string
		IsDupEntryError(err error) bool
		Close() error
	}

	// AdminDB defines the API for admin SQL operations for CLI and testing suites
	AdminDB interface {
		adminCRUD
		PluginName() string
		Close() error
	}
	// Conn defines the API for a single database connection
	Conn interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
		NamedExec(query string, arg interface{}) (sql.Result, error)
		Get(dest interface{}, query string, args ...interface{}) error
		Select(dest interface{}, query string, args ...interface{}) error
	}
)
