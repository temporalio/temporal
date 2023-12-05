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

package schema

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/blang/semver/v4"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	dbschemas "go.temporal.io/server/schema"
)

// SetupTask represents a task
// that sets up cassandra schema on
// a specified keyspace
type SetupTask struct {
	db     DB
	config *SetupConfig
	logger log.Logger
}

func newSetupSchemaTask(db DB, config *SetupConfig, logger log.Logger) *SetupTask {
	return &SetupTask{
		db:     db,
		config: config,
		logger: logger,
	}
}

// Run executes the task
func (task *SetupTask) Run() error {
	config := task.config
	task.logger.Info("Starting schema setup", tag.NewAnyTag("config", config))

	if config.Overwrite {
		err := task.db.DropAllTables()
		if err != nil {
			return err
		}
	}

	if !config.DisableVersioning {
		task.logger.Debug("Setting up version tables")
		if err := task.db.CreateSchemaVersionTables(); err != nil {
			return err
		}
	}

	if len(config.SchemaFilePath) > 0 || len(config.SchemaName) > 0 {
		var schemaBuf []byte
		var err error
		var schemaFilePath string
		if len(config.SchemaName) > 0 {
			fsys := dbschemas.Assets()
			schemaFilePath = filepath.Join(config.SchemaName, "schema"+schemaFileEnding(config.SchemaName))
			schemaBuf, err = fs.ReadFile(fsys, schemaFilePath)
		} else {
			schemaFilePath, err = filepath.Abs(config.SchemaFilePath)
			if err != nil {
				return err
			}
			schemaBuf, err = os.ReadFile(schemaFilePath)
		}
		if err != nil {
			return fmt.Errorf("error reading file %s: %w", schemaFilePath, err)
		}
		stmts, err := persistence.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBuffer(schemaBuf)})
		if err != nil {
			return fmt.Errorf("error parsing query: %v", err)
		}
		task.logger.Debug("----- Creating types and tables -----")
		for _, stmt := range stmts {
			task.logger.Debug(rmspaceRegex.ReplaceAllString(stmt, " "))
			if err := task.db.Exec(stmt); err != nil {
				return err
			}
		}
		task.logger.Debug("----- Done -----")
	}

	if !config.DisableVersioning {
		currVer, err := task.db.ReadSchemaVersion()
		if err != nil {
			currVer = "0.0"
		}

		currVerParsed, err := semver.ParseTolerant(currVer)
		if err != nil {
			task.logger.Fatal("Unable to parse current version",
				tag.NewStringTag("current version", currVer),
				tag.Error(err),
			)
		}

		initialVersionParsed, err := semver.ParseTolerant(config.InitialVersion)
		if err != nil {
			task.logger.Fatal("Unable to parse initial version",
				tag.NewStringTag("initial version", config.InitialVersion),
				tag.Error(err),
			)
		}

		if currVerParsed.GT(initialVersionParsed) {
			task.logger.Debug(fmt.Sprintf("Current database schema version %v is greater than initial schema version %v. Skip version upgrade", currVer, config.InitialVersion))
		} else {
			task.logger.Debug(fmt.Sprintf("Setting initial schema version to %v", config.InitialVersion))
			err := task.db.UpdateSchemaVersion(config.InitialVersion, config.InitialVersion)
			if err != nil {
				return err
			}
			task.logger.Debug("Updating schema update log")
			err = task.db.WriteSchemaUpdateLog("0", config.InitialVersion, "", "initial version")
			if err != nil {
				return err
			}
		}
	}

	task.logger.Info("Schema setup complete")

	return nil
}
