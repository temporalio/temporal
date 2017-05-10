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

package cassandra

import (
	"fmt"
	"regexp"
)

// SetupSchemaTask represents a task
// that sets up cassandra schema on
// a specified keyspace
type SetupSchemaTask struct {
	client CQLClient
	config *SetupSchemaConfig
}

func newSetupSchemaTask(config *SetupSchemaConfig) (*SetupSchemaTask, error) {
	client, err := newCQLClient(config.CassHosts, config.CassKeyspace)
	if err != nil {
		return nil, err
	}
	return &SetupSchemaTask{
		config: config,
		client: client,
	}, nil
}

// run executes the task
func (task *SetupSchemaTask) run() error {

	config := task.config

	fmt.Printf("Starting schema setup, config=%+v\n", config)

	if config.Overwrite {
		dropKeyspace(task.client)
	}

	if !config.DisableVersioning {
		fmt.Printf("Setting up version tables\n")
		if err := task.client.CreateSchemaVersionTables(); err != nil {
			return err
		}
	}

	stmts, err := ParseCQLFile(config.SchemaFilePath)
	if err != nil {
		return err
	}

	re := regexp.MustCompile("\\s+")

	fmt.Println("----- Creating types and tables -----")
	for _, stmt := range stmts {
		fmt.Println(re.ReplaceAllString(stmt, " "))
		if err := task.client.Exec(stmt); err != nil {
			return err
		}
	}
	fmt.Println("----- Done -----")

	if !config.DisableVersioning {
		fmt.Printf("Setting initial schema version to %v\n", config.InitialVersion)
		err := task.client.UpdateSchemaVersion(int64(config.InitialVersion), int64(config.InitialVersion))
		if err != nil {
			return err
		}
		fmt.Printf("Updating schema update log\n")
		err = task.client.WriteSchemaUpdateLog(int64(0), int64(config.InitialVersion), "", "initial version")
		if err != nil {
			return err
		}
	}

	fmt.Println("Schema setup complete")

	return nil
}
