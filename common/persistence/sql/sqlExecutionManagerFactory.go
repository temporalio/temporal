// Copyright (c) 2018 Uber Technologies, Inc.
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

package sql

import (
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

type (
	sqlExecutionManagerFactory struct {
		host               string
		port               int
		username           string
		password           string
		dbName             string
		currentClusterName string
		logger             bark.Logger
	}
)

// NewExecutionManagerFactory creates ExecutionManagerFactory for SQL persistence.
func NewExecutionManagerFactory(host string, port int, username, password, dbName string, currentClusterName string, logger bark.Logger) (persistence.ExecutionManagerFactory, error) {
	return &sqlExecutionManagerFactory{
		host:               host,
		port:               port,
		username:           username,
		password:           password,
		dbName:             dbName,
		currentClusterName: currentClusterName,
		logger:             logger,
	}, nil
}

func (f *sqlExecutionManagerFactory) CreateExecutionManager(shardID int) (persistence.ExecutionManager, error) {
	return NewSQLMatchingPersistence(f.host, f.port, f.username, f.password, f.dbName, f.logger)
}

func (f *sqlExecutionManagerFactory) Close() {
}
