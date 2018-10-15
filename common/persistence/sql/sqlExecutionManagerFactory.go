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
	"github.com/uber/cadence/common/service/config"
)

type (
	sqlExecutionManagerFactory struct {
		config             config.SQL
		currentClusterName string
		logger             bark.Logger
	}
)

// newExecutionManagerFactory creates ExecutionManagerFactory for SQL persistence.
func newExecutionManagerFactory(cfg config.SQL, currentClusterName string, logger bark.Logger) (persistence.ExecutionManagerFactory, error) {
	return &sqlExecutionManagerFactory{
		config:             cfg,
		currentClusterName: currentClusterName,
		logger:             logger,
	}, nil
}

func (f *sqlExecutionManagerFactory) NewExecutionManager(shardID int) (persistence.ExecutionManager, error) {
	pMgr, err := NewSQLMatchingPersistence(f.config, f.logger)
	if err != nil {
		return nil, err
	}
	mgr := persistence.NewExecutionManagerImpl(pMgr, f.logger)
	return mgr, nil
}

func (f *sqlExecutionManagerFactory) Close() {
}
