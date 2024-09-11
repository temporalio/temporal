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

package sql

import (
	"errors"
	"fmt"
	"slices"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/observability/log"
	"go.temporal.io/server/common/observability/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	expmaps "golang.org/x/exp/maps"
)

var ErrPluginNotSupported = errors.New("plugin not supported")

var supportedPlugins = map[string]sqlplugin.Plugin{}

// RegisterPlugin will register a SQL plugin
func RegisterPlugin(pluginName string, plugin sqlplugin.Plugin) {
	if _, ok := supportedPlugins[pluginName]; ok {
		panic("plugin " + pluginName + " already registered")
	}
	supportedPlugins[pluginName] = plugin
}

// NewSQLDB creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func NewSQLDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	mh metrics.Handler,
) (sqlplugin.DB, error) {
	plugin, err := getPlugin(cfg.PluginName)
	if err != nil {
		return nil, err
	}
	return plugin.CreateDB(dbKind, cfg, r, logger, mh)
}

// NewSQLAdminDB returns a AdminDB
func NewSQLAdminDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	mh metrics.Handler,
) (sqlplugin.AdminDB, error) {
	plugin, err := getPlugin(cfg.PluginName)
	if err != nil {
		return nil, err
	}
	return plugin.CreateAdminDB(dbKind, cfg, r, logger, mh)
}

func getPlugin(pluginName string) (sqlplugin.Plugin, error) {
	plugin, ok := supportedPlugins[pluginName]
	if !ok {
		keys := expmaps.Keys(supportedPlugins)
		slices.Sort(keys)
		return nil, fmt.Errorf(
			"%w: unknown plugin %q, supported plugins: %v",
			ErrPluginNotSupported,
			pluginName,
			keys,
		)
	}
	return plugin, nil
}
