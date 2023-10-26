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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/searchattribute"
)

func NewQueryConverter(
	pluginName string,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
	enableCountGroupByAnySA dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	countGroupByMaxGroups dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *QueryConverter {
	var pqc pluginQueryConverter
	switch pluginName {
	case mysql.PluginNameV8:
		pqc = &mysqlQueryConverter{}
	case postgresql.PluginNameV12, postgresql.PluginNameV12PGX:
		pqc = &pgQueryConverter{}
	case sqlite.PluginName:
		pqc = &sqliteQueryConverter{}
	default:
		return nil
	}
	return newQueryConverterInternal(
		pqc,
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
		enableCountGroupByAnySA,
		countGroupByMaxGroups,
	)
}
