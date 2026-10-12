//go:build !temporal_minimal || temporal_with_mysql

package main

import (
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql" // needed to load mysql plugin
)
