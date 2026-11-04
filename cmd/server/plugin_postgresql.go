//go:build !temporal_minimal || temporal_with_postgresql

package main

import (
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // needed to load postgresql plugin
)
