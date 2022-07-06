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

package sqlite

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"

	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resolver"
)

var (
	//go:embed v3/temporal/schema.sql
	executionSchema []byte
	//go:embed v3/visibility/schema.sql
	visibilitySchema []byte
)

// SetupSchema initializes the SQLite schema in an empty database.
//
// Note: this function may receive breaking changes or be removed in the future.
func SetupSchema(cfg *config.SQL) error {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	if err != nil {
		return fmt.Errorf("unable to create SQLite admin DB: %w", err)
	}
	defer func() { _ = db.Close() }()

	return SetupSchemaOnDB(db)
}

// SetupSchemaOnDB initializes the SQLite schema in an empty database using existing DB connection.
//
// Note: this function may receive breaking changes or be removed in the future.
func SetupSchemaOnDB(db sqlplugin.AdminDB) error {
	statements, err := p.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBuffer(executionSchema)})
	if err != nil {
		return fmt.Errorf("error loading execution schema: %w", err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			return fmt.Errorf("error executing statement %q: %w", stmt, err)
		}
	}

	statements, err = p.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBuffer(visibilitySchema)})
	if err != nil {
		return fmt.Errorf("error loading visibility schema: %w", err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			return fmt.Errorf("error executing statement %q: %w", stmt, err)
		}
	}

	return nil
}

// NamespaceConfig determines how namespaces should be configured during registration.
//
// Note: this struct may receive breaking changes or be removed in the future.
type NamespaceConfig struct {
	// Low level representation of a Namespace used by Temporal persistence drivers.
	Detail *persistencespb.NamespaceDetail
	// Global Namespaces provide support for replication of Workflow execution across clusters.
	IsGlobal bool
}

// CreateNamespaces creates namespaces in the target database without the need to have a running Temporal server.
//
// This exists primarily as a workaround for https://github.com/temporalio/temporal/issues/1336. Namespaces should
// typically be created through the Temporal API either via `tctl` or an SDK client.
//
// Attempting to create a namespace that already exists will be a no-op.
//
// Note: this function may receive breaking changes or be removed in the future.
func CreateNamespaces(cfg *config.SQL, namespaces ...*NamespaceConfig) error {
	db, err := sql.NewSQLDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	if err != nil {
		return fmt.Errorf("unable to create SQLite admin DB: %w", err)
	}
	defer func() { _ = db.Close() }()

	for _, ns := range namespaces {
		if err := createNamespaceIfNotExists(db, ns); err != nil {
			return fmt.Errorf("error creating namespace %q: %w", ns.Detail.Info.Name, err)
		}
	}

	return nil
}

// NewNamespaceConfig initializes a NamespaceConfig with the field values needed to pre-register
// the namespace via the CreateNamespaces function.
//
// Note: this function may receive breaking changes or be removed in the future.
func NewNamespaceConfig(activeClusterName, namespace string, global bool) *NamespaceConfig {
	detail := persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:    primitives.NewUUID().String(),
			State: enumspb.NAMESPACE_STATE_REGISTERED,
			Name:  namespace,
		},
		Config: &persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromHours(24),
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters:          p.GetOrUseDefaultClusters(activeClusterName, nil),
		},
		FailoverVersion:             common.EmptyVersion,
		FailoverNotificationVersion: -1,
	}
	return &NamespaceConfig{
		Detail:   &detail,
		IsGlobal: global,
	}
}

func createNamespaceIfNotExists(db sqlplugin.DB, namespace *NamespaceConfig) error {
	var (
		name = namespace.Detail.GetInfo().GetName()
		id   = primitives.MustParseUUID(namespace.Detail.GetInfo().GetId())
	)

	// Return early if namespace already exists
	rows, err := db.SelectFromNamespace(context.Background(), sqlplugin.NamespaceFilter{
		Name: &name,
	})
	if err == nil && len(rows) > 0 {
		return nil
	}

	blob, err := serialization.NewSerializer().NamespaceDetailToBlob(namespace.Detail, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}

	if _, err := db.InsertIntoNamespace(context.Background(), &sqlplugin.NamespaceRow{
		ID:                  id,
		Name:                name,
		Data:                blob.GetData(),
		DataEncoding:        blob.GetEncodingType().String(),
		IsGlobal:            namespace.IsGlobal,
		NotificationVersion: 0,
	}); err != nil {
		return err
	}

	return nil
}
