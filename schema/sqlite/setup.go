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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute/sadefs"
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
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
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
	db, err := sql.NewSQLDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
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
func NewNamespaceConfig(
	activeClusterName string,
	namespace string,
	global bool,
	customSearchAttributes map[string]enumspb.IndexedValueType,
) (*NamespaceConfig, error) {
	dbCustomSearchAttributes := sadefs.GetDBIndexSearchAttributes(nil).CustomSearchAttributes
	fieldToAliasMap := map[string]string{}
	for saName, saType := range customSearchAttributes {
		var targetFieldName string
		var cntUsed int
		for fieldName, fieldType := range dbCustomSearchAttributes {
			if fieldType != saType {
				continue
			}
			if _, ok := fieldToAliasMap[fieldName]; !ok {
				targetFieldName = fieldName
				break
			}
			cntUsed++
		}
		if targetFieldName == "" {
			return nil, fmt.Errorf(
				"cannot have more than %d search attributes of type %s",
				cntUsed,
				saType,
			)
		}
		fieldToAliasMap[targetFieldName] = saName
	}

	detail := persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:    primitives.NewUUID().String(),
			State: enumspb.NAMESPACE_STATE_REGISTERED,
			Name:  namespace,
		},
		Config: &persistencespb.NamespaceConfig{
			Retention:                    timestamp.DurationFromHours(24),
			HistoryArchivalState:         enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalState:      enumspb.ARCHIVAL_STATE_DISABLED,
			CustomSearchAttributeAliases: fieldToAliasMap,
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters:          []string{activeClusterName},
		},
		FailoverVersion:             common.EmptyVersion,
		FailoverNotificationVersion: -1,
	}
	return &NamespaceConfig{
		Detail:   &detail,
		IsGlobal: global,
	}, nil
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

	blob, err := serialization.NewSerializer().NamespaceDetailToBlob(namespace.Detail)
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
