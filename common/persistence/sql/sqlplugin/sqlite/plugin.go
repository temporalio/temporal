// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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
	"context"
	gosql "database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/tools/common/schema"

	tools "go.temporal.io/server/tools/sql"
)

const (
	// PluginName is the name of the plugin
	PluginName = "sqlite"
)

var (
	//go:embed schema/unversioned/temporal/schema.sql
	temporalSchema []byte
	//go:embed schema/unversioned/visibility/schema.sql
	visibilitySchema []byte
)

type plugin struct {
	mainDB *db
	logger log.Logger
}

var _ sqlplugin.Plugin = (*plugin)(nil)

// Register registers the SQLite plugin with default configuration.
func Register() {
	logger := log.NewCLILogger()
	RegisterPluginWithLogger(PluginName, logger)
}

// RegisterPluginWithLogger registers the SQLite plugin with a custom logger and name.
func RegisterPluginWithLogger(pluginName string, logger log.Logger) {
	sql.RegisterPlugin(pluginName, &plugin{logger: logger})
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.DB, error) {
	if p.mainDB == nil {
		conn, err := p.createDBConnection(dbKind, cfg, r)
		if err != nil {
			return nil, err
		}
		p.mainDB = newDB(dbKind, cfg.DatabaseName, conn, nil)

		// Ensure namespaces exist
		if nsConfig := cfg.ConnectAttributes["preCreateNamespaces"]; nsConfig != "" {
			namespaces := strings.Split(cfg.ConnectAttributes["preCreateNamespaces"], ",")
			for _, ns := range namespaces {
				if err := createNamespaceIfNotExists(p.mainDB, ns); err != nil {
					return nil, fmt.Errorf("error ensuring namespace exists: %w", err)
				}
			}
		}
	}
	return p.mainDB, nil
}

// CreateAdminDB initialize the db object
func (p *plugin) CreateAdminDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (sqlplugin.AdminDB, error) {
	if _, err := p.CreateDB(sqlplugin.DbKindMain, cfg, r); err != nil {
		return nil, err
	}
	return p.mainDB, nil
}

// CreateDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func (p *plugin) createDBConnection(dbKind sqlplugin.DbKind, cfg *config.SQL, _ resolver.ServiceResolver) (*sqlx.DB, error) {
	if cfg.ConnectAttributes["mode"] == "rwc" {
		// Create db parent directory if it does not yet exist
		if _, err := os.Stat(filepath.Dir(cfg.DatabaseName)); errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(filepath.Dir(cfg.DatabaseName), os.ModePerm); err != nil {
				panic(err)
			}
		}
	}

	sdb, err := gosql.Open(
		goSqlDriverName,
		fmt.Sprintf("file:%s?mode=%s", cfg.DatabaseName, cfg.ConnectAttributes["mode"]),
	)
	if err != nil {
		return nil, err
	}

	db := sqlx.NewDb(sdb, PluginName)
	// The following options are set based on advice from https://github.com/mattn/go-sqlite3#faq
	//
	// Dealing with the error `database is locked`
	// > ... set the database connections of the SQL package to 1.
	db.SetMaxOpenConns(1)
	// Settings for in-memory database (should be fine for file mode as well)
	// > Note that if the last database connection in the pool closes, the in-memory database is deleted.
	// > Make sure the max idle connection limit is > 0, and the connection lifetime is infinite.
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)

	// Apply migrations when in-memory mode is enabled
	if err := func(name string) error {
		// Create tmp schema file
		schemaFile, err := os.CreateTemp("", "schema.sql")
		if err != nil {
			return fmt.Errorf("error creating temp schema file: %w", err)
		}
		defer os.Remove(schemaFile.Name())

		if _, err := schemaFile.Write(temporalSchema); err != nil {
			return fmt.Errorf("error writing schema file: %w", err)
		}
		if _, err := schemaFile.Write(visibilitySchema); err != nil {
			return fmt.Errorf("error writing schema file: %w", err)
		}

		conn, err := tools.NewConnection(cfg)
		if err != nil {
			return fmt.Errorf("could not open sqlite: %w", err)
		}
		defer conn.Close()

		return schema.SetupFromConfig(&schema.SetupConfig{
			SchemaFilePath:    schemaFile.Name(),
			InitialVersion:    "1.0",
			Overwrite:         false,
			DisableVersioning: false,
			Logger:            p.logger,
		}, conn)
	}(cfg.DatabaseName); err != nil {
		// Ignore error from running migrations twice against the same db.
		if !strings.Contains(err.Error(), "table schema_version already exists") {
			return nil, fmt.Errorf("could not set up db %q: %w", cfg.DatabaseName, err)
		}
	}

	return db, nil
}

func createNamespaceIfNotExists(mainDB *db, namespace string) error {
	// Return early if namespace already exists
	rows, err := mainDB.SelectFromNamespace(context.Background(), sqlplugin.NamespaceFilter{
		Name: &namespace,
	})
	if err == nil && len(rows) > 0 {
		return nil
	}

	nsID := primitives.NewUUID()

	d, err := serialization.NewSerializer().NamespaceDetailToBlob(&persistence.NamespaceDetail{
		Info: &persistence.NamespaceInfo{
			Id:    nsID.String(),
			State: enums.NAMESPACE_STATE_REGISTERED,
			Name:  namespace,
		},
		Config:            &persistence.NamespaceConfig{},
		ReplicationConfig: &persistence.NamespaceReplicationConfig{},
	}, enums.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}

	if _, err := mainDB.InsertIntoNamespace(context.Background(), &sqlplugin.NamespaceRow{
		ID:                  nsID,
		Name:                namespace,
		Data:                d.GetData(),
		DataEncoding:        d.GetEncodingType().String(),
		IsGlobal:            false,
		NotificationVersion: 0,
	}); err != nil {
		return err
	}

	return nil
}
