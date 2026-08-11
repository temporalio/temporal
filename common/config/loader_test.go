package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/testutils"
)

const fileMode = os.FileMode(0644)

func TestLoad(t *testing.T) {
	const staticConfig = `log:
  level: warn
persistence:
  numHistoryShards: 16
  defaultStore: default
  datastores:
    default:
      sql:
        pluginName: "postgres12"
        databaseName: "temporal"
        connectAddr: "localhost:5432"
        connectProtocol: "tcp"
services:
  frontend:
    rpc:
      grpcPort: 9233
      bindOnIP: "0.0.0.0"
`

	const templateConfig = `# enable-template
log:
  level: {{ default "info" (env "LOG_LEVEL") }}
persistence:
  numHistoryShards: {{ default "4" (env "NUM_HISTORY_SHARDS") }}
  defaultStore: default
  datastores:
    default:
      sql:
        pluginName: "postgres12"
        databaseName: "temporal"
        connectAddr: "localhost:5432"
        connectProtocol: "tcp"
services:
  frontend:
    rpc:
      grpcPort: {{ default "7233" (env "FRONTEND_GRPC_PORT") }}
      bindOnIP: "127.0.0.1"
`

	const invalidYaml = `log:
  level: warn
  invalid indentation
    bad: yaml
`

	testCases := []struct {
		name           string
		configContent  string
		loadOptions    func(configPath string) []loadOption
		setupEnv       func(t *testing.T)
		expectError    bool
		errorContains  string
		validateConfig func(t *testing.T, cfg *Config)
	}{
		{
			name:          "static config without template",
			configContent: staticConfig,
			loadOptions: func(configPath string) []loadOption {
				return []loadOption{WithConfigDir(filepath.Dir(configPath))}
			},
			expectError: false,
			validateConfig: func(t *testing.T, cfg *Config) {
				require.Equal(t, "warn", cfg.Log.Level)
				require.Equal(t, int32(16), cfg.Persistence.NumHistoryShards)
				require.Equal(t, 9233, cfg.Services["frontend"].RPC.GRPCPort)
			},
		},
		{
			name:          "template config with file path uses system env vars",
			configContent: templateConfig,
			loadOptions: func(configPath string) []loadOption {
				return []loadOption{WithConfigFile(configPath)}
			},
			setupEnv: func(t *testing.T) {
				t.Setenv("LOG_LEVEL", "error")
				t.Setenv("NUM_HISTORY_SHARDS", "32")
				t.Setenv("FRONTEND_GRPC_PORT", "7777")
			},
			expectError: false,
			validateConfig: func(t *testing.T, cfg *Config) {
				require.Equal(t, "error", cfg.Log.Level)
				require.Equal(t, int32(32), cfg.Persistence.NumHistoryShards)
				require.Equal(t, 7777, cfg.Services["frontend"].RPC.GRPCPort)
			},
		},
		{
			name:          "invalid yaml returns error",
			configContent: invalidYaml,
			loadOptions: func(configPath string) []loadOption {
				return []loadOption{WithConfigDir(filepath.Dir(configPath))}
			},
			expectError:   true,
			errorContains: "yaml",
		},
		{
			name:          "non-existent directory returns error",
			configContent: "",
			loadOptions: func(configPath string) []loadOption {
				return []loadOption{WithConfigDir("/nonexistent/path")}
			},
			expectError:   true,
			errorContains: "no config files found",
		},
		{
			name:          "non-existent file path returns error",
			configContent: "",
			loadOptions: func(configPath string) []loadOption {
				return []loadOption{WithConfigFile("/nonexistent/path/config.yaml")}
			},
			expectError:   true,
			errorContains: "could not read config file",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var configPath string
			if tc.configContent != "" {
				tempDir := testutils.MkdirTemp(t, "", "load_test")
				configPath = filepath.Join(tempDir, "base.yaml")
				err := os.WriteFile(configPath, []byte(tc.configContent), fileMode)
				require.NoError(t, err)
			}

			if tc.setupEnv != nil {
				tc.setupEnv(t)
			}

			cfg, err := Load(tc.loadOptions(configPath)...)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
				if tc.errorContains == "failed to read config file" {
					require.Nil(t, cfg)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, cfg)
				if tc.validateConfig != nil {
					tc.validateConfig(t, cfg)
				}
			}
		})
	}
}

func createFile(t *testing.T, dir string, file string, uid, uid2 string) {
	err := os.WriteFile(path(dir, file), []byte(buildConfig(uid, uid2)), fileMode)
	require.NoError(t, err)
}

func path(dir string, file string) string {
	return dir + "/" + file
}

func buildConfig(uid, uid2 string) string {
	base := configBase
	if uid != "" {

		base += strings.ReplaceAll(appendItem1, "REP", uid)
	}

	if uid2 != "" {
		base += strings.ReplaceAll(appendItem2, "REP", uid2)
	}
	return base
}

func TestPathResolution(t *testing.T) {
	// this does not test that the fact that env+zone overrides base and retains non-overridden configs
	t.Parallel()
	testCases := []struct {
		name   string
		env    string
		zone   string
		before func(t *testing.T) string
		level  string
		level2 string
	}{
		{
			name: "just base.yaml",
			env:  "",
			zone: "",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "base.yaml", "base", "")
				return dir
			},
			level: "base",
		},
		{
			name: "just base.yaml env and zone defined",
			env:  "prod",
			zone: "east",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "base.yaml", "base", "")
				return dir
			},
			level: "base",
		},
		{
			name: "base.yaml and prod_east.yaml env and zone defined",
			env:  "prod",
			zone: "east",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "base.yaml", "base", "")
				createFile(t, dir, "prod_east.yaml", "prod_east", "")
				return dir
			},
			level: "prod_east",
		},
		{
			name: "prod_east.yaml env and zone defined",
			env:  "prod",
			zone: "east",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "prod_east.yaml", "prod_east", "")
				return dir
			},
			level: "prod_east",
		},
		{
			name: "base.yaml and development.yaml",
			env:  "",
			zone: "",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "base.yaml", "base", "")
				createFile(t, dir, "development.yaml", "development", "")
				return dir
			},
			level: "development",
		},
		{
			name: "base.yaml and development.yaml and development_zone.yaml",
			env:  "",
			zone: "zone",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "base.yaml", "base", "")
				createFile(t, dir, "development.yaml", "development", "")
				createFile(t, dir, "development_zone.yaml", "development_zone", "")
				return dir
			},
			level: "development_zone",
		},
		{
			name: "base.yaml and development.yaml and development_zone.yaml",
			env:  "prod",
			zone: "zone",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "base.yaml", "base", "")
				createFile(t, dir, "development.yaml", "development", "")
				createFile(t, dir, "development_zone.yaml", "development_zone", "")
				createFile(t, dir, "prod_zone.yaml", "prod_zone", "")
				return dir
			},
			level: "prod_zone",
		},
		{
			name: "env->env+zone combined",
			env:  "production",
			zone: "east",
			before: func(t *testing.T) string {
				dir := testutils.MkdirTemp(t, "", "loader.testHierarchy")
				createFile(t, dir, "production.yaml", "base", "SHOULD NOT")
				createFile(t, dir, "production_east.yaml", "", "development")
				return dir
			},
			level:  "base",
			level2: "development",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := tc.before(t)
			cfg, err := Load(
				WithEnv(tc.env),
				WithConfigDir(dir),
				WithZone(tc.zone),
			)
			require.NoError(t, err)
			require.Equal(t, tc.level, cfg.NamespaceDefaults.Archival.History.State)
			if tc.level2 != "" {
				require.Equal(t, tc.level2, cfg.DCRedirectionPolicy.Policy)
			}
		})
	}
}

const appendItem2 = `
dcRedirectionPolicy:
  policy: REP

`
const appendItem1 = `
namespaceDefaults:
  archival:
    history:
      state: REP
      URI: "file:///tmp/temporal_archival/development"

`
const configBase = `
log:
  stdout: true
  level: info 

persistence:
  defaultStore:  mysql-default
  visibilityStore: mysql-visibility
  numHistoryShards: 4
  datastores:
    mysql-default:
      sql:
        pluginName: "mysql8"
        databaseName: "temporal"
        connectAddr: "127.0.0.1:3306"
        connectProtocol: "tcp"
        user: "temporal"
        password: "temporal"
        maxConns: 20
        maxIdleConns: 20
        maxConnLifetime: "1h"
    mysql-visibility:
      sql:
        pluginName: "mysql8"
        databaseName: "temporal_visibility"
        connectAddr: "127.0.0.1:3306"
        connectProtocol: "tcp"
        user: "temporal"
        password: "temporal"
        maxConns: 2
        maxIdleConns: 2
        maxConnLifetime: "1h"

global:
  membership:
    maxJoinDuration: 30s
    broadcastAddress: "127.0.0.1"
  pprof:
    port: 7936
  metrics:
    prometheus:
      framework: "tally"
      timerType: "histogram"
      listenAddress: "127.0.0.1:8000"

services:
  frontend:
    rpc:
      grpcPort: 7233
      membershipPort: 6933
      bindOnLocalHost: true
      httpPort: 7243

  matching:
    rpc:
      grpcPort: 7235
      membershipPort: 6935
      bindOnLocalHost: true

  history:
    rpc:
      grpcPort: 7234
      membershipPort: 6934
      bindOnLocalHost: true

  worker:
    rpc:
      grpcPort: 7239
      membershipPort: 6939
      bindOnLocalHost: true

clusterMetadata:
  enableGlobalNamespace: false
  failoverVersionIncrement: 10
  masterClusterName: "active"
  currentClusterName: "active"
  clusterInformation:
    active:
      enabled: true
      initialFailoverVersion: 1
      rpcName: "frontend"
      rpcAddress: "localhost:7233"

archival:
  history:
    state: "enabled"
    enableRead: true
    provider:
      filestore:
        fileMode: "0666"
        dirMode: "0766"
      gstorage:
        credentialsPath: "/tmp/gcloud/keyfile.json"
  visibility:
    state: "enabled"
    enableRead: true
    provider:
      filestore:
        fileMode: "0666"
        dirMode: "0766"



dynamicConfigClient:
  filepath: "config/dynamicconfig/development-sql.yaml"
  pollInterval: "10s"`
