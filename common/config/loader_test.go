package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/testutils"
)

const fileMode = os.FileMode(0644)

func TestRenderTemplateWithEnvVars(t *testing.T) {
	templateContent := []byte(`# enable-template
log:
  level: {{ default .Env.LOG_LEVEL "info" }}
persistence:
  numHistoryShards: {{ default .Env.NUM_HISTORY_SHARDS "4" }}`)

	testCases := []struct {
		name              string
		envMap            map[string]string
		expectedLogLevel  string
		expectedNumShards string
	}{
		{
			name: "with environment variables set",
			envMap: map[string]string{
				"LOG_LEVEL":          "debug",
				"NUM_HISTORY_SHARDS": "8",
			},
			expectedLogLevel:  "debug",
			expectedNumShards: "8",
		},
		{
			name:              "with no environment variables - uses defaults",
			envMap:            map[string]string{},
			expectedLogLevel:  "info",
			expectedNumShards: "4",
		},
		{
			name: "with partial environment variables",
			envMap: map[string]string{
				"LOG_LEVEL": "warn",
			},
			expectedLogLevel:  "warn",
			expectedNumShards: "4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rendered, err := renderTemplate(templateContent, "test.yaml", tc.envMap)
			require.NoError(t, err)

			renderedStr := string(rendered)
			require.Contains(t, renderedStr, "level: "+tc.expectedLogLevel)
			require.Contains(t, renderedStr, "numHistoryShards: "+tc.expectedNumShards)
		})
	}
}

func TestProcessConfigFile(t *testing.T) {
	t.Run("without template enabled", func(t *testing.T) {
		content := []byte(`log:
  level: info`)

		envMap := map[string]string{"LOG_LEVEL": "debug"}
		processed, err := processConfigFile(content, "test.yaml", envMap)
		require.NoError(t, err)
		require.Equal(t, content, processed) // Should be unchanged
	})

	t.Run("with template enabled", func(t *testing.T) {
		content := []byte(`# enable-template
log:
  level: {{ .Env.LOG_LEVEL }}`)

		envMap := map[string]string{"LOG_LEVEL": "debug"}
		processed, err := processConfigFile(content, "test.yaml", envMap)
		require.NoError(t, err)
		require.Contains(t, string(processed), "level: debug")
	})
}

func TestLoadWithEmbeddedTemplate(t *testing.T) {
	envMap := map[string]string{
		"DB":             "postgres12",
		"POSTGRES_SEEDS": "localhost",
	}

	cfg, err := Load(WithEmbedded(), WithEnvMap(envMap))
	require.NoError(t, err)

	// Verify embedded template loaded with defaults
	require.Equal(t, "info", cfg.Log.Level)
	require.Equal(t, int32(4), cfg.Persistence.NumHistoryShards)
	require.NotNil(t, cfg.Services["frontend"])
	require.Equal(t, 7233, cfg.Services["frontend"].RPC.GRPCPort)
}

func TestLoadConfigFile(t *testing.T) {
	t.Run("without template", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_config_file_test")
		configPath := filepath.Join(tempDir, "base.yaml")

		configContent := `log:
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
		err := os.WriteFile(configPath, []byte(configContent), fileMode)
		require.NoError(t, err)

		cfg, err := Load(WithConfigDir(tempDir))
		require.NoError(t, err)
		require.Equal(t, "warn", cfg.Log.Level)
		require.Equal(t, int32(16), cfg.Persistence.NumHistoryShards)
		require.Equal(t, 9233, cfg.Services["frontend"].RPC.GRPCPort)
	})

	t.Run("with template and custom env vars", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_config_file_template_test")
		configPath := filepath.Join(tempDir, "base.yaml")

		configContent := `# enable-template
log:
  level: {{ default .Env.LOG_LEVEL "info" }}
persistence:
  numHistoryShards: {{ default .Env.NUM_HISTORY_SHARDS "4" }}
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
      grpcPort: {{ default .Env.FRONTEND_GRPC_PORT "7233" }}
      bindOnIP: "127.0.0.1"
`
		err := os.WriteFile(configPath, []byte(configContent), fileMode)
		require.NoError(t, err)

		envMap := map[string]string{
			"LOG_LEVEL":          "debug",
			"NUM_HISTORY_SHARDS": "8",
			"FRONTEND_GRPC_PORT": "8233",
		}
		cfg, err := Load(WithConfigDir(tempDir), WithEnvMap(envMap))
		require.NoError(t, err)
		require.Equal(t, "debug", cfg.Log.Level)
		require.Equal(t, int32(8), cfg.Persistence.NumHistoryShards)
		require.Equal(t, 8233, cfg.Services["frontend"].RPC.GRPCPort)
	})

	t.Run("with template and defaults", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_config_file_template_defaults_test")
		configPath := filepath.Join(tempDir, "base.yaml")

		configContent := `# enable-template
log:
  level: {{ default .Env.LOG_LEVEL "info" }}
persistence:
  numHistoryShards: {{ default .Env.NUM_HISTORY_SHARDS "4" }}
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
      grpcPort: {{ default .Env.FRONTEND_GRPC_PORT "7233" }}
      bindOnIP: "127.0.0.1"
`
		err := os.WriteFile(configPath, []byte(configContent), fileMode)
		require.NoError(t, err)

		cfg, err := Load(WithConfigDir(tempDir))
		require.NoError(t, err)
		require.Equal(t, "info", cfg.Log.Level)
		require.Equal(t, int32(4), cfg.Persistence.NumHistoryShards)
		require.Equal(t, 7233, cfg.Services["frontend"].RPC.GRPCPort)
	})

	t.Run("non-existent directory", func(t *testing.T) {
		_, err := Load(WithConfigDir("/nonexistent/path"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "no config files found")
	})

	t.Run("non-existent file path", func(t *testing.T) {
		cfg, err := LoadConfigFile("/nonexistent/path/config.yaml")
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "failed to read config file")
	})

	t.Run("single file with LoadConfigFile", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_single_config_file_test")
		configPath := filepath.Join(tempDir, "my-config.yaml")

		configContent := `# enable-template
log:
  level: {{ default .Env.TEST_LOG_LEVEL "warn" }}
persistence:
  numHistoryShards: 32
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
      grpcPort: 7777
      bindOnIP: "0.0.0.0"
`
		err := os.WriteFile(configPath, []byte(configContent), fileMode)
		require.NoError(t, err)

		t.Setenv("TEST_LOG_LEVEL", "error")

		cfg, err := LoadConfigFile(configPath)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "error", cfg.Log.Level)
		require.Equal(t, int32(32), cfg.Persistence.NumHistoryShards)
		require.Equal(t, 7777, cfg.Services["frontend"].RPC.GRPCPort)
	})

	t.Run("invalid yaml", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_config_file_invalid_test")
		configPath := filepath.Join(tempDir, "base.yaml")

		invalidContent := `log:
  level: warn
  invalid indentation
    bad: yaml
`
		err := os.WriteFile(configPath, []byte(invalidContent), fileMode)
		require.NoError(t, err)

		_, err = Load(WithConfigDir(tempDir))
		require.Error(t, err)
		require.Contains(t, err.Error(), "yaml")
	})
}
