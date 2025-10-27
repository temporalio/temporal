package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/tests/testutils"
)

const fileMode = os.FileMode(0644)

type (
	LoaderSuite struct {
		*require.Assertions
		suite.Suite
	}

	itemsConfig struct {
		Item1 string `yaml:"item1"`
		Item2 string `yaml:"item2"`
	}

	testConfig struct {
		Items itemsConfig `yaml:"items"`
	}
)

func TestLoaderSuite(t *testing.T) {
	suite.Run(t, new(LoaderSuite))
}

func (s *LoaderSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *LoaderSuite) TestBaseYaml() {
	dir := testutils.MkdirTemp(s.T(), "", "loader.testBaseYaml")

	data := buildConfig(false, "", "")
	err := os.WriteFile(filepath.Join(dir, "base.yaml"), []byte(data), fileMode)
	s.Nil(err)

	envs := []string{"", "prod"}
	zones := []string{"", "us-east-1a"}

	for _, env := range envs {
		for _, zone := range zones {
			var cfg testConfig
			err = Load(env, dir, zone, &cfg)
			s.Nil(err)
			s.Equal("hello__", cfg.Items.Item1)
			s.Equal("world__", cfg.Items.Item2)
		}
	}
}

func (s *LoaderSuite) TestHierarchy() {
	dir := testutils.MkdirTemp(s.T(), "", "loader.testHierarchy")

	s.createFile(dir, "base.yaml", false, "", "")
	s.createFile(dir, "development.yaml", false, "development", "")
	s.createFile(dir, "prod.yaml", true, "prod", "")
	s.createFile(dir, "prod_dca.yaml", true, "prod", "dca")

	testCases := []struct {
		env   string
		zone  string
		item1 string
		item2 string
	}{
		{"", "", "hello_development_", "world_development_"},
		{"", "dca", "hello_development_", "world_development_"},
		{"", "pdx", "hello_development_", "world_development_"},
		{"development", "", "hello_development_", "world_development_"},
		{"development", "dca", "hello_development_", "world_development_"},
		{"development", "pdx", "hello_development_", "world_development_"},
		{"prod", "", "HELLO_PROD_", "world_prod_"},
		{"prod", "dca", "HELLO_PROD_DCA", "world_prod_dca"},
		{"prod", "pdx", "HELLO_PROD_", "world_prod_"},
	}

	for _, tc := range testCases {
		var cfg testConfig
		err := Load(tc.env, dir, tc.zone, &cfg)
		s.Nil(err)
		s.Equal(tc.item1, cfg.Items.Item1)
		s.Equal(tc.item2, cfg.Items.Item2)
	}
}

func (s *LoaderSuite) TestInvalidPath() {
	// Create an empty directory to test that when no config files exist,
	// Load returns an error (no longer falls back to embedded template)
	dir := testutils.MkdirTemp(s.T(), "", "loader.testInvalidPath")

	var cfg testConfig
	err := Load("prod", dir, "", &cfg)
	// Should return an error since no config files exist
	s.Error(err)
	s.Contains(err.Error(), "no config files found")
}

func (s *LoaderSuite) createFile(dir string, file string, template bool, env string, zone string) {
	err := os.WriteFile(filepath.Join(dir, file), []byte(buildConfig(template, env, zone)), fileMode)
	s.Nil(err)
}

func buildConfig(template bool, env, zone string) string {
	comment := ""
	if template {
		comment = "# enable-template\n"
	}
	item1 := concat("hello", concat(env, zone))
	if template {
		item1 = `{{ "` + item1 + `" | upper }}`
	}
	item2 := concat("world", concat(env, zone))
	return comment + `
    items:
      item1: ` + item1 + `
      item2: ` + item2
}

func TestRenderTemplateWithEnvVars(t *testing.T) {
	templateContent := []byte(`# enable-template
log:
  level: {{ default "info" (index .Env "LOG_LEVEL") }}
persistence:
  numHistoryShards: {{ default "4" (index .Env "NUM_HISTORY_SHARDS") }}`)

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

	var cfg Config
	err := LoadWithEnvMap("", "", "", &cfg, envMap, true)
	require.NoError(t, err)

	// Verify embedded template loaded with defaults
	require.Equal(t, "info", cfg.Log.Level)
	require.Equal(t, int32(4), cfg.Persistence.NumHistoryShards)
	require.NotNil(t, cfg.Services["frontend"])
	require.Equal(t, 7233, cfg.Services["frontend"].RPC.GRPCPort)
}

func TestLoadWithEnvVarSubstitution(t *testing.T) {
	tempDir := testutils.MkdirTemp(t, "", "env_var_substitution_test")

	configWithEnvVars := `# enable-template
log:
  level: {{ default "info" (index .Env "LOG_LEVEL") }}
persistence:
  numHistoryShards: {{ default "4" (index .Env "NUM_HISTORY_SHARDS") }}
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
      grpcPort: {{ default "7233" (index .Env "FRONTEND_GRPC_PORT") }}
      bindOnIP: "127.0.0.1"
`

	err := os.WriteFile(filepath.Join(tempDir, "base.yaml"), []byte(configWithEnvVars), fileMode)
	require.NoError(t, err)

	// Test with custom env vars
	envMap := map[string]string{
		"LOG_LEVEL":          "debug",
		"NUM_HISTORY_SHARDS": "8",
		"FRONTEND_GRPC_PORT": "8233",
	}

	var cfg Config
	err = LoadWithEnvMap("development", tempDir, "", &cfg, envMap, false)
	require.NoError(t, err)

	require.Equal(t, "debug", cfg.Log.Level)
	require.Equal(t, int32(8), cfg.Persistence.NumHistoryShards)
	require.Equal(t, 8233, cfg.Services["frontend"].RPC.GRPCPort)
}

// TestLoadConfigFile tests loading a single config file
func TestLoadConfigFile(t *testing.T) {
	t.Run("valid file without template", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_config_file_test")
		configPath := filepath.Join(tempDir, "config.yaml")

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

		cfg, err := LoadConfigFile(configPath)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "warn", cfg.Log.Level)
		require.Equal(t, int32(16), cfg.Persistence.NumHistoryShards)
		require.Equal(t, 9233, cfg.Services["frontend"].RPC.GRPCPort)
	})

	t.Run("valid file with template enabled", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_config_file_template_test")
		configPath := filepath.Join(tempDir, "config.yaml")

		configContent := `# enable-template
log:
  level: {{ default "info" (index .Env "LOG_LEVEL") }}
persistence:
  numHistoryShards: {{ default "4" (index .Env "NUM_HISTORY_SHARDS") }}
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
      grpcPort: {{ default "7233" (index .Env "FRONTEND_GRPC_PORT") }}
      bindOnIP: "127.0.0.1"
`
		err := os.WriteFile(configPath, []byte(configContent), fileMode)
		require.NoError(t, err)

		// Set environment variables for the test
		t.Setenv("LOG_LEVEL", "error")
		t.Setenv("NUM_HISTORY_SHARDS", "32")
		t.Setenv("FRONTEND_GRPC_PORT", "8888")

		cfg, err := LoadConfigFile(configPath)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "error", cfg.Log.Level)
		require.Equal(t, int32(32), cfg.Persistence.NumHistoryShards)
		require.Equal(t, 8888, cfg.Services["frontend"].RPC.GRPCPort)
	})

	t.Run("non-existent file", func(t *testing.T) {
		cfg, err := LoadConfigFile("/nonexistent/path/config.yaml")
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "failed to read config file")
	})

	t.Run("invalid yaml", func(t *testing.T) {
		tempDir := testutils.MkdirTemp(t, "", "load_config_file_invalid_test")
		configPath := filepath.Join(tempDir, "invalid.yaml")

		invalidContent := `log:
  level: warn
  invalid indentation
    bad: yaml
`
		err := os.WriteFile(configPath, []byte(invalidContent), fileMode)
		require.NoError(t, err)

		cfg, err := LoadConfigFile(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "failed to unmarshal config file")
	})
}
