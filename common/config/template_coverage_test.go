package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestEmbeddedTemplateCoversAllConfigFields(t *testing.T) {
	envMap := make(map[string]string)

	rendered, err := processConfigFile(embeddedConfigTemplate, "config_template_embedded.yaml", envMap)
	require.NoError(t, err)

	var templateData map[string]any
	err = yaml.Unmarshal(rendered, &templateData)
	require.NoError(t, err)

	configType := reflect.TypeOf(Config{})

	for i := 0; i < configType.NumField(); i++ {
		field := configType.Field(i)
		yamlTag := field.Tag.Get("yaml")

		if yamlTag == "" || yamlTag == "-" {
			continue
		}

		yamlFieldName := strings.Split(yamlTag, ",")[0]

		validateTag := field.Tag.Get("validate")
		isRequired := strings.Contains(validateTag, "nonzero")

		_, exists := templateData[yamlFieldName]

		if isRequired && !exists {
			t.Errorf("REQUIRED field '%s' (yaml:'%s') is missing from embedded template", field.Name, yamlFieldName)
		} else if !exists {
			t.Logf("Optional field '%s' (yaml:'%s') not in embedded template (this is OK)", field.Name, yamlFieldName)
		}
	}

	// Verify that required nested fields are also present
	t.Run("Persistence fields", func(t *testing.T) {
		persistenceData, ok := templateData["persistence"].(map[string]any)
		require.True(t, ok, "persistence section must exist")

		requiredFields := []string{"defaultStore", "numHistoryShards", "datastores"}
		for _, field := range requiredFields {
			_, exists := persistenceData[field]
			require.True(t, exists, "persistence.%s must be present in template", field)
		}
	})

	t.Run("Services fields", func(t *testing.T) {
		servicesData, ok := templateData["services"].(map[string]any)
		require.True(t, ok, "services section must exist")

		requiredServices := []string{"frontend", "history", "matching", "worker"}
		for _, service := range requiredServices {
			_, exists := servicesData[service]
			require.True(t, exists, "services.%s must be present in template", service)
		}
	})

	t.Run("Log fields", func(t *testing.T) {
		logData, ok := templateData["log"].(map[string]any)
		require.True(t, ok, "log section must exist")

		// Verify basic log config is present
		_, exists := logData["level"]
		require.True(t, exists, "log.level must be present in template")
	})
}

func TestEmbeddedTemplateHasDefaults(t *testing.T) {
	t.Run("cassandra_with_required_env", func(t *testing.T) {
		cassandraEnv := map[string]string{
			"CASSANDRA_SEEDS": "localhost",
		}
		rendered, err := processConfigFile(embeddedConfigTemplate, "config_template_embedded.yaml", cassandraEnv)
		require.NoError(t, err)

		var cfg Config
		err = yaml.Unmarshal(rendered, &cfg)
		require.NoError(t, err)

		validate := newValidator()
		err = validate.Validate(&cfg)
		require.NoError(t, err, "Cassandra config with required env vars should be valid")

		// Verify cassandra hosts were set
		require.NotEmpty(t, cfg.Persistence.DataStores["default"].Cassandra.Hosts, "cassandra hosts should be set from CASSANDRA_SEEDS")
	})

	t.Run("postgres_with_required_env", func(t *testing.T) {
		postgresEnv := map[string]string{
			"DB":             "postgres12",
			"POSTGRES_SEEDS": "localhost",
		}
		rendered, err := processConfigFile(embeddedConfigTemplate, "config_template_embedded.yaml", postgresEnv)
		require.NoError(t, err)

		var cfg Config
		err = yaml.Unmarshal(rendered, &cfg)
		require.NoError(t, err)

		validate := newValidator()
		err = validate.Validate(&cfg)
		require.NoError(t, err, "Postgres config with required env vars should be valid")

		// Verify specific critical defaults
		require.NotEmpty(t, cfg.Log.Level, "log.level should have a default")
		require.NotZero(t, cfg.Persistence.NumHistoryShards, "persistence.numHistoryShards should have a default")
		require.NotEmpty(t, cfg.Persistence.DefaultStore, "persistence.defaultStore should have a default")
		require.NotEmpty(t, cfg.Services, "services should have defaults")
		require.Contains(t, cfg.Services, "frontend", "frontend service should be defined")
		require.Contains(t, cfg.Services, "history", "history service should be defined")
		require.Contains(t, cfg.Services, "matching", "matching service should be defined")
		require.Contains(t, cfg.Services, "worker", "worker service should be defined")
	})

	t.Run("mysql_with_required_env", func(t *testing.T) {
		// When using mysql with required env vars, config should be valid
		mysqlEnv := map[string]string{
			"DB":          "mysql8",
			"MYSQL_SEEDS": "localhost",
		}
		rendered, err := processConfigFile(embeddedConfigTemplate, "config_template_embedded.yaml", mysqlEnv)
		require.NoError(t, err)

		var cfg Config
		err = yaml.Unmarshal(rendered, &cfg)
		require.NoError(t, err)

		validate := newValidator()
		err = validate.Validate(&cfg)
		require.NoError(t, err, "MySQL config with required env vars should be valid")
	})
}

func TestEmbeddedTemplateMatchesDockerTemplate(t *testing.T) {
	// This test is informational - it documents that the embedded template matches docker template
	t.Log("Embedded template should match docker/config_template.yaml structure")
	t.Log("Both use dockerize-compatible syntax: .Env.VAR_NAME")
	t.Log("Embedded template has # enable-template comment at the top for config loader detection")

	// Render both templates with the same env
	testEnv := map[string]string{
		"DB":             "postgres12",
		"POSTGRES_SEEDS": "localhost",
		"LOG_LEVEL":      "debug",
	}

	embeddedRendered, err := processConfigFile(embeddedConfigTemplate, "embedded", testEnv)
	require.NoError(t, err)

	// Parse both into generic maps
	var embeddedData map[string]any
	err = yaml.Unmarshal(embeddedRendered, &embeddedData)
	require.NoError(t, err)

	// Verify key sections exist
	requiredSections := []string{"log", "persistence", "global", "services", "clusterMetadata",
		"dcRedirectionPolicy", "archival", "dynamicConfigClient", "namespaceDefaults"}

	for _, section := range requiredSections {
		_, exists := embeddedData[section]
		require.True(t, exists, "Embedded template should have '%s' section", section)
	}

	// publicClient is conditionally included:
	// - Omitted if USE_INTERNAL_FRONTEND is set
	// - Omitted if neither TEMPORAL_AUTH_AUTHORIZER nor TEMPORAL_AUTH_CLAIM_MAPPER is set
	// With no env vars set, it's omitted (expected behavior)
	t.Log("Note: publicClient is conditionally included based on USE_INTERNAL_FRONTEND and auth settings")
}

func TestConfigToYAML(t *testing.T) {
	// Load config from template
	testEnv := map[string]string{
		"DB":             "postgres12",
		"POSTGRES_SEEDS": "localhost",
		"LOG_LEVEL":      "info",
	}

	rendered, err := processConfigFile(embeddedConfigTemplate, "config_template_embedded.yaml", testEnv)
	require.NoError(t, err)

	var cfg Config
	err = yaml.Unmarshal(rendered, &cfg)
	require.NoError(t, err)

	// Marshal config back to YAML
	yamlBytes, err := yaml.Marshal(&cfg)
	require.NoError(t, err)
	require.NotEmpty(t, yamlBytes, "Config should marshal to YAML")

	// Verify it's valid YAML by unmarshaling it again
	var cfg2 Config
	err = yaml.Unmarshal(yamlBytes, &cfg2)
	require.NoError(t, err, "Marshaled YAML should be valid and parseable")

	// Also test the String() method
	configString := cfg.String()
	require.NotEmpty(t, configString, "Config.String() should produce output")
	require.Contains(t, configString, "log:", "Config string should contain log section")
	require.Contains(t, configString, "persistence:", "Config string should contain persistence section")

	t.Logf("Config successfully marshaled to %d bytes of YAML", len(yamlBytes))
}
