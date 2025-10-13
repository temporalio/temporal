package config

import (
	"bufio"
	"bytes"
	_ "embed"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"gopkg.in/yaml.v3"
)

//go:embed config_template_embedded.yaml
var embeddedConfigTemplate []byte

const (
	// EnvKeyRoot the environment variable key for runtime root dir
	EnvKeyRoot = "TEMPORAL_ROOT"
	// EnvKeyConfigDir the environment variable key for config dir
	EnvKeyConfigDir = "TEMPORAL_CONFIG_DIR"
	// EnvKeyEnvironment is the environment variable key for environment
	EnvKeyEnvironment = "TEMPORAL_ENVIRONMENT"
	// EnvKeyAvailabilityZone is the environment variable key for AZ
	EnvKeyAvailabilityZone = "TEMPORAL_AVAILABILITY_ZONE"
	// EnvKeyAvailabilityZoneTypo is the old environment variable key for AZ that
	// included a typo. This is deprecated and only here to support backwards
	// compatibility.
	EnvKeyAvailabilityZoneTypo = "TEMPORAL_AVAILABILTY_ZONE"
	// EnvKeyAllowNoAuth is the environment variable key for setting no authorizer
	EnvKeyAllowNoAuth = "TEMPORAL_ALLOW_NO_AUTH"
)

const (
	baseFile           = "base.yaml"
	envDevelopment     = "development"
	defaultConfigDir   = "config"
	enableTemplate     = "enable-template"
	commentSearchLimit = 1024
)

// Load loads the configuration from a set of
// yaml config files found in the config directory
//
// The loader first fetches the set of files matching
// a pre-determined naming convention, then sorts
// them by hierarchy order and after that, simply
// loads the files one after another with the
// key/values in the later files overriding the key/values
// in the earlier files
//
// The hierarchy is as follows from lowest to highest
//
//	base.yaml
//	    env.yaml   -- environment is one of the input params ex-development
//	      env_az.yaml -- zone is another input param
func Load(env string, configDir string, zone string, config interface{}) error {
	return LoadWithEnvMap(env, configDir, zone, config, getEnvMap())
}

// LoadWithEnvMap loads configuration with a specific environment variable map.
// This is useful for testing with controlled environment variables.
func LoadWithEnvMap(env string, configDir string, zone string, config interface{}, envMap map[string]string) error {
	if len(env) == 0 {
		env = envDevelopment
	}
	if len(configDir) == 0 {
		configDir = defaultConfigDir
	}

	// TODO: remove log dependency.
	stdlog.Printf("Loading config; env=%v,zone=%v,configDir=%v\n", env, zone, configDir)

	files, err := getConfigFiles(env, configDir, zone)
	if err != nil {
		// If no config files found, try to use embedded template
		stdlog.Printf("No config files found, attempting to use embedded template\n")

		// Process the embedded template
		processedData, procErr := processConfigFile(embeddedConfigTemplate, "config_template_embedded.yaml", envMap)
		if procErr != nil {
			return fmt.Errorf("failed to process embedded config template: %w", procErr)
		}

		err = yaml.Unmarshal(processedData, config)
		if err != nil {
			return fmt.Errorf("failed to unmarshal embedded config template: %w", err)
		}

		validate := newValidator()
		return validate.Validate(config)
	}

	// TODO: remove log dependency.
	stdlog.Printf("Loading config files=%v\n", files)

	for _, f := range files {
		// This is tagged nosec because the file names being read are for config files that are not user supplied
		// #nosec
		data, err := os.ReadFile(f)
		if err != nil {
			return err
		}

		// Process the file (with optional template rendering)
		processedData, err := processConfigFile(data, filepath.Base(f), envMap)
		if err != nil {
			return err
		}

		err = yaml.Unmarshal(processedData, config)
		if err != nil {
			return err
		}
	}

	validate := newValidator()
	return validate.Validate(config)
}

// processConfigFile processes a config file, rendering it as a template if enabled
func processConfigFile(data []byte, filename string, envMap map[string]string) ([]byte, error) {
	// If the config file contains "enable-template" in a comment within the first 1KB, then
	// we will treat the file as a template and render it.
	templating, err := checkTemplatingEnabled(data)
	if err != nil {
		return nil, err
	}

	if !templating {
		return data, nil
	}

	stdlog.Printf("using templating")
	return renderTemplate(data, filename, envMap)
}

// renderTemplate renders a config file as a Go template with environment variables
func renderTemplate(data []byte, filename string, envMap map[string]string) ([]byte, error) {
	templateFuncs := sprig.FuncMap()
	templateData := map[string]interface{}{
		"Env": envMap,
	}

	tpl, err := template.New(filename).Funcs(templateFuncs).Parse(string(data))
	if err != nil {
		return nil, err
	}

	var rendered bytes.Buffer
	err = tpl.Execute(&rendered, templateData)
	if err != nil {
		return nil, err
	}

	return rendered.Bytes(), nil
}

// Helper function for loading configuration
func LoadConfig(env string, configDir string, zone string) (*Config, error) {
	config := Config{}
	err := Load(env, configDir, zone, &config)
	if err != nil {
		return nil, fmt.Errorf("config file corrupted: %w", err)
	}
	return &config, nil
}

func checkTemplatingEnabled(content []byte) (bool, error) {
	scanner := bufio.NewScanner(io.LimitReader(bytes.NewReader(content), commentSearchLimit))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "#") && strings.Contains(line, enableTemplate) {
			return true, nil
		}
	}

	return false, scanner.Err()
}

// getConfigFiles returns the list of config files to
// process in the hierarchy order
func getConfigFiles(env string, configDir string, zone string) ([]string, error) {

	candidates := []string{
		path(configDir, baseFile),
		path(configDir, file(env, "yaml")),
	}

	if len(zone) > 0 {
		f := file(concat(env, zone), "yaml")
		candidates = append(candidates, path(configDir, f))
	}

	var result []string

	for _, c := range candidates {
		if _, err := os.Stat(c); err != nil {
			continue
		}
		result = append(result, c)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no config files found within %v", configDir)
	}

	return result, nil
}

func concat(a, b string) string {
	return a + "_" + b
}

func file(name string, suffix string) string {
	return name + "." + suffix
}

func path(dir string, file string) string {
	return dir + "/" + file
}

// getEnvMap returns all environment variables as a map for template access
func getEnvMap() map[string]string {
	envMap := make(map[string]string)
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}
	return envMap
}
