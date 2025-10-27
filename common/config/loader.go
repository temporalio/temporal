package config

import (
	"bufio"
	"bytes"
	_ "embed"
	"errors"
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

var (
	// ErrConfigFilesNotFound is returned when no config files are found in the specified directory
	ErrConfigFilesNotFound = errors.New("no config files found")
)

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
	// EnvKeyConfigFile is the environment variable key for specifying a config file path
	EnvKeyConfigFile = "TEMPORAL_SERVER_CONFIG_FILE_PATH"
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
func Load(env string, configDir string, zone string, config any) error {
	return LoadWithEnvMap(env, configDir, zone, config, getEnvMap(), false)
}

// LoadFromEnv loads the configuration using only the embedded template and environment variables.
func LoadFromEnv(config any) error {
	return LoadWithEnvMap("", "", "", config, getEnvMap(), true)
}

// LoadWithEnvMap loads configuration with a specific environment variable map.
// If useEmbeddedOnly is true, it will skip config file loading and use only the embedded template.
func LoadWithEnvMap(env string, configDir string, zone string, config any, envMap map[string]string, useEmbeddedOnly bool) error {
	// If using embedded template only, skip file loading
	if useEmbeddedOnly {
		stdlog.Println("Loading configuration from environment variables only")

		// Process the embedded template
		processedData, procErr := processConfigFile(embeddedConfigTemplate, "config_template_embedded.yaml", envMap)
		if procErr != nil {
			return fmt.Errorf("failed to process embedded config template: %w", procErr)
		}

		err := yaml.Unmarshal(processedData, config)
		if err != nil {
			return fmt.Errorf("failed to unmarshal embedded config template: %w", err)
		}

		validate := newValidator()
		return validate.Validate(config)
	}

	if env == "" {
		env = envDevelopment
	}
	if configDir == "" {
		configDir = defaultConfigDir
	}

	stdlog.Printf("Loading config; env=%v,zone=%v,configDir=%v\n", env, zone, configDir)

	files, err := getConfigFiles(env, configDir, zone)
	if err != nil {
		// Return error immediately - no fallback to embedded template
		return fmt.Errorf("failed to get config files: %w", err)
	}

	stdlog.Printf("Loading config files=%v\n", files)

	for _, f := range files {
		// This is tagged nosec because the file names being read are for config files that are not user supplied
		// #nosec
		data, err := os.ReadFile(f)
		if err != nil {
			return err
		}

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

	stdlog.Printf("Processing config file as template; filename=%v\n", filename)
	return renderTemplate(data, filename, envMap)
}

// templateContext mimics dockerize's Context struct to support .Env.VAR_NAME syntax.
// In dockerize, .Env is a method that returns the environment map, allowing dot-based access.
type templateContext struct {
	envMap map[string]string
}

// Env returns the environment variable map, matching dockerize's Context.Env() method.
// This allows templates to use .Env.VAR_NAME syntax for environment variable access.
func (c *templateContext) Env() map[string]string {
	return c.envMap
}

// defaultValue implements dockerize-compatible default handling.
// This properly handles nil values from missing map keys when using .Env.VAR syntax.
// Args order: value first, default second (e.g., {{ default .Env.VAR "fallback" }})
func defaultValue(args ...interface{}) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("default called with no values!")
	}

	if len(args) > 0 {
		if args[0] != nil {
			return args[0].(string), nil
		}
	}

	if len(args) > 1 {
		if args[1] == nil {
			return "", fmt.Errorf("default called with nil default value!")
		}

		if _, ok := args[1].(string); !ok {
			return "", fmt.Errorf("default is not a string value. hint: surround it w/ double quotes.")
		}

		return args[1].(string), nil
	}

	return "", fmt.Errorf("default called with no default value")
}

// renderTemplate renders a config file as a Go template with environment variables.
// It uses dockerize-compatible template functions and supports .Env.VAR syntax.
func renderTemplate(data []byte, filename string, envMap map[string]string) ([]byte, error) {
	templateFuncs := sprig.FuncMap()
	// Override sprig's default with dockerize's implementation that properly handles
	// nil values from missing environment variables
	templateFuncs["default"] = defaultValue

	// Create a context with Env() method that returns the environment map
	// Templates access environment variables using .Env.VAR_NAME syntax
	ctx := &templateContext{envMap: envMap}

	tpl, err := template.New(filename).Funcs(templateFuncs).Parse(string(data))
	if err != nil {
		return nil, err
	}

	var rendered bytes.Buffer
	err = tpl.Execute(&rendered, ctx)
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

// LoadConfigFile loads configuration from a single specified file path.
// Template rendering is enabled if the file contains "# enable-template" comment.
func LoadConfigFile(filePath string) (*Config, error) {
	stdlog.Printf("Loading configuration from file; filePath=%v\n", filePath)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	processedData, err := processConfigFile(data, filepath.Base(filePath), getEnvMap())
	if err != nil {
		return nil, fmt.Errorf("failed to process config file %s: %w", filePath, err)
	}

	config := &Config{}
	err = yaml.Unmarshal(processedData, config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file %s: %w", filePath, err)
	}

	validate := newValidator()
	if err := validate.Validate(config); err != nil {
		return nil, fmt.Errorf("config file validation failed for %s: %w", filePath, err)
	}

	return config, nil
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
// process in the hierarchy order.
// Returns ErrConfigFilesNotFound if no config files are found.
// Returns other errors if there are permission or I/O issues accessing the files.
func getConfigFiles(env string, configDir string, zone string) ([]string, error) {

	// Pre-allocate candidates slice with maximum possible size (3)
	candidates := make([]string, 2, 3)
	candidates[0] = filepath.Join(configDir, baseFile)
	candidates[1] = filepath.Join(configDir, file(env, "yaml"))

	if zone != "" {
		f := file(concat(env, zone), "yaml")
		candidates = append(candidates, filepath.Join(configDir, f))
	}

	// Pre-allocate result with capacity matching candidates
	result := make([]string, 0, len(candidates))
	var firstNonNotExistError error

	for _, c := range candidates {
		_, err := os.Stat(c)
		if err == nil {
			// File exists, add it to results
			result = append(result, c)
			continue
		}

		if !os.IsNotExist(err) && firstNonNotExistError == nil {
			firstNonNotExistError = fmt.Errorf("error accessing config file %s: %w", c, err)
		}
	}

	if firstNonNotExistError != nil {
		return nil, firstNonNotExistError
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("%w in directory: %s", ErrConfigFilesNotFound, configDir)
	}

	return result, nil
}

func concat(a, b string) string {
	return a + "_" + b
}

func file(name string, suffix string) string {
	return name + "." + suffix
}

func getEnvMap() map[string]string {
	environ := os.Environ()
	envMap := make(map[string]string, len(environ))

	for _, env := range environ {
		key, value, found := strings.Cut(env, "=")
		if found && key != "" {
			envMap[key] = value
		}
	}
	return envMap
}
