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
type loadOptions struct {
	env             string
	configDir       string
	zone            string
	configFilePath  string
	useEmbeddedOnly bool
	envMap          map[string]string
}

type loadOption func(*loadOptions)

func WithEnv(env string) loadOption {
	return func(o *loadOptions) {
		if env != "" {
			o.env = env
		}
	}
}

func WithConfigDir(configDir string) loadOption {
	return func(o *loadOptions) {
		if configDir != "" {
			o.configDir = configDir
		}
	}
}

func WithZone(zone string) loadOption {
	return func(o *loadOptions) {
		if zone != "" {
			o.zone = zone
		}
	}
}

func WithConfigFile(configFilePath string) loadOption {
	return func(o *loadOptions) {
		if configFilePath != "" {
			o.configFilePath = configFilePath
		}
	}
}

func WithEmbedded() loadOption {
	return func(o *loadOptions) {
		o.useEmbeddedOnly = true
	}
}

func WithEnvMap(envMap map[string]string) loadOption {
	return func(o *loadOptions) {
		if envMap != nil {
			o.envMap = envMap
		}
	}
}

func Load(opts ...loadOption) (*Config, error) {
	cfg := &Config{}
	options := &loadOptions{
		envMap: getEnvMap(),
	}

	for _, opt := range opts {
		opt(options)
	}

	if err := load(options, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func load(opts *loadOptions, config any) error {
	if opts.envMap == nil {
		opts.envMap = getEnvMap()
	}

	if opts.useEmbeddedOnly {
		stdlog.Println("Loading configuration from environment variables only")
		return loadAndUnmarshalContent(embeddedConfigTemplate, "config_template_embedded.yaml", opts.envMap, config)
	}

	if opts.configFilePath != "" {
		content, err := readConfigFile(opts.configFilePath)
		if err != nil {
			return err
		}
		return loadAndUnmarshalContent(content, filepath.Base(opts.configFilePath), opts.envMap, config)
	}
	return loadLegacy(opts, config)

}

// loadLegacy loads configuration data from a set of YAML files
// located in the config directory.
//
// Deprecated: This loader is maintained only for backward compatibility
// and should not be used in new code.
//
// The loader first identifies all files matching a predefined
// naming convention, then sorts them according to their hierarchy.
// It then loads the files sequentially, with key/value pairs in
// later files overriding those in earlier ones.
//
// The hierarchy, from lowest to highest precedence, is as follows:
//
//   base.yaml
//     env.yaml     -- where "environment" is one of the input parameters (e.g., "development")
//       env_az.yaml -- where "zone" is another input parameter

func loadLegacy(opts *loadOptions, config any) error {
	stdlog.Printf("Loading config; env=%v,zone=%v,configDir=%v\n", opts.env, opts.zone, opts.configDir)
	if opts.env == "" {
		opts.env = envDevelopment
	}
	if opts.configDir == "" {
		opts.configDir = defaultConfigDir
	}

	stdlog.Printf("Loading config; env=%v,zone=%v,configDir=%v\n", opts.env, opts.zone, opts.configDir)

	files, err := getConfigFiles(opts.env, opts.configDir, opts.zone)
	if err != nil {
		return fmt.Errorf("failed to get config files: %w", err)
	}

	stdlog.Printf("Loading config files=%v\n", files)

	for _, f := range files {
		data, err := readConfigFile(f)
		if err != nil {
			return err
		}

		processedData, err := processConfigFile(data, filepath.Base(f), opts.envMap)
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

func readConfigFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %s. error: %w", path, err)

	}
	return data, nil
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
func defaultValue(args ...any) (string, error) {
	if len(args) == 0 {
		return "", errors.New("default called with no values")
	}

	if len(args) > 0 {
		if args[0] != nil {
			val, ok := args[0].(string)
			if !ok {
				return "", errors.New("first argument is not a string")
			}
			return val, nil
		}
	}

	if len(args) > 1 {
		if args[1] == nil {
			return "", errors.New("default called with nil default value")
		}

		val, ok := args[1].(string)
		if !ok {
			return "", errors.New("default is not a string value, hint: surround it w/ double quotes")
		}

		return val, nil
	}

	return "", errors.New("default called with no default value")
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

func LoadConfig(env string, configDir string, zone string) (*Config, error) {
	cfg, err := Load(WithEnv(env), WithConfigDir(configDir), WithZone(zone))
	if err != nil {
		return nil, fmt.Errorf("config file corrupted: %w", err)
	}
	return cfg, nil
}

func loadAndUnmarshalContent(content []byte, filename string, envMap map[string]string, config any) error {
	processed, err := processConfigFile(content, filename, envMap)
	if err != nil {
		return fmt.Errorf("failed to process config file %s: %w", filename, err)
	}

	if err := yaml.Unmarshal(processed, config); err != nil {
		return fmt.Errorf("failed to unmarshal config file %s: %w", filename, err)
	}

	validate := newValidator()
	return validate.Validate(config)
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
	candidates := make([]string, 2, 3)
	candidates[0] = filepath.Join(configDir, baseFile)
	candidates[1] = filepath.Join(configDir, file(env, "yaml"))

	if zone != "" {
		f := file(concat(env, zone), "yaml")
		candidates = append(candidates, filepath.Join(configDir, f))
	}

	result := make([]string, 0, len(candidates))

	for _, c := range candidates {
		_, err := os.Stat(c)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("error accessing config file %s: %w", c, err)
		}
		result = append(result, c)
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
