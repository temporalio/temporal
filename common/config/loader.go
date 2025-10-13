package config

import (
	"bufio"
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

//go:embed config_template_embedded.yaml
var embeddedConfigTemplate []byte

var (
	// ErrConfigFilesNotFound is returned when no config files are found in the specified directory
	ErrConfigFilesNotFound = errors.New("no config files found")

	// logger is a structured logger used during config loading (bootstrap phase).
	// Uses zap with console encoding to match Temporal's standard logging format.
	// Initialized lazily using sync.Once for thread-safety.
	logger     *zap.Logger
	loggerOnce sync.Once
)

// getLogger returns the bootstrap logger, initializing it lazily for thread-safety.
func getLogger() *zap.Logger {
	loggerOnce.Do(func() {
		logger = newBootstrapLogger()
	})
	return logger
}

// newBootstrapLogger creates a zap logger for config loading with console encoding
// that matches Temporal's standard log format.
// Panics if the logger cannot be created, as logging is essential for bootstrap.
func newBootstrapLogger() *zap.Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "logging-call-at",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	config := zap.Config{
		Level:         zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:   false,
		Encoding:      "console",
		EncoderConfig: encoderConfig,
		OutputPaths:   []string{"stderr"},
	}

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to create bootstrap logger: %v", err))
	}
	return logger
}

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
	return LoadWithEnvMap(env, configDir, zone, config, getEnvMap(), false)
}

// LoadFromEnv loads the configuration using only the embedded template and environment variables.
// This ignores any config files on disk and relies entirely on environment variable substitution.
func LoadFromEnv(config interface{}) error {
	return LoadWithEnvMap("", "", "", config, getEnvMap(), true)
}

// LoadWithEnvMap loads configuration with a specific environment variable map.
// This is useful for testing with controlled environment variables.
// If useEmbeddedOnly is true, it will skip config file loading and use only the embedded template.
func LoadWithEnvMap(env string, configDir string, zone string, config interface{}, envMap map[string]string, useEmbeddedOnly bool) error {
	// If using embedded template only, skip file loading
	if useEmbeddedOnly {
		getLogger().Info("Loading configuration from environment variables only")

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

	getLogger().Info("Loading configuration",
		zap.String("env", env),
		zap.String("zone", zone),
		zap.String("configDir", configDir),
	)

	files, err := getConfigFiles(env, configDir, zone)
	if err != nil {
		// Return error immediately - no fallback to embedded template
		return fmt.Errorf("failed to get config files: %w", err)
	}

	getLogger().Info("Config files found",
		zap.Strings("files", files),
		zap.Int("count", len(files)),
	)

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

	getLogger().Debug("Processing config file as template",
		zap.String("filename", filename),
	)
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

		// If the error is NOT "file not found", it could be a permission issue,
		// I/O error, or other problem that we should report
		if !os.IsNotExist(err) && firstNonNotExistError == nil {
			firstNonNotExistError = fmt.Errorf("error accessing config file %s: %w", c, err)
		}
	}

	// If we encountered a non-NotExist error (like permission denied), return it
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

// getEnvMap returns all environment variables as a map for template access.
// Environment variables are expected to be in KEY=value format.
// Empty keys are skipped for safety.
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
