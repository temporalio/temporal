package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

const (
	// EnvKeyRoot the environment variable key for runtime root dir
	EnvKeyRoot = "TEMPORAL_ROOT"
	// EnvKeyConfigDir the environment variable key for config dir
	EnvKeyConfigDir = "TEMPORAL_CONFIG_DIR"
	// EnvKeyEnvironment is the environment variable key for environment
	EnvKeyEnvironment = "TEMPORAL_ENVIRONMENT"
	// EnvKeyAvailabilityZone is the environment variable key for AZ
	EnvKeyAvailabilityZone = "TEMPORAL_AVAILABILTY_ZONE"
)

const (
	baseFile         = "base.yaml"
	envDevelopment   = "development"
	defaultConfigDir = "config"
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
//   base.yaml
//       env.yaml   -- environment is one of the input params ex-development
//         env_az.yaml -- zone is another input param
//
func Load(env string, configDir string, zone string, config interface{}) error {

	if len(env) == 0 {
		env = envDevelopment
	}

	if len(configDir) == 0 {
		configDir = defaultConfigDir
	}

	files, err := getConfigFiles(env, configDir, zone)
	if err != nil {
		return err
	}

	log.Printf("Loading configFiles=%v\n", files)

	for _, f := range files {
		// This is tagged nosec because the file names being read are for config files that are not user supplied
		// #nosec
		data, err := ioutil.ReadFile(f)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(data, config)
		if err != nil {
			return err
		}
	}

	return validator.Validate(config)
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
