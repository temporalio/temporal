// The MIT License
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

package config

import (
	"fmt"
	stdlog "log"
	"os"

	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v3"
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

	// TODO: remove log dependency.
	stdlog.Printf("Loading config; env=%v,zone=%v,configDir=%v\n", env, zone, configDir)

	files, err := getConfigFiles(env, configDir, zone)
	if err != nil {
		return err
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
		err = yaml.Unmarshal(data, config)
		if err != nil {
			return err
		}
	}

	return validator.Validate(config)
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
