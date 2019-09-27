// Copyright (c) 2019 Uber Technologies, Inc.
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

package canary

import (
	"fmt"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/config"
	"go.uber.org/zap"
)

type (
	// Config contains the configurable yaml
	// properties for the canary runtime
	Config struct {
		Domains  []string `yaml:"domains"`
		Excludes []string `yaml:"excludes"`
	}
)

const (
	// ConfigurationKey is the config YAML key for the canary module
	ConfigurationKey = "canary"
)

// Init validates and initializes the config
func newCanaryConfig(provider config.Provider) (*Config, error) {
	raw := provider.Get(ConfigurationKey)
	var cfg Config
	if err := raw.Populate(&cfg); err != nil {
		return nil, fmt.Errorf("failed to load canary configuration with error: %v", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Domains) == 0 {
		return fmt.Errorf("missing value for domains property")
	}
	return nil
}

// RuntimeContext contains all the context
// information needed to run the canary
type RuntimeContext struct {
	Env     string
	logger  *zap.Logger
	metrics tally.Scope
	service workflowserviceclient.Interface
}

// NewRuntimeContext builds a runtime context from the config
func newRuntimeContext(env string, logger *zap.Logger, scope tally.Scope, service workflowserviceclient.Interface) *RuntimeContext {
	return &RuntimeContext{
		Env:     env,
		logger:  logger,
		metrics: scope,
		service: service,
	}
}
