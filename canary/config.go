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
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/zap"

	"github.com/temporalio/temporal/common/service/config"
)

const (
	// EnvKeyRoot the environment variable key for runtime root dir
	EnvKeyRoot = "TEMPORAL_CANARY_ROOT"
	// EnvKeyConfigDir the environment variable key for config dir
	EnvKeyConfigDir = "TEMPORAL_CANARY_CONFIG_DIR"
	// EnvKeyEnvironment is the environment variable key for environment
	EnvKeyEnvironment = "TEMPORAL_CANARY_ENVIRONMENT"
	// EnvKeyAvailabilityZone is the environment variable key for AZ
	EnvKeyAvailabilityZone = "TEMPORAL_CANARY_AVAILABILITY_ZONE"
)

const (
	// CadenceLocalHostPort is the default address for cadence frontend service
	ServiceHostPort = "127.0.0.1:7933"
	// CadenceServiceName is the default service name for cadence frontend
	ServiceName = "frontend"
	// CanaryServiceName is the default service name for cadence canary
	CanaryServiceName = "canary"
)

type (
	// Config contains the configurable yaml
	// properties for the canary runtime
	Config struct {
		Canary  Canary         `yaml:"canary"`
		Cadence Cadence        `yaml:"temporal"`
		Log     config.Logger  `yaml:"log"`
		Metrics config.Metrics `yaml:"metrics"`
	}

	// Canary contains the configuration for canary tests
	Canary struct {
		Domains  []string `yaml:"domains"`
		Excludes []string `yaml:"excludes"`
	}

	// Cadence contains the configuration for cadence service
	Cadence struct {
		ServiceName     string `yaml:"service"`
		HostNameAndPort string `yaml:"host"`
	}
)

// Validate validates canary configration
func (c *Config) Validate() error {
	if len(c.Canary.Domains) == 0 {
		return fmt.Errorf("missing value for domains property")
	}
	return nil
}

// RuntimeContext contains all the context
// information needed to run the canary
type RuntimeContext struct {
	logger   *zap.Logger
	metrics  tally.Scope
	hostPort string
	service  workflowservice.WorkflowServiceClient
}

// NewRuntimeContext builds a runtime context from the config
func NewRuntimeContext(
	logger *zap.Logger,
	scope tally.Scope,
	hostPort string,
	service workflowservice.WorkflowServiceClient,
) *RuntimeContext {
	return &RuntimeContext{
		logger:   logger,
		metrics:  scope,
		hostPort: hostPort,
		service:  service,
	}
}
