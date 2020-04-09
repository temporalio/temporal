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
		Namespaces []string `yaml:"namespaces"`
		Excludes   []string `yaml:"excludes"`
	}

	// Cadence contains the configuration for cadence service
	Cadence struct {
		ServiceName     string `yaml:"service"`
		HostNameAndPort string `yaml:"host"`
	}
)

// Validate validates canary configration
func (c *Config) Validate() error {
	if len(c.Canary.Namespaces) == 0 {
		return fmt.Errorf("missing value for namespaces property")
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
