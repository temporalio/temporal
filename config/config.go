package config

import (
	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/go-common.git/x/tchannel"

	jaeger "github.com/uber/jaeger-client-go/config"
)

// AppConfig - application configuration
type AppConfig struct {
	Logging     log.Configuration
	Metrics     metrics.Configuration
	Jaeger      jaeger.Configuration
	TChannel    xtchannel.Configuration
	Sentry      log.SentryConfiguration
	Verbose     bool
	ServiceName string `yaml:"serviceName"`
}

// LoadConfig loads the workflow configuration
func LoadConfig() (*AppConfig, error) {
	var cfg AppConfig
	if err := config.Load(&cfg); err != nil {
		log.Fatalf("Error initializing configuration: %s", err)
	}
	log.Configure(&cfg.Logging, cfg.Verbose)
	log.ConfigureSentry(&cfg.Sentry)

	return &cfg, nil
}
