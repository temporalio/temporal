package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/go-common.git/x/tchannel"

	jaeger "github.com/uber/jaeger-client-go/config"
)

type appConfig struct {
	Logging     log.Configuration
	Metrics     metrics.Configuration
	Jaeger      jaeger.Configuration
	TChannel    xtchannel.Configuration
	Sentry      log.SentryConfiguration
	Verbose     bool
	ServiceName string `yaml:"serviceName"`
}
