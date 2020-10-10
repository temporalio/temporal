package temporal

import (
	"log"

	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/tools/cassandra"
	"go.temporal.io/server/tools/sql"
)

type (
	serverOptions struct {
		config    config.Config
		configDir string
		env       string
		zone      string

		serviceNames []string

		interruptCh   <-chan interface{}
		blockingStart bool
	}
)

func newServerOptions(opts []ServerOption) *serverOptions {
	so := &serverOptions{
		// all defaults
	}

	for _, opt := range opts {
		opt.apply(so)
	}

	return so
}

func isValidService(service string) bool {
	for _, s := range Services {
		if s == service {
			return true
		}
	}
	return false
}

func (so *serverOptions) validate() error {
	for _, serviceName := range so.serviceNames {
		if !isValidService(serviceName) {
			log.Fatalf("invalid service %q in service list [%v]", serviceName, so.serviceNames)
		}
	}

	// check option correctess
	// server names
	// consistency
	// etc

	so.loadConfig()
	so.validateConfig()

	return nil
}

func (so *serverOptions) loadConfig() {
	err := config.Load(so.env, so.configDir, so.zone, &so.config)
	if err != nil {
		log.Fatal("Config file corrupted.", err)
	}
}

func (so *serverOptions) validateConfig() {
	if err := so.config.Validate(); err != nil {
		log.Fatalf("config validation failed: %v", err)
	}
	if so.config.PublicClient.HostPort == "" {
		log.Fatal("need to provide an endpoint config for PublicClient")
	}
	for _, name := range so.serviceNames {
		if _, ok := so.config.Services[name]; !ok {
			log.Fatalf("%q service missing config", name)
		}
	}

	// cassandra schema version validation
	if err := cassandra.VerifyCompatibleVersion(so.config.Persistence); err != nil {
		log.Fatalf("cassandra schema version compatibility check failed: %v", err)
	}
	// sql schema version validation
	if err := sql.VerifyCompatibleVersion(so.config.Persistence); err != nil {
		log.Fatalf("sql schema version compatibility check failed: %v", err)
	}
}
