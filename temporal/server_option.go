package temporal

import (
	"go.temporal.io/server/common/service/config"
)

type (
	ServerOption interface {
		apply(*serverOptions)
	}
)

func WithConfig(cfg config.Config) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.config = cfg
	})
}

func WithConfigLoader(configDir string, env string, zone string) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.configDir, s.env, s.zone = configDir, env, zone
	})
}

func ForServices(names []string) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.serviceNames = names
	})
}

// InterruptOn interrupts server on the signal from server. If channel is nil Start() will block forever.
func InterruptOn(interruptCh <-chan interface{}) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.blockingStart = true
		s.interruptCh = interruptCh
	})
}
