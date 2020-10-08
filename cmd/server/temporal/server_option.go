package temporal

import (
	"go.temporal.io/server/common/service/config"
)

type ServerOption interface {
	apply(*server)
}

type applyFuncContainer struct {
	applyInternal func(*server)
}

func (fso *applyFuncContainer) apply(s *server) {
	fso.applyInternal(s)
}

func newApplyFuncContainer(apply func(option *server)) *applyFuncContainer {
	return &applyFuncContainer{
		applyInternal: apply,
	}
}

func WithConfig(config *config.Config) ServerOption {
	return newApplyFuncContainer(func(s *server) {
		s.config = config
	})
}

func ForService(name string) ServerOption {
	return newApplyFuncContainer(func(s *server) {
		s.name = name
	})
}
