package server

import (
	"go.temporal.io/server/common/service/config"
)

type ServerOption interface {
	apply(*Server)
}

type applyFuncContainer struct {
	applyInternal func(*Server)
}

func (fso *applyFuncContainer) apply(s *Server) {
	fso.applyInternal(s)
}

func newApplyFuncContainer(apply func(option *Server)) *applyFuncContainer {
	return &applyFuncContainer{
		applyInternal: apply,
	}
}

func WithConfig(config *config.Config) ServerOption {
	return newApplyFuncContainer(func(s *Server) {
		s.config = config
	})
}

func ForService(name string) ServerOption {
	return newApplyFuncContainer(func(s *Server) {
		s.name = name
	})
}
