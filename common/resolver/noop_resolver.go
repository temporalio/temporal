package resolver

import (
	"time"
)

type (
	NoopResolver struct {
	}
)

func NewNoopResolver() *NoopResolver {
	return &NoopResolver{}
}

func (c *NoopResolver) RefreshInterval() time.Duration {
	return time.Duration(0)
}

func (c *NoopResolver) Resolve(service string) []string {
	return []string{service}
}
