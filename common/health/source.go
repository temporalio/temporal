package health

import (
	"strconv"
	"strings"

	"go.temporal.io/server/common/primitives"
)

// Component identifies which part of a service produced a signal. It is optional:
// service-wide checks leave it empty.
type Component string

const (
	ComponentGRPC        Component = "grpc"
	ComponentPersistence Component = "persistence"
)

// scope names used to distinguish the all-keys bucket from a named group
const (
	scopeOverall = "overall"
	scopeGroup   = "group"
)

// Source identifies the aggregator a set of checks came from. It is the prefix of every
// check name Evaluate emits, so that checks from different services and components stay
// distinguishable in one response.
type Source struct {
	Service   primitives.ServiceName
	Component Component
}

// String renders the source, e.g. history.persistence, or just history when the
// source has no component
func (s Source) String() string {
	if s.Component == "" {
		return string(s.Service)
	}

	return string(s.Service) + "." + string(s.Component)
}

// overallCheckType names a check on the bucket spanning every key, e.g.
// history.persistence.overall.latency.p99, history.grpc.overall.error_ratio
// if not a quantile, pass in 0
func (s Source) overallCheckType(checkType string, quantile float64) string {
	return s.checkType([]string{scopeOverall}, checkType, quantile)
}

// groupCheckType names a check on one configured group, e.g. history.grpc.group.critical.latency.p99
func (s Source) groupCheckType(groupName string, checkType string, quantile float64) string {
	return s.checkType([]string{scopeGroup, groupName}, checkType, quantile)
}

func (s Source) checkType(scope []string, checkType string, quantile float64) string {
	parts := append([]string{s.String()}, scope...)
	parts = append(parts, checkType)

	if quantile > 0 {
		parts = append(parts, formatQuantile(quantile))
	}

	return strings.Join(parts, ".")
}

// formatQuantile renders a quantile as: 0.99 -> p99, 0.999 -> p99.9
func formatQuantile(quantile float64) string {
	return "p" + strconv.FormatFloat(100.0*quantile, 'f', -1, 64)
}
