package failpoints

import (
	// Every server can return the server's errors from failpoints.
	_ "go.temporal.io/server/common/testing/failpointerrors"
	"go.temporal.io/testx/failpoint"
)

// EnvVar is the environment variable that, together with the failpoints.enabled server config
// setting, enables failpoints. It must be "1".
const EnvVar = "TEMPORAL_FAILPOINTS"

// Decision is whether the static startup switch enables failpoints.
type Decision struct {
	// Enabled is true if both the config setting and the environment variable are set.
	Enabled bool
	// Reason names the inputs of the decision, for logs.
	Reason string
}

// Decide reads the static switch: failpoints are enabled only if the failpoints.enabled config
// setting is true and the TEMPORAL_FAILPOINTS environment variable, read with lookup, is "1".
// Dynamic config has no say. The server decides once at startup.
func Decide(configEnabled bool, lookup func(string) (string, bool)) Decision {
	value, _ := lookup(EnvVar)
	envEnabled := value == "1"
	switch {
	case configEnabled && envEnabled:
		return Decision{Enabled: true, Reason: "config failpoints.enabled and environment " + EnvVar + "=1"}
	case configEnabled:
		return Decision{Reason: "config failpoints.enabled is set but environment " + EnvVar + "=1 is not"}
	case envEnabled:
		return Decision{Reason: "environment " + EnvVar + "=1 is set but config failpoints.enabled is not"}
	default:
		return Decision{Reason: "neither config failpoints.enabled nor environment " + EnvVar + "=1 is set"}
	}
}

// Source is the failpoint registry of a process, provided to every consumer by fx instead of being
// reached through test hooks. It holds no registry unless the startup switch enabled failpoints,
// so call sites cost a nil check. It is a failpoint.Source. A nil *Source holds no registry.
type Source struct {
	registry failpoint.Registry
}

// NewSource returns a Source with a fresh registry if decision enables failpoints, else one
// without a registry.
func NewSource(decision Decision) *Source {
	if !decision.Enabled {
		return &Source{}
	}
	return NewEnabledSource()
}

// NewEnabledSource returns a Source with a fresh registry, for tests and for servers that enabled
// failpoints.
func NewEnabledSource() *Source {
	return &Source{registry: failpoint.NewRegistry()}
}

// Failpoints returns the registry, or nil if failpoints are not enabled.
func (s *Source) Failpoints() failpoint.Registry {
	if s == nil {
		return nil
	}
	return s.registry
}

// Enabled reports whether s has a registry.
func (s *Source) Enabled() bool {
	return s.Failpoints() != nil
}
