// Package buggify configures BUGGIFY, seeded random failpoint activation (see
// fpbuggify.Config), for the server in test builds: the server's defaults per key prefix,
// the testing.buggify dynamic config setting, the seed environment variables, and the
// randomization of dynamic config settings marked with dynamicconfig.Buggifiable.
package buggify

import (
	"fmt"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/primitives"
	_ "go.temporal.io/server/common/testing/failpointerrors" // the errors that the defaults name
	"go.temporal.io/server/common/testing/failpointrules"
	"go.temporal.io/testx/failpoint"
	fpbuggify "go.temporal.io/testx/failpoint/buggify"
)

const (
	// SettingKey is the key of the dynamic config setting that configures BUGGIFY, see Value.
	SettingKey = "testing.buggify"
	// GomadEventsEnv enables printing BUGGIFY events as gomad event lines, see GomadLine.
	GomadEventsEnv = "FAILPOINT_GOMAD_EVENTS"
	// KnobPrefix prefixes the key of a dynamic config setting to name it in BUGGIFY decisions, so
	// that the configuration's prefixes and patterns also apply to the randomization of settings.
	KnobPrefix = "dynamicconfig:"
)

// Startup is the BUGGIFY configuration that a server resolved once at startup, when it set up its
// dynamic config client, so that the randomized settings and the failpoint registry use the same
// seed even if the setting changes meanwhile.
type Startup struct {
	// Resolved is false if the server didn't resolve a configuration, such as in a build
	// without test hooks.
	Resolved bool
	// Value and Env are the value of the setting and the environment that Config was resolved
	// from.
	Value      Value
	Env        Env
	Config     fpbuggify.Config
	SeedSource string
}

// Defaults returns the server's BUGGIFY configuration without a seed. Hand-placed keys use
// FoundationDB's probabilities, and only fire at fpbuggify.Fires call sites. Keys derived from
// gRPC methods and outgoing HTTP requests, of which there are hundreds, are activated and fire
// much less often, and fail the request before it is handled with Unavailable, which callers
// retry. Persistence keys (out of scope for now), response keys (the handler already ran), the
// FailpointService and health checks are excluded. The failpoints of any gRPC method or HTTP
// request (failpoint.AnyGRPCKey and the others) don't match these prefixes, and their adapters
// never buggify them.
func Defaults() fpbuggify.Config {
	return fpbuggify.Config{
		Activation: 0.25,
		Fire:       0.25,
		Prefixes: []fpbuggify.Prefix{
			{Prefix: "grpc:", Activation: 0.1, Fire: 0.01, Error: "Unavailable"},
			{Prefix: "http:", Activation: 0.1, Fire: 0.01, Error: "Unavailable"},
		},
		Exclude: []string{
			"persistence:*",
			"grpc-response:*",
			"http-response:*",
			"grpc:" + api.FailpointServicePrefix + "*",
			"grpc:/grpc.health.v1.Health/*",
		},
	}
}

// InjectsInboundGRPCErrors reports whether BUGGIFY injects errors into the gRPC requests that
// service receives. It does for the calls between services, which history, matching, worker and
// the internal frontend receive, but not for the public frontend: its requests come from clients,
// so an error there would exercise the client's retries rather than the server. Registered
// failpoints apply to every service.
func InjectsInboundGRPCErrors(service primitives.ServiceName) bool {
	return service != primitives.FrontendService
}

// Value is the value of the testing.buggify dynamic config setting.
type Value struct {
	// Seed seeds BUGGIFY. Zero means the seed of the environment, see Env.
	Seed uint64
	// Disabled turns BUGGIFY off, even with a seed in the environment.
	Disabled bool
	// Activation, Fire and Error replace those of Defaults for keys that match no prefix.
	Activation *float64
	Fire       *float64
	Error      *string
	// Prefixes replace the prefixes of Defaults with the same Prefix, and add the others.
	Prefixes []fpbuggify.Prefix
	// Include and Exclude replace those of Defaults.
	Include *[]string
	Exclude *[]string
}

// ConvertValue converts a value of the testing.buggify setting, a map with the fields of Value
// (seed, disabled, activation, fire, error, prefixes with prefix, activation, fire and error,
// include, exclude). It returns an error if a field is unknown or malformed.
func ConvertValue(value any) (Value, error) {
	if v, ok := value.(Value); ok {
		return v, nil
	}
	var v Value
	if value == nil {
		return v, nil
	}
	if err := failpointrules.DecodeStrict(value, &v); err != nil {
		return Value{}, err
	}
	return v, nil
}

// Resolve returns the BUGGIFY configuration of a server for a value of the setting and the
// environment: Defaults, with the probabilities of env, then the overrides of value, seeded by
// value, else by env. It also describes where the seed came from. The configuration is disabled
// if value is, or if neither has a seed. It returns an error if the configuration that value
// describes is invalid, also if value disables BUGGIFY or there is no seed.
func Resolve(value Value, env Env) (fpbuggify.Config, string, error) {
	config := Defaults()
	if env.Config.Enabled() {
		config.Seed = env.Config.Seed
		config.Activation = env.Config.Activation
		config.Fire = env.Config.Fire
	}
	source := env.Source
	if value.Seed != 0 {
		config.Seed = value.Seed
		source = "dynamic config " + SettingKey
	}
	if value.Activation != nil {
		config.Activation = *value.Activation
	}
	if value.Fire != nil {
		config.Fire = *value.Fire
	}
	if value.Error != nil {
		config.Error = *value.Error
	}
	for _, p := range value.Prefixes {
		if i := indexOfPrefix(config.Prefixes, p.Prefix); i >= 0 {
			config.Prefixes[i] = p
		} else {
			config.Prefixes = append(config.Prefixes, p)
		}
	}
	if value.Include != nil {
		config.Include = *value.Include
	}
	if value.Exclude != nil {
		config.Exclude = *value.Exclude
	}
	// A value is validated also if it disables BUGGIFY or has no seed, so an invalid value is
	// never accepted silently.
	if err := config.Validate(); err != nil {
		return fpbuggify.Config{}, "", fmt.Errorf("invalid %s: %w", SettingKey, err)
	}
	if value.Disabled || config.Seed == 0 {
		return fpbuggify.Config{}, "", nil
	}
	return config, source, nil
}

func indexOfPrefix(prefixes []fpbuggify.Prefix, prefix string) int {
	for i, p := range prefixes {
		if p.Prefix == prefix {
			return i
		}
	}
	return -1
}

// Env is the BUGGIFY configuration in the environment of the server process, read once, so a
// random seed is picked once.
type Env struct {
	// Config is the configuration of fpbuggify.ConfigFromEnv: the seed and probabilities,
	// or zero.
	Config fpbuggify.Config
	// Source describes the seed variable, such as "FAILPOINT_BUGGIFY_SEED=random".
	Source string
}

// ReadEnv reads the BUGGIFY variables of fpbuggify.ConfigFromEnv with lookup, such as
// os.LookupEnv. It returns an error if a variable is malformed.
func ReadEnv(lookup func(string) (string, bool)) (Env, error) {
	config, err := fpbuggify.ConfigFromEnv(lookup)
	if err != nil || !config.Enabled() {
		return Env{}, err
	}
	seed, _ := lookup(fpbuggify.SeedEnv)
	return Env{Config: config, Source: fpbuggify.SeedEnv + "=" + seed}, nil
}

// GomadLine formats a BUGGIFY event as a stable line that gomad can use as an event or as a
// fault-rule anchor, and reports whether the event is one.
func GomadLine(e failpoint.Event) (string, bool) {
	switch e.Kind {
	case failpoint.FallbackActivationEvent:
		return fmt.Sprintf("GOMAD failpoint buggify-activation key=%q active=%t", e.Key.Name(), e.Activated), true
	case failpoint.FallbackFireEvent:
		// Executions are numbered per scope, so the line names the scope.
		return fmt.Sprintf("GOMAD failpoint hit key=%q scope=%q n=%d action=%s buggify=true", e.Key.Name(), e.Key.Scope(), e.Execution, e.Action), true
	default:
		return "", false
	}
}
