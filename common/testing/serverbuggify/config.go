// Package serverbuggify configures BUGGIFY, seeded random failpoint activation (see Config in
// go.temporal.io/testx/failpoint/buggify), for the server if the startup switch enabled
// failpoints: the failpoints.buggify block of the static config, which has the seed and the
// allowlist of keys that BUGGIFY applies to, and the randomization of dynamic config settings of
// BuggifiableSettings. The server reads no environment variable for BUGGIFY, and no dynamic config
// setting changes it while the server runs.
package serverbuggify

import (
	"errors"
	"fmt"

	"go.temporal.io/server/common/config"
	_ "go.temporal.io/server/common/testing/failpointerrors" // the errors that the keys name
	"go.temporal.io/testx/failpoint/buggify"
)

// SettingPrefix prefixes the key of a dynamic config setting to name it in BUGGIFY decisions, so
// that an entry of failpoints.buggify.keys also applies to the randomization of settings.
const SettingPrefix = "dynamicconfig:"

// StaticConfig is the BUGGIFY configuration of the failpoints.buggify block of the static config,
// validated and read once at startup.
type StaticConfig struct {
	// Config has the seed and the keys of the block, or is zero without a seed.
	Config buggify.Config
	// Trace is whether BUGGIFY decisions are printed as trace lines, see TraceLine.
	Trace bool
}

// ReadStatic validates the failpoints.buggify block of the static config. Without a seed it
// returns a configuration that leaves BUGGIFY off, after validating the keys of the block. It
// returns an error if a seed has no keys, a key has no match or repeats the match of another, a
// probability is missing or not in [0, 1], or an error name is not registered.
func ReadStatic(c config.FailpointsBuggify) (StaticConfig, error) {
	keys := make([]buggify.Key, len(c.Keys))
	for i, k := range c.Keys {
		if k.ActivationProbability == nil || k.FireProbability == nil {
			field := "activationProbability"
			if k.ActivationProbability != nil {
				field = "fireProbability"
			}
			return StaticConfig{}, fmt.Errorf("invalid failpoints.buggify.keys[%d] (match %q): %s is required", i, k.Match, field)
		}
		keys[i] = buggify.Key{
			Match:                 k.Match,
			ActivationProbability: *k.ActivationProbability,
			FireProbability:       *k.FireProbability,
			Error:                 k.Error,
		}
	}
	if c.Seed != 0 && len(keys) == 0 {
		return StaticConfig{}, errors.New("invalid failpoints.buggify: a seed needs keys, " +
			"because BUGGIFY applies only to the keys that failpoints.buggify.keys matches")
	}
	configured := buggify.Config{Seed: c.Seed, Keys: keys}
	if err := configured.Validate(); err != nil {
		return StaticConfig{}, fmt.Errorf("invalid failpoints.buggify: %w", err)
	}
	static := StaticConfig{Trace: c.Trace}
	if c.Seed != 0 {
		static.Config = configured
	}
	return static, nil
}

// TraceLine formats a BUGGIFY event as a trace line, and reports whether the event has one. The
// trace has one line per BUGGIFY decision, in a stable format that a harness can parse: the prefix
// "BUGGIFY ", then space-separated key=value fields in this order, with the string values quoted
// like Go strings (strconv.Quote) and the others bare:
//
//	BUGGIFY event=activation key="<failpoint name>" active=<true|false>
//	BUGGIFY event=fire key="<failpoint name>" scope="<scope>" execution=<n> action=<action>
//
// An activation line is the decision whether the name is activated for the run. A fire line is an
// execution of an activated name that fires, with the number of the execution in its scope
// (executions are numbered per scope, which is why the line names it, and is empty for the unscoped
// failpoint), and the action that it takes, such as Return. Fields are only added at the end of a
// line, and the name of an event or field never changes meaning.
func TraceLine(e buggify.Event) (string, bool) {
	switch e.Kind {
	case buggify.EventActivation:
		return fmt.Sprintf("BUGGIFY event=activation key=%q active=%t", e.Name, e.Activated), true
	case buggify.EventFire:
		return fmt.Sprintf("BUGGIFY event=fire key=%q scope=%q execution=%d action=%s", e.Name, e.Scope, e.ExecutionNumber, e.Action), true
	default:
		return "", false
	}
}
