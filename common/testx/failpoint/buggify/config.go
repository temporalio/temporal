// Package buggify implements BUGGIFY, seeded randomized fault activation, as a failpoint.Policy of
// a failpoint registry: with a seed, keys that have nothing registered are activated for a run at
// random, and their executions fire at random.
package buggify

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"go.temporal.io/testx/failpoint"
)

// Config configures BUGGIFY, seeded randomized fault activation. With a seed, each key name that
// the code under test executes, that has no failpoint registered and that an entry of Keys
// matches, is activated for the whole run with a probability. Each execution of an activated key
// then fires with another probability. Both decisions are pure functions of the seed, the key name
// and, for firing, the number of the execution of that name, so the same seed activates the same
// keys.
//
// The zero value disables BUGGIFY.
type Config struct {
	// Seed seeds every decision. Zero disables BUGGIFY. A seed needs Keys.
	Seed uint64
	// Keys is the allowlist of the key names that BUGGIFY applies to: the first entry that matches a
	// name sets its probabilities and error, and a name that matches no entry is never activated.
	// To keep some names of a group out, put an entry for them with an ActivationProbability of 0
	// before the entry of the group.
	Keys []Key
}

// Key is an entry of Config.Keys.
type Key struct {
	// Match is a pattern for the key names of the entry, in which "*" matches any text, including
	// none, anywhere in the name. There is no other syntax.
	Match string
	// ActivationProbability and FireProbability are the probabilities, in [0, 1], that a matching
	// key is activated for the run, and that an execution of an activated key fires.
	ActivationProbability float64
	FireProbability       float64
	// Error names a registered error (see failpoint.RegisterError) that failpoint.CheckError returns
	// when a matching key fires. Without it, the keys only fire at switch call sites.
	Error string
}

// clone returns a copy of c that shares no slices with it.
func (c Config) clone() Config {
	c.Keys = slices.Clone(c.Keys)
	return c
}

// Enabled reports whether the configuration enables BUGGIFY.
func (c Config) Enabled() bool {
	return c.Seed != 0
}

// Validate reports whether Buggify.Set accepts the configuration. It rejects a seed without keys,
// an empty or duplicate match, a probability outside [0, 1] and an error that is not registered.
func (c Config) Validate() error {
	if c.Seed != 0 && len(c.Keys) == 0 {
		return errors.New("buggify seed needs keys: BUGGIFY applies only to the keys that Keys matches")
	}
	matches := make(map[string]bool, len(c.Keys))
	for _, k := range c.Keys {
		if k.Match == "" {
			return errors.New("buggify key match must not be empty")
		}
		if matches[k.Match] {
			return fmt.Errorf("duplicate buggify key match %q", k.Match)
		}
		matches[k.Match] = true
		for _, p := range []float64{k.ActivationProbability, k.FireProbability} {
			if math.IsNaN(p) || p < 0 || p > 1 {
				return fmt.Errorf("buggify probability %v for key match %q is not in [0, 1]", p, k.Match)
			}
		}
		if k.Error != "" {
			if _, ok := failpoint.LookupError(k.Error); !ok {
				return fmt.Errorf("unknown error %q for buggify key match %q", k.Error, k.Match)
			}
		}
	}
	return nil
}

// Draw returns a number in [0, 1) that is a pure function of the seed, name and n. BUGGIFY
// activates a key name if Draw(name, 0) is below its activation probability. Code that makes
// other seeded choices, such as picking a configuration value, uses it to stay reproducible with
// the same seed.
func (c Config) Draw(name string, n uint64) float64 {
	return draw(c.Seed, nameMix(name), n)
}

// nameMix and scopeMix return the hashes of a name and a scope that the draws use, so callers
// that draw repeatedly for the same name or scope compute them once. The hash of the empty scope is
// zero, so an execution without a scope draws as the executions of Draw do.
func nameMix(name string) uint64 {
	return mix64(fnv64a(name))
}

func scopeMix(scope string) uint64 {
	if scope == "" {
		return 0
	}
	return mix64(fnv64a(scope) ^ 0x6a09e667f3bcc909)
}

// draw is Draw for the combined hash of a name and a scope.
func draw(seed, mix, n uint64) float64 {
	x := mix64(seed ^ mix ^ mix64(n+0x9e3779b97f4a7c15))
	return float64(x>>11) / (1 << 53)
}

// Activated reports whether BUGGIFY activates the key name for the run. It is false for names
// that no entry of Keys matches, and if BUGGIFY is disabled.
func (c Config) Activated(name string) bool {
	key, ok := c.params(name)
	return ok && c.Draw(name, 0) < key.ActivationProbability
}

// params returns the first entry of Keys that matches a key name, and whether BUGGIFY applies to
// the name.
func (c Config) params(name string) (Key, bool) {
	if !c.Enabled() {
		return Key{}, false
	}
	for _, k := range c.Keys {
		if matchPattern(k.Match, name) {
			return k, true
		}
	}
	return Key{}, false
}

// matchPattern reports whether name matches pattern, in which "*" matches any text, including
// none.
func matchPattern(pattern, name string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == name
	}
	if !strings.HasPrefix(name, parts[0]) {
		return false
	}
	name = name[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, part := range parts[1 : len(parts)-1] {
		i := strings.Index(name, part)
		if i < 0 {
			return false
		}
		name = name[i+len(part):]
	}
	return strings.HasSuffix(name, last)
}

func fnv64a(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// mix64 is the finalizer of SplitMix64.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
