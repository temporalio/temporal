// Package buggify implements BUGGIFY, FoundationDB-style randomized fault activation, as a
// fallback of a failpoint registry (see failpoint.Fallback): with a seed, keys that have nothing
// registered are activated for a run at random, and their executions fire at random.
package buggify

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"go.temporal.io/testx/failpoint"
)

// Config configures BUGGIFY, FoundationDB-style randomized fault activation. With a seed,
// each key name that the code under test executes, and that has no failpoint registered, is
// activated for the whole run with a probability. Each execution of an activated key then fires
// with another probability. Both decisions are pure functions of the seed, the key name and, for
// firing, the number of the execution of that name, so the same seed activates the same keys.
//
// The zero value disables BUGGIFY.
type Config struct {
	// Seed seeds every decision. Zero disables BUGGIFY.
	Seed uint64
	// Activation and Fire are the probabilities, in [0, 1], that a key that matches no prefix is
	// activated for the run, and that an execution of an activated key fires.
	Activation float64
	Fire       float64
	// Error names a registered error (see failpoint.RegisterError) that failpoint.Execute returns
	// when a key that matches no prefix fires. Without it, such keys only fire at Fires call sites.
	Error string
	// Prefixes override the probabilities and the error for the key names they prefix. The
	// longest prefix that matches applies.
	Prefixes []Prefix
	// Include and Exclude select the key names that BUGGIFY applies to, as patterns in which "*"
	// matches any text. An empty Include selects every name; Exclude wins over Include.
	Include []string
	Exclude []string
}

// Prefix overrides the BUGGIFY probabilities and error for key names with a prefix.
type Prefix struct {
	Prefix     string
	Activation float64
	Fire       float64
	// Error names a registered error that failpoint.Execute returns when a key fires. Without it,
	// the keys only fire at Fires call sites.
	Error string
}

// clone returns a copy of c that shares no slices with it.
func (c Config) clone() Config {
	c.Prefixes = slices.Clone(c.Prefixes)
	c.Include = slices.Clone(c.Include)
	c.Exclude = slices.Clone(c.Exclude)
	return c
}

// Enabled reports whether the configuration enables BUGGIFY.
func (c Config) Enabled() bool {
	return c.Seed != 0
}

// Validate reports whether Buggify.Set accepts the configuration.
func (c Config) Validate() error {
	if err := validateParams("", c.Activation, c.Fire, c.Error); err != nil {
		return err
	}
	prefixes := make(map[string]bool, len(c.Prefixes))
	for _, p := range c.Prefixes {
		if p.Prefix == "" {
			return errors.New("buggify prefix must not be empty")
		}
		if prefixes[p.Prefix] {
			return fmt.Errorf("duplicate buggify prefix %q", p.Prefix)
		}
		prefixes[p.Prefix] = true
		if err := validateParams(p.Prefix, p.Activation, p.Fire, p.Error); err != nil {
			return err
		}
	}
	if slices.Contains(c.Include, "") || slices.Contains(c.Exclude, "") {
		return errors.New("buggify include and exclude patterns must not be empty")
	}
	return nil
}

func validateParams(prefix string, activation, fire float64, errorName string) error {
	for _, p := range []float64{activation, fire} {
		if math.IsNaN(p) || p < 0 || p > 1 {
			return fmt.Errorf("buggify probability %v for prefix %q is not in [0, 1]", p, prefix)
		}
	}
	if errorName != "" {
		if _, ok := failpoint.LookupError(errorName); !ok {
			return fmt.Errorf("unknown error %q for buggify prefix %q", errorName, prefix)
		}
	}
	return nil
}

// Draw returns a number in [0, 1) that is a pure function of the seed, name and n. BUGGIFY
// activates a key name if Draw(name, 0) is below its activation probability, and fires the n-th
// execution of the name in a scope if DrawScoped(name, scope, n) is below its fire probability.
// Code that makes other seeded choices, such as picking a configuration value, uses it to stay
// reproducible with the same seed.
func (c Config) Draw(name string, n uint64) float64 {
	x := mix64(c.Seed ^ mix64(fnv64a(name)) ^ mix64(n+0x9e3779b97f4a7c15))
	return float64(x>>11) / (1 << 53)
}

// DrawScoped is Draw for the executions of a name in a scope, which BUGGIFY numbers separately for
// each scope, so the executions in one scope don't change which executions fire in another.
// Without a scope, it is Draw.
func (c Config) DrawScoped(name, scope string, n uint64) float64 {
	if scope == "" {
		return c.Draw(name, n)
	}
	x := mix64(c.Seed ^ mix64(fnv64a(name)) ^ mix64(fnv64a(scope)^0x6a09e667f3bcc909) ^ mix64(n+0x9e3779b97f4a7c15))
	return float64(x>>11) / (1 << 53)
}

// Activated reports whether BUGGIFY activates the key name for the run. It is false for names
// that the configuration doesn't select, and if BUGGIFY is disabled.
func (c Config) Activated(name string) bool {
	params, ok := c.params(name)
	return ok && c.Draw(name, 0) < params.Activation
}

// params returns the probabilities and error of a key name, and whether BUGGIFY selects it.
func (c Config) params(name string) (Prefix, bool) {
	if !c.Enabled() || !c.selects(name) {
		return Prefix{}, false
	}
	params := Prefix{Activation: c.Activation, Fire: c.Fire, Error: c.Error}
	for _, p := range c.Prefixes {
		if strings.HasPrefix(name, p.Prefix) && len(p.Prefix) > len(params.Prefix) {
			params = p
		}
	}
	return params, true
}

func (c Config) selects(name string) bool {
	for _, pattern := range c.Exclude {
		if matchPattern(pattern, name) {
			return false
		}
	}
	if len(c.Include) == 0 {
		return true
	}
	for _, pattern := range c.Include {
		if matchPattern(pattern, name) {
			return true
		}
	}
	return false
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
