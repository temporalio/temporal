package buggify

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
)

// Environment variables that ConfigFromEnv reads.
const (
	// SeedEnv seeds BUGGIFY: a positive integer, SeedRandom or SeedGomad.
	SeedEnv = "FAILPOINT_BUGGIFY_SEED"
	// ActivationEnv and FireEnv set the probabilities of Config.Activation
	// and Config.Fire, in [0, 1]. They default to FoundationDB's 0.25.
	ActivationEnv = "FAILPOINT_BUGGIFY_ACTIVATION"
	FireEnv       = "FAILPOINT_BUGGIFY_FIRE"
	// GomadSeedEnv is the seed of a gomad run, see SeedGomad.
	GomadSeedEnv = "GOMADSEED"
)

// Values of SeedEnv that don't name a seed.
const (
	// SeedRandom picks a random seed. The caller should log the returned seed, so the run
	// can be reproduced with it.
	SeedRandom = "random"
	// SeedGomad derives the seed from GomadSeedEnv, so a gomad seed reproduces the BUGGIFY
	// decisions too. GomadSeedEnv alone doesn't enable BUGGIFY.
	SeedGomad = "gomad"
)

// DefaultProbability is FoundationDB's probability that a site is activated, and that an
// activated site fires.
const DefaultProbability = 0.25

// ConfigFromEnv returns the BUGGIFY configuration in the environment variables that lookup
// reads, such as os.LookupEnv: the seed of SeedEnv and the probabilities of
// ActivationEnv and FireEnv. Without SeedEnv it returns the zero
// configuration, which disables BUGGIFY. It returns an error if a variable is malformed, or if a
// probability is set without a seed.
func ConfigFromEnv(lookup func(string) (string, bool)) (Config, error) {
	seedValue, hasSeed := lookup(SeedEnv)
	if !hasSeed || seedValue == "" {
		for _, name := range []string{ActivationEnv, FireEnv} {
			if _, ok := lookup(name); ok {
				return Config{}, fmt.Errorf("%s requires %s", name, SeedEnv)
			}
		}
		return Config{}, nil
	}
	seed, err := parseSeed(seedValue, lookup)
	if err != nil {
		return Config{}, err
	}
	config := Config{Seed: seed, Activation: DefaultProbability, Fire: DefaultProbability}
	for name, p := range map[string]*float64{ActivationEnv: &config.Activation, FireEnv: &config.Fire} {
		s, ok := lookup(name)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(v) || v < 0 || v > 1 {
			return Config{}, fmt.Errorf("invalid %s %q: want a probability in [0, 1]", name, s)
		}
		*p = v
	}
	return config, nil
}

func parseSeed(value string, lookup func(string) (string, bool)) (uint64, error) {
	switch value {
	case SeedRandom:
		var b [8]byte
		for {
			_, _ = rand.Read(b[:])
			if seed := binary.LittleEndian.Uint64(b[:]); seed != 0 {
				return seed, nil
			}
		}
	case SeedGomad:
		gomadSeed, ok := lookup(GomadSeedEnv)
		if !ok || gomadSeed == "" {
			return 0, fmt.Errorf("%s=%s requires %s", SeedEnv, SeedGomad, GomadSeedEnv)
		}
		// gomad seeds can be 0, so the seed is derived rather than taken as is.
		h := fnv.New64a()
		_, _ = h.Write([]byte("gomad:" + gomadSeed))
		return max(h.Sum64(), 1), nil
	default:
		seed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seed == 0 {
			return 0, fmt.Errorf("invalid %s %q: want a positive integer, %s or %s", SeedEnv, value, SeedRandom, SeedGomad)
		}
		return seed, nil
	}
}
