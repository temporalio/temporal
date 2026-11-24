package failpoint

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
)

// Environment variables that BuggifyConfigFromEnv reads.
const (
	// BuggifySeedEnv seeds BUGGIFY: a positive integer, BuggifySeedRandom or BuggifySeedGomad.
	BuggifySeedEnv = "FAILPOINT_BUGGIFY_SEED"
	// BuggifyActivationEnv and BuggifyFireEnv set the probabilities of BuggifyConfig.Activation
	// and BuggifyConfig.Fire, in [0, 1]. They default to FoundationDB's 0.25.
	BuggifyActivationEnv = "FAILPOINT_BUGGIFY_ACTIVATION"
	BuggifyFireEnv       = "FAILPOINT_BUGGIFY_FIRE"
	// GomadSeedEnv is the seed of a gomad run, see BuggifySeedGomad.
	GomadSeedEnv = "GOMADSEED"
)

// Values of BuggifySeedEnv that don't name a seed.
const (
	// BuggifySeedRandom picks a random seed. The caller should log the returned seed, so the run
	// can be reproduced with it.
	BuggifySeedRandom = "random"
	// BuggifySeedGomad derives the seed from GomadSeedEnv, so a gomad seed reproduces the BUGGIFY
	// decisions too. GomadSeedEnv alone doesn't enable BUGGIFY.
	BuggifySeedGomad = "gomad"
)

// DefaultBuggifyProbability is FoundationDB's probability that a site is activated, and that an
// activated site fires.
const DefaultBuggifyProbability = 0.25

// BuggifyConfigFromEnv returns the BUGGIFY configuration in the environment variables that lookup
// reads, such as os.LookupEnv: the seed of BuggifySeedEnv and the probabilities of
// BuggifyActivationEnv and BuggifyFireEnv. Without BuggifySeedEnv it returns the zero
// configuration, which disables BUGGIFY. It returns an error if a variable is malformed, or if a
// probability is set without a seed.
func BuggifyConfigFromEnv(lookup func(string) (string, bool)) (BuggifyConfig, error) {
	seedValue, hasSeed := lookup(BuggifySeedEnv)
	if !hasSeed || seedValue == "" {
		for _, name := range []string{BuggifyActivationEnv, BuggifyFireEnv} {
			if _, ok := lookup(name); ok {
				return BuggifyConfig{}, fmt.Errorf("%s requires %s", name, BuggifySeedEnv)
			}
		}
		return BuggifyConfig{}, nil
	}
	seed, err := parseBuggifySeed(seedValue, lookup)
	if err != nil {
		return BuggifyConfig{}, err
	}
	config := BuggifyConfig{Seed: seed, Activation: DefaultBuggifyProbability, Fire: DefaultBuggifyProbability}
	for name, p := range map[string]*float64{BuggifyActivationEnv: &config.Activation, BuggifyFireEnv: &config.Fire} {
		s, ok := lookup(name)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(v) || v < 0 || v > 1 {
			return BuggifyConfig{}, fmt.Errorf("invalid %s %q: want a probability in [0, 1]", name, s)
		}
		*p = v
	}
	return config, nil
}

func parseBuggifySeed(value string, lookup func(string) (string, bool)) (uint64, error) {
	switch value {
	case BuggifySeedRandom:
		var b [8]byte
		for {
			_, _ = rand.Read(b[:])
			if seed := binary.LittleEndian.Uint64(b[:]); seed != 0 {
				return seed, nil
			}
		}
	case BuggifySeedGomad:
		gomadSeed, ok := lookup(GomadSeedEnv)
		if !ok || gomadSeed == "" {
			return 0, fmt.Errorf("%s=%s requires %s", BuggifySeedEnv, BuggifySeedGomad, GomadSeedEnv)
		}
		// gomad seeds can be 0, so the seed is derived rather than taken as is.
		h := fnv.New64a()
		_, _ = h.Write([]byte("gomad:" + gomadSeed))
		return max(h.Sum64(), 1), nil
	default:
		seed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seed == 0 {
			return 0, fmt.Errorf("invalid %s %q: want a positive integer, %s or %s", BuggifySeedEnv, value, BuggifySeedRandom, BuggifySeedGomad)
		}
		return seed, nil
	}
}
