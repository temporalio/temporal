package faultinjection

import (
	"maps"
	"math/rand"
	"slices"
	"sync"
	"time"
)

type entry struct {
	name      string
	rate      float64
	threshold float64
}

type sampler struct {
	mu      sync.Mutex
	rnd     *rand.Rand
	total   float64
	entries []entry
}

// newSampler expects probabilities validated by Config.Validate.
func newSampler(cfg Config) *sampler {
	entries := make([]entry, 0, len(cfg.Errors))
	total := 0.0
	for _, name := range slices.Sorted(maps.Keys(cfg.Errors)) {
		probability := cfg.Errors[name]
		total += probability
		entries = append(entries, entry{name: name, rate: probability, threshold: total})
	}
	seedNano := cfg.Seed
	if seedNano == 0 {
		seedNano = time.Now().UnixNano()
	}
	return &sampler{
		rnd:     rand.New(rand.NewSource(seedNano)),
		total:   total,
		entries: entries,
	}
}

// sample returns no value when the roll falls outside the configured probabilities.
func (s *sampler) sample() *entry {
	if s.total > 0 {
		s.mu.Lock()
		roll := s.rnd.Float64()
		s.mu.Unlock()
		for i := range s.entries {
			if roll < s.entries[i].threshold {
				return &s.entries[i]
			}
		}
	}
	return nil
}
