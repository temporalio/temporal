package probability

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"
)

type entry[T any] struct {
	value     T
	threshold float64
}

type Sampler[T any] struct {
	mu      sync.Mutex
	rnd     *rand.Rand
	total   float64
	entries []entry[T]
}

// NewSampler expects probabilities validated by ValidateProbabilities at the config boundary.
func NewSampler[T any](probabilities map[string]float64, seed int64, value func(string, float64) T) *Sampler[T] {
	entries := make([]entry[T], 0, len(probabilities))
	total := 0.0
	for _, name := range slices.Sorted(maps.Keys(probabilities)) {
		probability := probabilities[name]
		total += probability
		entries = append(entries, entry[T]{value: value(name, probability), threshold: total})
	}
	seedNano := seed
	if seedNano == 0 {
		seedNano = time.Now().UnixNano()
	}
	return &Sampler[T]{
		rnd:     rand.New(rand.NewSource(seedNano)),
		total:   total,
		entries: entries,
	}
}

// Sample returns no value when the roll falls outside the configured probabilities.
func (s *Sampler[T]) Sample() (T, bool) {
	if s.total > 0 {
		s.mu.Lock()
		roll := s.rnd.Float64()
		s.mu.Unlock()
		for _, entry := range s.entries {
			if roll < entry.threshold {
				return entry.value, true
			}
		}
	}
	var zero T
	return zero, false
}

func ValidateProbabilities(probabilities map[string]float64) error {
	total := 0.0
	for _, name := range slices.Sorted(maps.Keys(probabilities)) {
		probability := probabilities[name]
		if math.IsNaN(probability) || probability < 0 || probability > 1 {
			return fmt.Errorf("errors.%s: probability must be between 0 and 1", name)
		}
		total += probability
	}
	if total > 1 {
		return errors.New("error probabilities must sum to at most 1")
	}
	return nil
}
