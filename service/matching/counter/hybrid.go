package counter

import (
	"math/rand/v2"
)

type (
	CounterParams struct {
		MapLimit int
		CMS      CMSketchParams
	}

	// hybridCounter is a Counter that uses a mapCounter until it has params.MapLimit entries,
	// then switches to a cmSketch. After switching, it continues to track the top MapLimit
	// entries in the mapCounter, so they can be preserved when cmSketch resizes and persisted.
	// hybridCounter is not safe for concurrent use.
	hybridCounter struct {
		mapCounter mapCounter
		cmSketch   *cmSketch
		params     CounterParams
		src        rand.Source
	}
)

var _ Counter = (*hybridCounter)(nil)

var DefaultCounterParams = CounterParams{
	MapLimit: 100,
	CMS: CMSketchParams{
		W: 100,
		D: 5,
		Grow: CMSGrowParams{
			SkipRateDecay: 10_000,
			Threshold:     0.35,
			Ratio:         1.5,
			MaxW:          10_000,
		},
	},
}

func NewHybridCounter(params CounterParams, src rand.Source) *hybridCounter {
	return &hybridCounter{
		mapCounter: *NewMapCounter(params.MapLimit),
		params:     params,
		src:        src,
	}
}

func (h *hybridCounter) GetPass(key string, base int64, inc int64) int64 {
	if h.cmSketch != nil {
		p := h.cmSketch.GetPass(key, base, inc)
		// after migration, continue updating top-K tracker
		_ = h.mapCounter.updateHeap(key, p)
		return p
	}

	p, overflow := h.mapCounter.getPassWithOverflow(key, base, inc)
	if overflow {
		h.migrateToCMS()
	}
	return p
}

func (h *hybridCounter) migrateToCMS() {
	h.cmSketch = NewCMSketchCounter(h.params.CMS, h.src, h.mapCounter.TopK)
	// move existing counts into CMS
	for _, entry := range h.mapCounter.heap {
		_ = h.cmSketch.GetPass(entry.Key, entry.Count, 0)
	}
}

func (h *hybridCounter) EstimateDistinctKeys() int {
	if h.cmSketch != nil {
		return h.cmSketch.EstimateDistinctKeys()
	}
	return h.mapCounter.EstimateDistinctKeys()
}

// TopK returns the current top-K entries being tracked.
func (h *hybridCounter) TopK() []TopKEntry {
	return h.mapCounter.TopK()
}
