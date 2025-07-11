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
	// then switches to a cmSketch. hybridCounter is not safe for concurrent use.
	hybridCounter struct {
		Counter
		params CounterParams
		src    rand.Source
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
		Counter: NewMapCounter(),
		params:  params,
		src:     src,
	}
}

func (h *hybridCounter) GetPass(key string, base int64, inc int64) int64 {
	p := h.Counter.GetPass(key, base, inc)
	if m, ok := h.Counter.(*mapCounter); ok && len(m.m) > h.params.MapLimit {
		h.migrateToCMS(m.m)
	}
	return p
}

func (h *hybridCounter) migrateToCMS(m map[string]int64) {
	cms := NewCMSketchCounter(h.params.CMS, h.src)
	// move existing counts into CMS
	for key, count := range m {
		_ = cms.GetPass(key, count, 0)
	}
	h.Counter = cms
}
