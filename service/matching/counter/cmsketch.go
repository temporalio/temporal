package counter

import (
	"hash/maphash"
	"math"
	"math/bits"
	"math/rand/v2"
)

type (
	CMSketchParams struct {
		W    int // width of sketch
		D    int // depth of sketch
		Grow CMSGrowParams
	}

	CMSGrowParams struct {
		SkipRateDecay int     // how often (in calls to Inc) the skip rate is halved (this makes it an exponential moving average)
		Threshold     float64 // at what skip ratio should we grow W
		Ratio         float64 // how much to grow W each time
		MaxW          int     // cap for W
	}

	// cmSketch is a Counter that uses a count-min sketch.
	// cmSketch is not safe for concurrent use.
	cmSketch struct {
		params CMSketchParams
		seed0  maphash.Seed
		seeds  []uint64
		// TODO: use uint32 with a sliding window
		cells []int64

		skips, incs int // used to calculate skip rate
	}
)

var _ Counter = (*cmSketch)(nil)

func NewCMSketchCounter(params CMSketchParams, src rand.Source) *cmSketch {
	params.D = max(2, params.D)
	params.W = max(2, params.W)
	params.Grow.SkipRateDecay = max(1_000, params.Grow.SkipRateDecay)
	return &cmSketch{
		params: params,
		seed0:  maphash.MakeSeed(),
		seeds:  makeSeeds(params.D, src),
		cells:  make([]int64, params.W*params.D),
	}
}

func (s *cmSketch) GetPass(key string, base, inc int64) int64 {
	if inc < 0 {
		return base // we don't handle negatives here
	}

	indexes := make([]int, s.params.D)
	s.fillIndexes(key, indexes)

	current := s.getByIndexes(indexes)
	pass := max(base, current+inc)
	s.skips += s.ensureByIndexes(indexes, pass)

	if s.incs++; s.incs > s.params.Grow.SkipRateDecay {
		s.maybeGrow()
		s.skips >>= 1
		s.incs >>= 1
	}

	return int64(pass)
}

func (s *cmSketch) SkipRate() float64 {
	return float64(s.skips) / float64(s.incs*s.params.D)
}

func (s *cmSketch) EstimateDistinctKeys() int {
	// TODO: improve this estimate with more math
	count := 0
	for _, d := range s.cells {
		if d > 0 {
			count++
		}
	}
	return count / s.params.D
}

func (s *cmSketch) fillIndexes(k string, indexes []int) {
	w := s.params.W
	// get 64 bits of hash
	h0 := maphash.String(s.seed0, k)

	for i, seed := range s.seeds {
		h1 := bits.RotateLeft64(h0, i*39)
		h2l := mix(uint32(h1), uint32(seed))
		h2h := mix(uint32(h1>>32), uint32(seed>>32))
		h3 := mix(h2l, h2h)
		// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
		indexes[i] = i*w + int((uint64(h3)*uint64(w))>>32)
	}
}

func (s *cmSketch) maybeGrow() {
	if s.params.Grow.Threshold == 0 ||
		s.params.Grow.Ratio == 0 ||
		s.params.W >= s.params.Grow.MaxW ||
		s.SkipRate() < s.params.Grow.Threshold {
		return
	}
	// TODO: instead of throwing away the whole sketch, try to preserve a sample of large values
	s.params.W = min(int(float64(s.params.W)*s.params.Grow.Ratio), s.params.Grow.MaxW)
	s.seed0 = maphash.MakeSeed()
	s.cells = make([]int64, s.params.W*s.params.D)
	s.skips, s.incs = 0, 0
}

func (s *cmSketch) getByIndexes(indexes []int) int64 {
	// TODO: consider using better estimator: https://dl.acm.org/doi/pdf/10.1145/3219819.3219975
	minVal := int64(math.MaxInt64)
	for _, idx := range indexes {
		minVal = min(minVal, s.cells[idx])
	}
	return minVal
}

func (s *cmSketch) ensureByIndexes(indexes []int, target int64) (skips int) {
	for _, idx := range indexes {
		if s.cells[idx] < target {
			s.cells[idx] = target
		} else {
			skips++
		}
	}
	return
}

func makeSeeds(d int, src rand.Source) []uint64 {
	out := make([]uint64, d)
	for i := range out {
		out[i] = src.Uint64()
	}
	return out
}

// from https://www.pcg-random.org/posts/developing-a-seed_seq-alternative.html
func mix(x, y uint32) uint32 {
	result := 0xca01f9dd*x - 0x4973f715*y
	result ^= result >> 16
	return result
}
