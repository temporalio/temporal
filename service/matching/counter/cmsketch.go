package counter

import (
	"hash/maphash"
	"math"
	"math/bits"
	"math/rand/v2"
)

type (
	CMSketchParams struct {
		W      int // width of sketch
		D      int // depth of sketch
		Grow   CMSGrowParams
		Reseed CMSReseedParams
	}

	CMSGrowParams struct {
		SkipRateDecay int     // how often (in calls to Inc) the skip rate is halved (this makes it an exponential moving average)
		Threshold     float64 // at what skip ratio should we grow W
		Ratio         float64 // how much to grow W each time
		MaxW          int     // cap for W
	}

	CMSReseedParams struct {
		Interval int // reseed every N operations (0 = disabled)
	}

	// topKFunc is a callback that returns the top-K entries to preserve during resize.
	topKFunc func() []TopKEntry

	// cmSketch is a Counter that uses a count-min sketch.
	// cmSketch is not safe for concurrent use.
	cmSketch struct {
		params CMSketchParams
		seed0  maphash.Seed
		seeds  []uint64 // length D+1 (D active rows + 1 shadow row)
		// cells stores offsets from base. The actual value for a cell is base + int64(cells[i]).
		// This allows us to use uint32 for storage while supporting the full int64 range.
		base  int64
		cells []uint32 // length W*(D+1)

		// shadowRow is the row index (0 to D) currently acting as the shadow.
		// The shadow row receives writes but is excluded from min calculations.
		// On reseed, shadowRow rotates and the new shadow is zeroed with a new seed.
		shadowRow int

		skips, incs int // used to calculate skip rate
		reseedOps   int // operations since last reseed

		src          rand.Source // for generating new seeds on reseed
		topKProvider topKFunc    // callback to get top-K entries on resize
	}
)

// slideHeadroom is how much space to leave in the sliding window when we need to slide
const slideHeadroom = math.MaxUint32 / 2

var _ Counter = (*cmSketch)(nil)

func NewCMSketchCounter(params CMSketchParams, src rand.Source, topKProvider topKFunc) *cmSketch {
	params.D = max(1, params.D)
	params.W = max(1, params.W)
	params.Grow.SkipRateDecay = max(1_000, params.Grow.SkipRateDecay)
	numRows := params.D + 1 // + 1 for shadow row
	return &cmSketch{
		params:       params,
		seed0:        maphash.MakeSeed(),
		seeds:        makeSeeds(numRows, src),
		cells:        make([]uint32, params.W*numRows),
		shadowRow:    0,
		src:          src,
		topKProvider: topKProvider,
	}
}

func (s *cmSketch) GetPass(key string, base, inc int64) int64 {
	if inc < 0 {
		return base // we don't handle negatives here
	}

	numRows := s.params.D + 1
	indexes := make([]int, numRows)
	s.fillIndexes(key, indexes)

	current := s.getByIndexes(indexes)
	pass := max(base, current+inc)
	s.skips += s.ensureByIndexes(indexes, pass)

	if s.incs++; s.incs > s.params.Grow.SkipRateDecay {
		s.maybeGrow()
		s.skips >>= 1
		s.incs >>= 1
	}

	if s.reseedOps++; s.params.Reseed.Interval > 0 && s.reseedOps >= s.params.Reseed.Interval {
		s.reseed()
		s.reseedOps = 0
	}

	return int64(pass)
}

func (s *cmSketch) SkipRate() float64 {
	return float64(s.skips) / float64(s.incs*s.params.D)
}

func (s *cmSketch) EstimateDistinctKeys() int {
	// this is not very accurate at all, especially with the shadow row.
	// TODO: improve this estimate with more math
	count := 0
	for _, c := range s.cells {
		if c > 0 {
			count++
		}
	}
	return count / s.params.D
}

func (s *cmSketch) TopK() []TopKEntry {
	return s.topKProvider()
}

// fillIndexes computes cell indexes for all D+1 rows (D active + 1 shadow).
// len(indexes) must == len(s.seeds) == D+1
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
	// get top entries before resetting (if provider available)
	var topK []TopKEntry
	if s.topKProvider != nil {
		topK = s.topKProvider()
	}

	numRows := s.params.D + 1
	s.params.W = min(int(float64(s.params.W)*s.params.Grow.Ratio), s.params.Grow.MaxW)
	s.seed0 = maphash.MakeSeed()
	// we're resetting everything so might as well reseed now too
	s.seeds = makeSeeds(numRows, s.src)
	s.base = 0
	s.cells = make([]uint32, s.params.W*numRows)
	s.shadowRow = s.params.D // reset shadow to last row
	s.skips, s.incs, s.reseedOps = 0, 0, 0

	if len(topK) > 0 {
		// restore top entries after resize. GetPass can in theory call back into maybeGrow,
		// but we can just reset the counters each time to prevent that.
		for _, entry := range topK {
			_ = s.GetPass(entry.Key, entry.Count, 0)
			s.skips, s.incs, s.reseedOps = 0, 0, 0
		}
	}
}

// reseed rotates the shadow row to break persistent hash collisions over time.
// The current shadow becomes active, and the next row becomes the new shadow
// (zeroed with a fresh seed). The shadow row has been accumulating writes for the
// entire previous interval, so it already has accurate counts for active keys.
func (s *cmSketch) reseed() {
	numRows := s.params.D + 1
	s.shadowRow = (s.shadowRow + 1) % numRows
	s.seeds[s.shadowRow] = s.src.Uint64()

	// clear the new shadow row
	rowStart := s.shadowRow * s.params.W
	for i := range s.params.W {
		s.cells[rowStart+i] = 0
	}
}

func (s *cmSketch) getByIndexes(indexes []int) int64 {
	// TODO: consider using better estimator: https://dl.acm.org/doi/pdf/10.1145/3219819.3219975
	minVal := uint32(math.MaxUint32)
	for i, idx := range indexes {
		if i == s.shadowRow {
			continue // skip shadow row for reads
		}
		minVal = min(minVal, s.cells[idx])
	}
	return s.base + int64(minVal)
}

func (s *cmSketch) ensureByIndexes(indexes []int, target int64) (skips int) {
	offset := target - s.base
	if offset < 0 {
		// target is below our window floor, all cells are already high enough
		return s.params.D // only count active rows for skips
	}
	if offset > math.MaxUint32 {
		// would overflow uint32, need to slide the base up first
		s.slideBase(offset + slideHeadroom - math.MaxUint32)
		offset = math.MaxUint32 - slideHeadroom
	}

	uoffset := uint32(offset)
	for i, idx := range indexes {
		if s.cells[idx] < uoffset {
			s.cells[idx] = uoffset
		} else if i != s.shadowRow {
			skips++ // only count skips for active rows, not shadow
		}
	}
	return
}

// slideBase increases the base by delta, subtracting delta from all cells.
// Cells that would go negative are clamped to 0 (those keys are "dragged up").
func (s *cmSketch) slideBase(delta int64) {
	if delta <= 0 {
		return
	}
	s.base += delta
	if delta >= math.MaxUint32 {
		for i := range s.cells {
			s.cells[i] = 0
		}
		return
	}
	// delta fits in uint32
	udelta := uint32(delta)
	for i := range s.cells {
		if s.cells[i] > udelta {
			s.cells[i] -= udelta
		} else {
			s.cells[i] = 0
		}
	}
}

func makeSeeds(rows int, src rand.Source) []uint64 {
	out := make([]uint64, rows)
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
