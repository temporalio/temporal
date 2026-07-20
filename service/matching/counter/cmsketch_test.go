package counter

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCMSketch_Basic(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	// one key
	assert.Equal(t, int64(1), cms.GetPass("one", 0, 1))
	assert.Equal(t, int64(5), cms.GetPass("one", 0, 4))
	assert.Equal(t, int64(35), cms.GetPass("one", 0, 30))

	// base
	assert.Equal(t, int64(1000), cms.GetPass("one", 1000, 5))

	// more increments
	assert.Equal(t, int64(1008), cms.GetPass("one", 0, 8))
}

func TestCMSketch_CrossMaxInt32(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	for _, base := range []int64{
		math.MaxInt32 - 123,
		math.MaxUint32 - 123,
	} {
		cms.GetPass("one", base, 0)
		cms.GetPass("one", 0, 50)
		cms.GetPass("one", 0, 50)
		cms.GetPass("one", 0, 50)
		cms.GetPass("one", 0, 50)
		assert.Equal(t, base+200, cms.GetPass("one", base, 0))
	}
}

func TestCMSketch_Grow(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{
		W: 10,
		D: 3,
		Grow: CMSGrowParams{
			SkipRateDecay: 1_000,
			Threshold:     0.1,
			Ratio:         2,
			MaxW:          10_000,
		},
	}, src, nil)

	for i := range 1000 {
		cms.GetPass(fmt.Sprintf("key%d", i), 0, 1)
	}
	assert.Equal(t, 10, cms.params.W)
	cms.GetPass("onemore", 0, 1)
	assert.Greater(t, cms.params.W, 10)
}

func TestCMSketch_Grow_PreservedOnResize(t *testing.T) {
	var topKCalls int
	topK := func() []TopKEntry {
		topKCalls++
		return []TopKEntry{
			TopKEntry{Key: "topkey1", Count: 9999},
			TopKEntry{Key: "topkey2", Count: 99999},
		}
	}

	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{
		W: 10,
		D: 3,
		Grow: CMSGrowParams{
			SkipRateDecay: 1_000,
			Threshold:     0.1,
			Ratio:         2,
			MaxW:          10_000,
		},
	}, src, topK)

	for i := range 1000 {
		cms.GetPass(fmt.Sprintf("key%d", i), 0, 1)
	}
	assert.Equal(t, 10, cms.params.W)
	assert.Zero(t, topKCalls)
	cms.GetPass("onemore", 0, 1)
	assert.Greater(t, cms.params.W, 10)
	assert.Equal(t, 1, topKCalls)

	assert.GreaterOrEqual(t, cms.GetPass("topkey1", 0, 1), int64(9999))
	assert.GreaterOrEqual(t, cms.GetPass("topkey2", 0, 1), int64(99999))
}

func TestCMSketch_SlideBase(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	// start with a value that will require sliding when we add more
	startBase := int64(math.MaxUint32) - 100
	p1 := cms.GetPass("hot", startBase, 0)
	assert.Equal(t, startBase, p1)
	assert.Equal(t, int64(0), cms.base, "base should still be 0, value fits in uint32")

	// push past MaxUint32, which should trigger a slide
	p2 := cms.GetPass("hot", 0, 200)
	assert.Equal(t, startBase+200, p2)
	assert.Positive(t, cms.base, "base should have slid up")

	// the value should still be correct after sliding
	p3 := cms.GetPass("hot", 0, 0)
	assert.Equal(t, startBase+200, p3)
}

func TestCMSketch_SlideBase_DragUp(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	// set up a cold key at a low value
	coldPass := cms.GetPass("cold", 0, 100)
	assert.Equal(t, int64(100), coldPass)

	// set up a hot key near the uint32 boundary
	hotStart := int64(math.MaxUint32) - 50
	cms.GetPass("hot", hotStart, 0)

	// push hot key past the boundary, triggering a slide
	cms.GetPass("hot", 0, 200)

	// cold key should have been dragged up to the new base
	coldPassAfter := cms.GetPass("cold", 0, 0)
	assert.Greater(t, coldPassAfter, coldPass, "cold key should have been dragged up")
	assert.Equal(t, cms.base, coldPassAfter, "cold key should be at or above base")
}

func TestCMSketch_SlideBase_Headroom(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	// push a key just past MaxUint32 to trigger a slide
	startBase := int64(math.MaxUint32) + 1
	cms.GetPass("key", startBase, 0)

	// after sliding, there should be some headroom so we can do many more increments
	// without another slide
	baseBeforeIncs := cms.base
	for range 1000 {
		cms.GetPass("key", 0, 1000)
	}
	assert.Equal(t, baseBeforeIncs, cms.base, "base should not have changed after moderate increments")
}

func TestCMSketch_SlideBase_TargetBelowBase(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	// push past MaxUint32 to establish a high base
	highValue := int64(math.MaxUint32) + 1000
	cms.GetPass("key", highValue, 0)
	assert.Positive(t, cms.base)

	// try to set a value below the base, should be a no-op
	result := cms.GetPass("key", 100, 0) // base arg of 100 is below cms.base
	assert.Equal(t, highValue, result, "should return current value, not the low base")

	// new key with a low base should get dragged up to the current base
	newKeyResult := cms.GetPass("newkey", 100, 0)
	assert.Equal(t, cms.base, newKeyResult, "new key should be at base")
}

func TestCMSketch_SlideBase_MultipleSlides(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	// do multiple slides by repeatedly jumping past the uint32 boundary
	const jump = int64(math.MaxUint32 * 3 / 4)
	var lastPass int64
	for range 5 {
		lastPass = cms.GetPass("key", lastPass+jump, 0)
	}

	// values should still be tracked correctly
	assert.Equal(t, 5*jump, lastPass)
}

func TestCMSketch_Reseed(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	interval := 20
	cms := NewCMSketchCounter(CMSketchParams{
		W:      100,
		D:      3,
		Reseed: CMSReseedParams{Interval: interval},
	}, src, nil)

	// do interval-1 operations
	for i := range interval - 1 {
		_ = cms.GetPass(fmt.Sprintf("k%d", i), 0, 1)
	}
	assert.Equal(t, 0, cms.shadowRow, "shadow should not have rotated yet")

	// trigger reseed
	prevShadow := cms.shadowRow
	prevSeeds := slices.Clone(cms.seeds)
	_ = cms.GetPass("trigger", 0, 1)
	assert.NotEqual(t, prevShadow, cms.shadowRow, "shadow should have rotated")
	assert.NotEqual(t, prevSeeds, cms.seeds[1], "some seed should have changed")

	// counts for existing keys should not be reset
	for i := range interval - 1 {
		assert.GreaterOrEqual(t, cms.GetPass(fmt.Sprintf("k%d", i), 0, 0), int64(1))
	}
	assert.GreaterOrEqual(t, cms.GetPass("trigger", 0, 0), int64(1))
}

func TestCMSketch_Reseed_BreaksCollision(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{
		W: 5, // small to force collision
		D: 3,
	}, src, nil)

	// set up a "hot" key
	hotCount := int64(100)
	_ = cms.GetPass("hot", hotCount, 0)

	// find a key that collides with "hot" on all rows
	var collider string
	// should succeed within several hundred iterations
	for i := 0; ; i++ {
		key := fmt.Sprintf("probe%d", i)
		if cms.GetPass(key, 0, 0) >= hotCount {
			collider = key
			break
		}
	}
	t.Log("found colliding key", collider)

	for i := range 15 {
		// try reseed to break the collision. usually succeeds in 1-2 reseeds but could take
		// more if we get very unlucky.
		t.Log("reseeding", i+1)
		cms.reseed()
		hotCount++
		_ = cms.GetPass("hot", hotCount, 0)
		if cms.GetPass(collider, 0, 0) < hotCount {
			return
		}
	}
	assert.Fail(t, "couldn't break collision after multiple tries")
}

func TestCMSketch_SlideBase_LargeDelta(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src, nil)

	// set a cell to some value
	cms.GetPass("key1", 1000, 0)

	const delta = int64(math.MaxUint32) + 1
	cms.slideBase(delta)

	// all cells should be zero
	for _, val := range cms.cells {
		assert.Zero(t, val)
	}
	assert.Equal(t, delta, cms.base)
}
