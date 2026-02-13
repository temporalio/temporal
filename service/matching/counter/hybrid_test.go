package counter

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHybridCounter_StartsWithMap(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	h := NewHybridCounter(CounterParams{
		MapLimit: 10,
		CMS:      CMSketchParams{W: 10, D: 3},
	}, src)

	// using mapCounter initially
	assert.Nil(t, h.cmSketch)

	for i := range 5 {
		h.GetPass(fmt.Sprintf("key%d", i), 0, int64(i+1))
	}

	// should still be using mapCounter
	assert.Nil(t, h.cmSketch)
}

func TestHybridCounter_Migrates(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	h := NewHybridCounter(CounterParams{
		MapLimit: 5,
		CMS:      CMSketchParams{W: 100, D: 3},
	}, src)

	// add entries up to limit
	for i := range 5 {
		h.GetPass(fmt.Sprintf("key%d", i), 0, int64(i+1))
	}
	assert.Nil(t, h.cmSketch, "should still be map at limit")

	// one more should trigger migration
	h.GetPass("trigger", 0, 100)
	assert.NotNil(t, h.cmSketch, "should have migrated")
}

func TestHybridCounter_TopKTracking(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	h := NewHybridCounter(CounterParams{
		MapLimit: 3,
		CMS:      CMSketchParams{W: 100, D: 3},
	}, src)

	h.GetPass("low", 0, 1)
	h.GetPass("mid", 0, 5)
	h.GetPass("high", 0, 10)
	h.GetPass("trigger", 0, 3) // triggers migration

	require.NotNil(t, h.cmSketch, "should have migrated")

	assert.ElementsMatch(t, []TopKEntry{
		TopKEntry{Key: "mid", Count: 5},
		TopKEntry{Key: "high", Count: 10},
		TopKEntry{Key: "trigger", Count: 3},
	}, h.TopK())
}

func TestHybridCounter_TopKUpdatesAfterMigration(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	h := NewHybridCounter(CounterParams{
		MapLimit: 2,
		CMS:      CMSketchParams{W: 100, D: 3},
	}, src)

	h.GetPass("a", 0, 10)
	h.GetPass("b", 0, 20)
	h.GetPass("c", 0, 5) // triggers migration

	require.NotNil(t, h.cmSketch)

	// TopK should still update
	h.GetPass("d", 0, 100)
	assert.ElementsMatch(t, []TopKEntry{
		TopKEntry{Key: "d", Count: 100},
		TopKEntry{Key: "b", Count: 20},
	}, h.TopK())
}

func TestHybridCounter_TopKPreservedOnCMSResize(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	h := NewHybridCounter(CounterParams{
		MapLimit: 5,
		CMS: CMSketchParams{
			W: 10,
			D: 5,
			Grow: CMSGrowParams{
				SkipRateDecay: 1_000,
				Threshold:     0.1,
				Ratio:         2,
				MaxW:          1000,
			},
		},
	}, src)

	// add keys until we see a resize
	for i := 0; true; i++ {
		if h.cmSketch == nil {
			h.GetPass(fmt.Sprintf("key%d", i), 0, 1)
			continue
		}

		prevTop := h.TopK()
		prevW := h.cmSketch.params.W

		h.GetPass(fmt.Sprintf("key%d", i), 0, 1)

		if h.cmSketch.params.W > prevW {
			// we resized, now check
			for _, entry := range prevTop {
				assert.GreaterOrEqual(t, h.cmSketch.GetPass(entry.Key, 0, 1), entry.Count)
			}
			break
		}
	}
}
