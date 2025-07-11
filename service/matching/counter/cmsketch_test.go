package counter

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCMSketch_Basic(t *testing.T) {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src)

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
	cms := NewCMSketchCounter(CMSketchParams{W: 10, D: 3}, src)

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
	}, src)

	for i := range 1000 {
		cms.GetPass(fmt.Sprintf("key%d", i), 0, 1)
	}
	assert.Equal(t, 10, cms.params.W)
	cms.GetPass("onemore", 0, 1)
	assert.Greater(t, cms.params.W, 10)
}
