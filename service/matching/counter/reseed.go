package counter

import (
	"math/rand/v2"
	"time"
)

type (
	ReseedParams struct {
		Interval time.Duration // how often one seed is rotated
	}

	reseeder struct {
		params    ReseedParams
		src       rand.Source
		lastTime  time.Duration
		lastIndex int
	}
)

func newReseeder(params ReseedParams, src rand.Source) reseeder {
	return reseeder{
		params: params,
		src:    src,
	}
}

func (r *reseeder) iter(now time.Duration, seeds []uint64) {
	if r.params.Interval == 0 {
		return
	}
	elapsed := now - r.lastTime
	num := int(elapsed / r.params.Interval)
	r.lastTime += time.Duration(num) * r.params.Interval
	num = min(num, len(seeds))

	for range num {
		r.lastIndex = (r.lastIndex + 1) % len(seeds)
		seeds[r.lastIndex] = r.src.Uint64()
	}
}
