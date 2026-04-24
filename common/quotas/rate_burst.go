package quotas

import (
	"math"
	"sync/atomic"
)

var (
	_ RateBurst = (*RateBurstImpl)(nil)
	_ RateBurst = (*MutableRateBurstImpl)(nil)
	_ RateBurst = (*NamespaceRateBurstImpl)(nil)
	_ RateBurst = (*OperatorRateBurstImpl)(nil)
)

var (
	DefaultIncomingBurstRatioFn = func() float64 {
		return defaultIncomingRateBurstRatio
	}
	DefaultOutgoingBurstRatioFn = func() float64 {
		return defaultOutgoingRateBurstRatio
	}
	DefaultIncomingNamespaceBurstRatioFn = func(_ string) float64 {
		return defaultIncomingRateBurstRatio
	}
	DefaultOutgoingNamespaceBurstRatioFn = func(_ string) float64 {
		return defaultOutgoingRateBurstRatio
	}
)

type (
	// RateFn returns a float64 as the RPS
	RateFn func() float64

	// NamespaceRateFn returns a float64 as the RPS for the given namespace
	NamespaceRateFn func(namespace string) float64

	// BurstFn returns an int as the burst / bucket size
	BurstFn func() int

	// BurstRatioFn returns a float as the ratio of burst to rate
	BurstRatioFn func() float64

	// NamespaceBurstFn returns an int as the burst / bucket size for the given namespace
	NamespaceBurstFn func(namespace string) int

	// NamespaceBurstRatioFn returns a float as the ratio of burst to rate for the given namespace
	NamespaceBurstRatioFn func(namespace string) float64

	// RateBurst returns rate & burst for rate limiter
	RateBurst interface {
		Rate() float64
		Burst() int
	}

	RateBurstImpl struct {
		rateFn  RateFn
		burstFn BurstFn
	}

	// MutableRateBurstImpl stores the dynamic rate & burst for rate limiter
	MutableRateBurstImpl struct {
		rate  atomic.Uint64
		burst atomic.Int64
	}

	MutableRateBurst interface {
		SetRPS(rps float64)
		SetBurst(burst int)
		RateBurst
	}

	NamespaceRateBurstImpl struct {
		namespaceName string
		rateFn        NamespaceRateFn
		burstFn       NamespaceBurstFn
	}

	OperatorRateBurstImpl struct {
		operatorRateRatio func() float64
		baseRateBurstFn   RateBurst
	}
)

func NewRateBurst(
	rateFn RateFn,
	burstFn BurstFn,
) *RateBurstImpl {
	return &RateBurstImpl{
		rateFn:  rateFn,
		burstFn: burstFn,
	}
}

func NewDefaultIncomingRateBurst(
	rateFn RateFn,
) *RateBurstImpl {
	return NewDefaultRateBurst(rateFn, func() float64 {
		return defaultIncomingRateBurstRatio
	})
}

func NewDefaultOutgoingRateBurst(
	rateFn RateFn,
) *RateBurstImpl {
	return NewDefaultRateBurst(rateFn, func() float64 {
		return defaultOutgoingRateBurstRatio
	})
}

func NewDefaultRateBurst(
	rateFn RateFn,
	rateToBurstRatio BurstRatioFn,
) *RateBurstImpl {
	burstFn := func() int {
		rate := rateFn()
		if rate < 0 {
			rate = 0
		}

		ratio := rateToBurstRatio()
		if ratio < 0 {
			ratio = 0
		}
		burst := int(rate * ratio)
		if burst == 0 && rate > 0 && ratio > 0 {
			burst = 1
		}
		return burst
	}
	return NewRateBurst(rateFn, burstFn)
}

func (d *RateBurstImpl) Rate() float64 {
	return d.rateFn()
}

func (d *RateBurstImpl) Burst() int {
	return d.burstFn()
}

func NewMutableRateBurst(
	rate float64,
	burst int,
) *MutableRateBurstImpl {
	d := &MutableRateBurstImpl{}
	d.SetRPS(rate)
	d.SetBurst(burst)

	return d
}

func (d *MutableRateBurstImpl) SetRPS(rate float64) {
	d.rate.Store(math.Float64bits(rate))
}

func (d *MutableRateBurstImpl) SetBurst(burst int) {
	d.burst.Store(int64(burst))
}

func (d *MutableRateBurstImpl) Rate() float64 {
	return math.Float64frombits(d.rate.Load())
}

func (d *MutableRateBurstImpl) Burst() int {
	return int(d.burst.Load())
}

func NewNamespaceRateBurst(
	namespaceName string,
	rateFn NamespaceRateFn,
	burstRatioFn NamespaceBurstRatioFn,
) *NamespaceRateBurstImpl {
	return &NamespaceRateBurstImpl{
		namespaceName: namespaceName,
		rateFn:        rateFn,
		burstFn: func(namespace string) int {
			return max(1, int(math.Ceil(rateFn(namespace)*burstRatioFn(namespace))))
		},
	}
}

func (n *NamespaceRateBurstImpl) Rate() float64 {
	return n.rateFn(n.namespaceName)
}

func (n *NamespaceRateBurstImpl) Burst() int {
	return n.burstFn(n.namespaceName)
}

func NewOperatorRateBurst(
	baseRateBurstFn RateBurst,
	operatorRateRatio func() float64,
) *OperatorRateBurstImpl {
	return &OperatorRateBurstImpl{
		operatorRateRatio: operatorRateRatio,
		baseRateBurstFn:   baseRateBurstFn,
	}
}

func (c *OperatorRateBurstImpl) Rate() float64 {
	return c.operatorRateRatio() * c.baseRateBurstFn.Rate()
}

func (c *OperatorRateBurstImpl) Burst() int {
	return int(c.operatorRateRatio() * float64(c.baseRateBurstFn.Burst()))
}
