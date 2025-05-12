package faultinjection

import (
	"math/rand"
	"sync"
	"time"
)

type (
	faultMetadata struct {
		fault     fault
		threshold float64 // threshold for this fault to be returned
	}

	methodFaultGenerator struct {
		rndMu sync.Mutex
		rnd   *rand.Rand // rand is not thread-safe

		rate           float64         // chance for one of the errors for this method to be returned
		faultsMetadata []faultMetadata // faults with their thresholds that might be generated for this method
	}
)

func newMethodFaultGenerator(faults []fault, seed int64) *methodFaultGenerator {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	fm := make([]faultMetadata, 0, len(faults))
	totalRate := 0.0
	for _, f := range faults {
		totalRate += f.rate
		fm = append(fm, faultMetadata{
			fault:     f,
			threshold: totalRate,
		})
	}

	return &methodFaultGenerator{
		rate:           totalRate,
		faultsMetadata: fm,
		rnd:            rand.New(rand.NewSource(seed)),
	}
}

func (p *methodFaultGenerator) generate(_ string) *fault {
	if p.rate <= 0 {
		return nil
	}

	p.rndMu.Lock()
	roll := p.rnd.Float64()
	p.rndMu.Unlock()

	if roll < p.rate {
		// Yes, this method call should be failed.
		// Let's find out with what fault.
		for i := range p.faultsMetadata {
			if roll < p.faultsMetadata[i].threshold {
				return &p.faultsMetadata[i].fault
			}
		}
	}
	return nil
}
