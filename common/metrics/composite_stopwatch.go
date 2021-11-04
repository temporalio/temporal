package metrics

import (
	"time"
)

type CompositeStopwatch struct {
	items []Stopwatch
}

func NewCompositeStopwatch(items ...Stopwatch) *CompositeStopwatch {
	return &CompositeStopwatch{
		items: items,
	}
}

func (c CompositeStopwatch) Stop() {
	for _, stopwatch := range c.items {
		stopwatch.Stop()
	}
}

func (c CompositeStopwatch) Subtract(d time.Duration) {
	for _, stopwatch := range c.items {
		stopwatch.Subtract(d)
	}
}
