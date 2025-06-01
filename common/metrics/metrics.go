//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination metrics_mock.go

package metrics

import (
	"io"
	"time"

	"go.temporal.io/server/common/log"
)

// Mostly cribbed from
// https://github.com/temporalio/sdk-go/blob/master/internal/common/metrics/handler.go
// and adapted to depend on golang.org/x/exp/event
type (
	// Handler is a wrapper around a metrics client.
	// If you are interacting with metrics registered with New*Def functions, e.g. NewCounterDef, please use the With
	// method of those definitions instead of calling Counter directly on the Handler. This will ensure that you don't
	// accidentally use the wrong metric type, and you don't need to re-specify metric types or units.
	Handler interface {
		// WithTags creates a new Handler with provided Tag list.
		// Tags are merged with registered Tags from the source Handler.
		WithTags(...Tag) Handler

		// Counter obtains a counter for the given name.
		Counter(string) CounterIface

		// Gauge obtains a gauge for the given name.
		Gauge(string) GaugeIface

		// Timer obtains a timer for the given name.
		Timer(string) TimerIface

		// Histogram obtains a histogram for the given name.
		Histogram(string, MetricUnit) HistogramIface

		Stop(log.Logger)

		// StartBatch returns a BatchHandler that can emit a series of metrics as a single "wide event".
		// If wide events aren't supported in the underlying implementation, metrics can still be sent individually.
		StartBatch(string) BatchHandler
	}

	BatchHandler interface {
		Handler
		io.Closer
	}

	// CounterIface is an ever-increasing counter.
	CounterIface interface {
		// Record increments the counter value.
		// Tags provided are merged with the source MetricsHandler
		Record(int64, ...Tag)
	}
	// GaugeIface can be set to any float and represents a latest value instrument.
	GaugeIface interface {
		// Record updates the gauge value.
		// Tags provided are merged with the source MetricsHandler
		Record(float64, ...Tag)
	}

	// TimerIface records time durations.
	TimerIface interface {
		// Record sets the timer value.
		// Tags provided are merged with the source MetricsHandler
		Record(time.Duration, ...Tag)
	}

	// HistogramIface records a distribution of values.
	HistogramIface interface {
		// Record adds a value to the distribution
		// Tags provided are merged with the source MetricsHandler
		Record(int64, ...Tag)
	}

	CounterFunc   func(int64, ...Tag)
	GaugeFunc     func(float64, ...Tag)
	TimerFunc     func(time.Duration, ...Tag)
	HistogramFunc func(int64, ...Tag)
)

func (c CounterFunc) Record(v int64, tags ...Tag)       { c(v, tags...) }
func (c GaugeFunc) Record(v float64, tags ...Tag)       { c(v, tags...) }
func (c TimerFunc) Record(v time.Duration, tags ...Tag) { c(v, tags...) }
func (c HistogramFunc) Record(v int64, tags ...Tag)     { c(v, tags...) }
