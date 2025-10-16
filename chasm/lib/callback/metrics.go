package callback

import "go.temporal.io/server/common/metrics"

var RequestCounter = metrics.NewCounterDef(
	"callback_outbound_requests",
	metrics.WithDescription("The number of callback outbound requests made by the history service."),
)
var RequestLatencyHistogram = metrics.NewTimerDef(
	"callback_outbound_latency",
	metrics.WithDescription("Latency histogram of outbound callback requests made by the history service."),
)
