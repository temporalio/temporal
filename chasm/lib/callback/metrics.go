package callback

import "go.temporal.io/server/common/metrics"

var RequestCounter = metrics.NewCounterDef(
	"chasm_callback_outbound_requests",
	metrics.WithDescription("The number of CHASM outbound callback requests made by the history service."),
)
var RequestLatencyHistogram = metrics.NewTimerDef(
	"chasm_callback_outbound_latency",
	metrics.WithDescription("Latency histogram of CHASM outbound callback requests made by the history service."),
)
