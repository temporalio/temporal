package callback

import "go.temporal.io/server/components/callbacks"

// Re-export the callback metrics from components/callbacks to maintain a consistent interface.
// Both the HSM-based callbacks (components/callbacks) and CHASM callbacks use the same metrics,
// allowing for unified monitoring and drop-in compatibility.
var (
	RequestCounter          = callbacks.RequestCounter
	RequestLatencyHistogram = callbacks.RequestLatencyHistogram
)
