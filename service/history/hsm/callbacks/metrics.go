package callbacks

import (
	chasmcallbacks "go.temporal.io/server/chasm/lib/callback"
)

var (
	RequestCounter          = chasmcallbacks.RequestCounter
	RequestLatencyHistogram = chasmcallbacks.RequestLatencyHistogram
)
