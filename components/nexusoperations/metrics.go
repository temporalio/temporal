package nexusoperations

import "go.temporal.io/server/common/metrics"

var OutboundRequestCounter = metrics.NewCounterDef(
	"nexus_outbound_requests",
	metrics.WithDescription("The number of Nexus outbound requests made by the history service."),
)
var OutboundRequestLatency = metrics.NewTimerDef(
	"nexus_outbound_latency",
	metrics.WithDescription("Latency of outbound Nexus requests made by the history service."),
)

type NexusMetricTagConfig struct {
	// Include service name as a metric tag
	IncludeServiceTag bool
	// Include operation name as a metric tag
	IncludeOperationTag bool
	// Configuration for mapping request headers to metric tags
	HeaderTagMappings []NexusHeaderTagMapping
}

type NexusHeaderTagMapping struct {
	// Name of the request header to extract value from
	SourceHeader string
	// Name of the metric tag to set with the header value
	TargetTag string
}
