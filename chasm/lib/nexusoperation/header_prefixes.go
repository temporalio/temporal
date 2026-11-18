package nexusoperation

import (
	"strconv"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
)

const (
	// HeaderPrefixSourceWorkflow identifies headers set via a ScheduleNexusOperation workflow command.
	HeaderPrefixSourceWorkflow = "workflow"
	// HeaderPrefixSourceStandalone identifies headers set via a StartNexusOperationExecution request.
	HeaderPrefixSourceStandalone = "standalone"

	headerPrefixSourceTagName   = "request_source"
	headerPrefixTagName         = "prefix"
	headerPrefixRejectedTagName = "rejected"
)

// RecordHeaderPrefixUsage emits a metric per matched prefix and a warning log for a request whose header keys start
// with tracked or rejected prefixes. The logger is expected to be throttled since this may be called on every request.
func RecordHeaderPrefixUsage(
	metricsHandler metrics.Handler,
	logger log.Logger,
	namespace string,
	source string,
	matches commonnexus.HeaderPrefixMatches,
	rejected bool,
	tags ...tag.Tag,
) {
	counter := HeaderPrefixUsageCounter.With(metricsHandler)
	for _, match := range matches {
		counter.Record(
			1,
			metrics.NamespaceTag(namespace),
			metrics.StringTag(headerPrefixSourceTagName, source),
			metrics.StringTag(headerPrefixTagName, match.Prefix),
			metrics.StringTag(headerPrefixRejectedTagName, strconv.FormatBool(rejected)),
		)
	}
	logger.Warn("Nexus Operation request header keys start with a tracked or rejected prefix.",
		append([]tag.Tag{
			tag.WorkflowNamespace(namespace),
			tag.NewStringsTag("header-prefixes", matches.Prefixes()),
			tag.NewStringsTag("header-keys", matches.Keys()),
			tag.NewStringTag(headerPrefixSourceTagName, source),
			tag.NewBoolTag(headerPrefixRejectedTagName, rejected),
		}, tags...)...,
	)
}
