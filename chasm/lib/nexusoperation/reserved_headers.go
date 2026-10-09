package nexusoperation

import (
	"strconv"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	// ReservedHeaderSourceWorkflow identifies headers set via a ScheduleNexusOperation workflow command.
	ReservedHeaderSourceWorkflow = "workflow"
	// ReservedHeaderSourceStandalone identifies headers set via a StartNexusOperationExecution request.
	ReservedHeaderSourceStandalone = "standalone"

	reservedHeaderSourceTagName   = "request_source"
	reservedHeaderRejectedTagName = "rejected"
)

// RecordReservedHeaderUsage emits a metric and a warning log for a request whose headers include keys with the
// reserved prefix. The logger is expected to be throttled since this may be called on every request.
func RecordReservedHeaderUsage(
	metricsHandler metrics.Handler,
	logger log.Logger,
	namespace string,
	source string,
	keys []string,
	rejected bool,
	tags ...tag.Tag,
) {
	ReservedHeaderUsageCounter.With(metricsHandler).Record(
		1,
		metrics.NamespaceTag(namespace),
		metrics.StringTag(reservedHeaderSourceTagName, source),
		metrics.StringTag(reservedHeaderRejectedTagName, strconv.FormatBool(rejected)),
	)
	logger.Warn("Nexus Operation request headers use the reserved prefix.",
		append([]tag.Tag{
			tag.WorkflowNamespace(namespace),
			tag.NewStringsTag("headers", keys),
			tag.NewStringTag(reservedHeaderSourceTagName, source),
			tag.NewBoolTag(reservedHeaderRejectedTagName, rejected),
		}, tags...)...,
	)
}
