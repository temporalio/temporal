package wideevents

// NamespaceLifecycle phase values. These are the published contract that queries key on, so they
// live here rather than next to their emitters; the payloads themselves are built at the emitting
// call site, which is what has the data.

// Namespace handover. A handover cannot complete until every shard's replication watermark has
// been acked by the target, so these cover both ends of it: the per-shard watermark bookkeeping
// on the history side, and the wait that blocks on it on the worker side.
const (
	PhaseHandoverWatermarkSet     = "shard_handover_watermark_set"
	PhaseHandoverWatermarkRemoved = "shard_handover_watermark_removed"
	PhaseHandoverIncomplete       = "shard_handover_incomplete"
)

// Reasons for a watermark set, distinguishing the two ways a shard arrives at one.
const (
	// WatermarkAdded: the shard saw this namespace enter handover and took a watermark.
	WatermarkAdded = "added"
	// WatermarkUpdated: a newer namespace notification advanced the watermark.
	WatermarkUpdated = "updated"
)
