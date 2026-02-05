package nsreplication

// NamespaceDataMerger provides custom merge logic for namespace data
// during replication task execution.
//
// In OSS, this is a no-op that returns task data directly.
// In SaaS, this can implement custom merge logic for MCN config data fields.
type NamespaceDataMerger interface {
	// MergeData performs a conflict-free merge of namespace data.
	// This handles CRDT-style merges where both current and task data may have
	// valid entries that should be combined.
	// Returns the merged data and whether any merge occurred.
	// In OSS, this is a no-op that returns taskData and merged=false.
	MergeData(currentData, taskData map[string]string) (mergedData map[string]string, merged bool)
}

// NoopDataMerger is the default implementation that returns task data directly.
type NoopDataMerger struct{}

// NewNoopDataMerger creates a new NoopDataMerger.
func NewNoopDataMerger() NamespaceDataMerger {
	return &NoopDataMerger{}
}

// MergeData returns taskData directly without any merging.
func (n *NoopDataMerger) MergeData(currentData, taskData map[string]string) (map[string]string, bool) {
	return taskData, false
}
