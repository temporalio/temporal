package nsreplication

// NamespaceDataMerger provides custom merge logic for namespace data
// during replication task execution.
type NamespaceDataMerger interface {
	// MergeData performs a business specific merge of namespace data.
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
