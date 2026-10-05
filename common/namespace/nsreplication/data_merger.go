package nsreplication

// NamespaceDataMerger provides custom merge logic for namespace data
// during replication task execution.
type NamespaceDataMerger interface {
	// MergeData performs a business specific merge of namespace data.
	MergeData(
		currentData map[string]string,
		taskData map[string]string,
		currentConfigVersion int64,
		taskConfigVersion int64,
	) (mergedData map[string]string, merged bool)
}

// NoopDataMerger is the default implementation that returns task data directly.
type NoopDataMerger struct{}

// NewNoopDataMerger creates a new NoopDataMerger.
func NewNoopDataMerger() NamespaceDataMerger {
	return &NoopDataMerger{}
}

// MergeData returns taskData directly without any merging.
func (n *NoopDataMerger) MergeData(
	currentData map[string]string,
	taskData map[string]string,
	_ int64,
	_ int64,
) (map[string]string, bool) {
	return taskData, false
}
