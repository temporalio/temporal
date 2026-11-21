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

// MutationDataMerger merges namespace data for snapshots that the CHASM
// namespace mutation transport delivers. Such a snapshot can be the only
// carrier of a data change, while a deployment that also replicates namespace
// data through its own messages on the legacy queue can keep the local value
// for queue snapshots. The default is the NamespaceDataMerger.
type MutationDataMerger interface {
	NamespaceDataMerger
}

// NewDefaultMutationDataMerger creates the default MutationDataMerger.
func NewDefaultMutationDataMerger(merger NamespaceDataMerger) MutationDataMerger {
	return merger
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
