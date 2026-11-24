package nsreplication

import "go.temporal.io/server/common/namespace"

// NamespaceDataMerger provides custom merge logic for namespace data
// during replication task execution.
type NamespaceDataMerger interface {
	// MergeData performs a business specific merge of namespace data. When this
	// cluster does not have the namespace yet, currentData is nil and
	// currentConfigVersion is -1.
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
// for queue snapshots. The default merges as the NamespaceDataMerger does.
type MutationDataMerger interface {
	// MergeMutationData performs a business specific merge of the data of the
	// namespace the snapshot describes, which it names so the merger can report
	// on it. When this cluster does not have the namespace yet, currentData is
	// nil and currentConfigVersion is -1.
	MergeMutationData(
		namespaceID namespace.ID,
		namespaceName namespace.Name,
		currentData map[string]string,
		taskData map[string]string,
		currentConfigVersion int64,
		taskConfigVersion int64,
	) (mergedData map[string]string, merged bool)
}

type defaultMutationDataMerger struct {
	merger NamespaceDataMerger
}

// NewDefaultMutationDataMerger creates the default MutationDataMerger, which
// merges as merger does.
func NewDefaultMutationDataMerger(merger NamespaceDataMerger) MutationDataMerger {
	return defaultMutationDataMerger{merger: merger}
}

func (m defaultMutationDataMerger) MergeMutationData(
	_ namespace.ID,
	_ namespace.Name,
	currentData map[string]string,
	taskData map[string]string,
	currentConfigVersion int64,
	taskConfigVersion int64,
) (map[string]string, bool) {
	return m.merger.MergeData(currentData, taskData, currentConfigVersion, taskConfigVersion)
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
