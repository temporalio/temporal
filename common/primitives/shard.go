package primitives

import "github.com/dgryski/go-farm"

// WorkflowIDToHistoryShard is used to map namespaceID-workflowID pair to a shardID.
// TODO: rename to BusinessIDToHistoryShard.
func WorkflowIDToHistoryShard(
	namespaceID string,
	workflowID string,
	numberOfShards int32,
) int32 {
	idBytes := []byte(namespaceID + "_" + workflowID)
	hash := farm.Fingerprint32(idBytes)
	return int32(hash%uint32(numberOfShards)) + 1 // ShardID starts with 1
}
