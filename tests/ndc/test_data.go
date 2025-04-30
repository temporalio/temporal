package ndc

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/tests/testcore"
)

var (
	clusterName              = []string{"cluster-a", "cluster-b", "cluster-c"}
	clusterReplicationConfig = []*replicationpb.ClusterReplicationConfig{
		{ClusterName: clusterName[0]},
		{ClusterName: clusterName[1]},
		{ClusterName: clusterName[2]},
	}
)

func GetEventBatchesFromTestEvents(fileName string, workflowId string) ([][]*historypb.HistoryEvent, *historyspb.VersionHistory, error) {
	filePath := "./testevents/" + fileName
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}
	var result map[string]interface{}

	// Unmarshal the JSON data into a map
	err = json.Unmarshal(content, &result)
	if err != nil {
		return nil, nil, err
	}
	batches, ok := result[workflowId].([]interface{})
	if !ok {
		return nil, nil, errors.New("workflowId not found in test data")
	}
	var elements [][]byte
	for i, v := range batches {
		subArray, ok := v.([]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("element %d is not a sub-array, is type %s", i, fmt.Sprintf("%T", v))
		}
		temp, err := json.Marshal(subArray)
		if err != nil {
			return nil, nil, err
		}
		elements = append(elements, temp)
	}
	historyBatches := make([][]*historypb.HistoryEvent, len(batches))
	encoder := codec.NewJSONPBEncoder()
	for i, batch := range elements {
		historyEvents, err := encoder.DecodeHistoryEvents(batch)
		if err != nil {
			return nil, nil, err
		}
		historyBatches[i] = historyEvents
	}
	var eventsFlatted []*historypb.HistoryEvent
	for _, batch := range historyBatches {
		eventsFlatted = append(eventsFlatted, batch...)
	}
	versionHistory, err := testcore.EventBatchesToVersionHistory(nil, []*historypb.History{
		{Events: eventsFlatted},
	})
	if err != nil {
		return nil, nil, err
	}
	return historyBatches, versionHistory, nil
}
