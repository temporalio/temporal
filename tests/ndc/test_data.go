// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	"go.temporal.io/server/tests"
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
	versionHistory, err := tests.EventBatchesToVersionHistory(nil, []*historypb.History{
		{Events: eventsFlatted},
	})
	if err != nil {
		return nil, nil, err
	}
	return historyBatches, versionHistory, nil
}
