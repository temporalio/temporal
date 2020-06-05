// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package executions

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

func validateShards(shards Shards) error {
	if shards.List == nil && shards.Range == nil {
		return errors.New("must provide either List or Range")
	}
	if shards.List != nil && shards.Range != nil {
		return errors.New("only one of List or Range can be provided")
	}
	if shards.List != nil && len(shards.List) == 0 {
		return errors.New("empty List provided")
	}
	if shards.Range != nil && shards.Range.Max <= shards.Range.Min {
		return errors.New("empty Range provided")
	}
	return nil
}

func flattenShards(shards Shards) ([]int, int, int) {
	shardList := shards.List
	if len(shardList) == 0 {
		shardList = []int{}
		for i := shards.Range.Min; i < shards.Range.Max; i++ {
			shardList = append(shardList, i)
		}
	}
	min := shardList[0]
	max := shardList[0]
	for i := 1; i < len(shardList); i++ {
		if shardList[i] < min {
			min = shardList[i]
		}
		if shardList[i] > max {
			max = shardList[i]
		}
	}
	return shardList, min, max
}

func resolveFixerConfig(overwrites FixerWorkflowConfigOverwrites) ResolvedFixerWorkflowConfig {
	resolvedConfig := ResolvedFixerWorkflowConfig{
		Concurrency:             25,
		BlobstoreFlushThreshold: 1000,
		ActivityBatchSize:       200,
		InvariantCollections: InvariantCollections{
			InvariantCollectionMutableState: true,
			InvariantCollectionHistory:      true,
		},
	}
	if overwrites.Concurrency != nil {
		resolvedConfig.Concurrency = *overwrites.Concurrency
	}
	if overwrites.BlobstoreFlushThreshold != nil {
		resolvedConfig.BlobstoreFlushThreshold = *overwrites.BlobstoreFlushThreshold
	}
	if overwrites.InvariantCollections != nil {
		resolvedConfig.InvariantCollections = *overwrites.InvariantCollections
	}
	if overwrites.ActivityBatchSize != nil {
		resolvedConfig.ActivityBatchSize = *overwrites.ActivityBatchSize
	}
	return resolvedConfig
}

func getShortActivityContext(ctx workflow.Context) workflow.Context {
	activityOptions := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       1.7,
			ExpirationInterval:       10 * time.Minute,
			NonRetriableErrorReasons: []string{ErrScanWorkflowNotClosed, ErrSerialization},
		},
	}
	return workflow.WithActivityOptions(ctx, activityOptions)
}

func getLongActivityContext(ctx workflow.Context) workflow.Context {
	activityOptions := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    8 * time.Hour,
		HeartbeatTimeout:       time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       1.7,
			ExpirationInterval:       8 * time.Hour,
			NonRetriableErrorReasons: []string{ErrScanWorkflowNotClosed, ErrSerialization},
		},
	}
	return workflow.WithActivityOptions(ctx, activityOptions)
}

func shardInBounds(minShardID, maxShardID, shardID int) error {
	if shardID > maxShardID || shardID < minShardID {
		return fmt.Errorf("requested shard %v is outside of bounds (min: %v and max: %v)", shardID, minShardID, maxShardID)
	}
	return nil
}

func getShardBatches(
	batchSize int,
	concurrency int,
	shards []int,
	workerIdx int,
) [][]int {
	batchIndices := getBatchIndices(batchSize, concurrency, len(shards), workerIdx)
	var result [][]int
	for _, batch := range batchIndices {
		var curr []int
		for _, i := range batch {
			curr = append(curr, shards[i])
		}
		result = append(result, curr)
	}
	return result
}

func getCorruptedKeysBatches(
	batchSize int,
	concurrency int,
	corruptedKeys []CorruptedKeysEntry,
	workerIdx int,
) [][]CorruptedKeysEntry {
	batchIndices := getBatchIndices(batchSize, concurrency, len(corruptedKeys), workerIdx)
	var result [][]CorruptedKeysEntry
	for _, batch := range batchIndices {
		var curr []CorruptedKeysEntry
		for _, i := range batch {
			curr = append(curr, corruptedKeys[i])
		}
		result = append(result, curr)
	}
	return result
}

func getBatchIndices(
	batchSize int,
	concurrency int,
	sliceLength int,
	workerIdx int,
) [][]int {
	var batches [][]int
	var currBatch []int
	for i := 0; i < sliceLength; i++ {
		if i%concurrency == workerIdx {
			currBatch = append(currBatch, i)
			if len(currBatch) == batchSize {
				batches = append(batches, currBatch)
				currBatch = nil
			}
		}
	}
	if len(currBatch) > 0 {
		batches = append(batches, currBatch)
	}
	return batches
}
