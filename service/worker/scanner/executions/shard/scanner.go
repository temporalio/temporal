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

package shard

import (
	"fmt"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/reconciliation/common"
	"github.com/uber/cadence/common/reconciliation/invariants"
)

type (
	scanner struct {
		shardID          int
		itr              common.ExecutionIterator
		failedWriter     common.ExecutionWriter
		corruptedWriter  common.ExecutionWriter
		invariantManager common.InvariantManager
		progressReportFn func()
	}
)

// NewScanner constructs a new scanner
func NewScanner(
	shardID int,
	pr common.PersistenceRetryer,
	persistencePageSize int,
	blobstoreClient blobstore.Client,
	blobstoreFlushThreshold int,
	invariantCollections []common.InvariantCollection,
	progressReportFn func(),
	scanType common.ScanType,
) common.Scanner {
	id := uuid.New()
	return &scanner{
		shardID:          shardID,
		itr:              common.NewPersistenceIterator(pr, persistencePageSize, shardID, scanType),
		failedWriter:     common.NewBlobstoreWriter(id, common.FailedExtension, blobstoreClient, blobstoreFlushThreshold),
		corruptedWriter:  common.NewBlobstoreWriter(id, common.CorruptedExtension, blobstoreClient, blobstoreFlushThreshold),
		invariantManager: invariants.NewInvariantManager(invariantCollections, pr, scanType),
		progressReportFn: progressReportFn,
	}
}

// Scan scans over all executions in shard and runs invariant checks per execution.
func (s *scanner) Scan() common.ShardScanReport {
	result := common.ShardScanReport{
		ShardID: s.shardID,
		Stats: common.ShardScanStats{
			CorruptionByType: make(map[common.InvariantType]int64),
		},
	}

	for s.itr.HasNext() {
		s.progressReportFn()
		exec, err := s.itr.Next()
		if err != nil {
			result.Result.ControlFlowFailure = &common.ControlFlowFailure{
				Info:        "persistence iterator returned error",
				InfoDetails: err.Error(),
			}
			return result
		}
		checkResult := s.invariantManager.RunChecks(exec)
		result.Stats.ExecutionsCount++
		switch checkResult.CheckResultType {
		case common.CheckResultTypeHealthy:
			// do nothing if execution is healthy
		case common.CheckResultTypeCorrupted:
			if err := s.corruptedWriter.Add(common.ScanOutputEntity{
				Execution: exec,
				Result:    checkResult,
			}); err != nil {
				result.Result.ControlFlowFailure = &common.ControlFlowFailure{
					Info:        "blobstore add failed for corrupted execution check",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.CorruptedCount++
			result.Stats.CorruptionByType[*checkResult.DeterminingInvariantType]++
			if common.ExecutionOpen(exec) {
				result.Stats.CorruptedOpenExecutionCount++
			}
		case common.CheckResultTypeFailed:
			if err := s.failedWriter.Add(common.ScanOutputEntity{
				Execution: exec,
				Result:    checkResult,
			}); err != nil {
				result.Result.ControlFlowFailure = &common.ControlFlowFailure{
					Info:        "blobstore add failed for failed execution check",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.CheckFailedCount++
		default:
			panic(fmt.Sprintf("unknown CheckResultType: %v", checkResult.CheckResultType))
		}
	}

	if err := s.failedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &common.ControlFlowFailure{
			Info:        "failed to flush for failed execution checks",
			InfoDetails: err.Error(),
		}
		return result
	}
	if err := s.corruptedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &common.ControlFlowFailure{
			Info:        "failed to flush for corrupted execution checks",
			InfoDetails: err.Error(),
		}
		return result
	}

	result.Result.ShardScanKeys = &common.ShardScanKeys{
		Corrupt: s.corruptedWriter.FlushedKeys(),
		Failed:  s.failedWriter.FlushedKeys(),
	}
	return result
}
