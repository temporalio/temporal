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
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/reconciliation/store"
)

type (
	scanner struct {
		shardID          int
		itr              pagination.Iterator
		failedWriter     store.ExecutionWriter
		corruptedWriter  store.ExecutionWriter
		invariantManager invariant.Manager
		progressReportFn func()
	}
)

// NewScanner constructs a new scanner
func NewScanner(
	shardID int,
	pr persistence.Retryer,
	persistencePageSize int,
	blobstoreClient blobstore.Client,
	blobstoreFlushThreshold int,
	invariantCollections []invariant.Collection,
	progressReportFn func(),
	scanType ScanType,
) Scanner {
	id := uuid.New()

	var ivs []invariant.Invariant
	for _, fn := range scanType.ToInvariants(invariantCollections) {
		ivs = append(ivs, fn(pr))
	}

	iterator := scanType.ToIterator()

	return &scanner{
		shardID:          shardID,
		itr:              iterator(pr, persistencePageSize),
		failedWriter:     store.NewBlobstoreWriter(id, store.FailedExtension, blobstoreClient, blobstoreFlushThreshold),
		corruptedWriter:  store.NewBlobstoreWriter(id, store.CorruptedExtension, blobstoreClient, blobstoreFlushThreshold),
		invariantManager: invariant.NewInvariantManager(ivs),
		progressReportFn: progressReportFn,
	}
}

// Scan scans over all executions in shard and runs invariant checks per execution.
func (s *scanner) Scan() ScanReport {
	result := ScanReport{
		ShardID: s.shardID,
		Stats: ScanStats{
			CorruptionByType: make(map[invariant.Name]int64),
		},
	}

	for s.itr.HasNext() {
		s.progressReportFn()
		exec, err := s.itr.Next()
		if err != nil {
			result.Result.ControlFlowFailure = &ControlFlowFailure{
				Info:        "persistence iterator returned error",
				InfoDetails: err.Error(),
			}
			return result
		}
		checkResult := s.invariantManager.RunChecks(exec)
		result.Stats.ExecutionsCount++
		switch checkResult.CheckResultType {
		case invariant.CheckResultTypeHealthy:
			// do nothing if execution is healthy
		case invariant.CheckResultTypeCorrupted:
			if err := s.corruptedWriter.Add(store.ScanOutputEntity{
				Execution: exec,
				Result:    checkResult,
			}); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for corrupted execution check",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.CorruptedCount++
			result.Stats.CorruptionByType[*checkResult.DeterminingInvariantType]++
			if invariant.ExecutionOpen(exec) {
				result.Stats.CorruptedOpenExecutionCount++
			}
		case invariant.CheckResultTypeFailed:
			if err := s.failedWriter.Add(store.ScanOutputEntity{
				Execution: exec,
				Result:    checkResult,
			}); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
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
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for failed execution checks",
			InfoDetails: err.Error(),
		}
		return result
	}
	if err := s.corruptedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for corrupted execution checks",
			InfoDetails: err.Error(),
		}
		return result
	}

	result.Result.ShardScanKeys = &ScanKeys{
		Corrupt: s.corruptedWriter.FlushedKeys(),
		Failed:  s.failedWriter.FlushedKeys(),
	}
	return result
}
