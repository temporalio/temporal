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
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/reconciliation/store"
)

type (
	fixer struct {
		shardID          int
		itr              store.ScanOutputIterator
		skippedWriter    store.ExecutionWriter
		failedWriter     store.ExecutionWriter
		fixedWriter      store.ExecutionWriter
		invariantManager invariant.Manager
		progressReportFn func()
	}
)

// NewFixer constructs a new fixer
func NewFixer(
	shardID int,
	pr persistence.Retryer,
	blobstoreClient blobstore.Client,
	keys store.Keys,
	blobstoreFlushThreshold int,
	invariantCollections []invariant.Collection,
	progressReportFn func(),
	scanType ScanType,
) Fixer {
	id := uuid.New()

	var ivs []invariant.Invariant
	for _, fn := range scanType.ToInvariants(invariantCollections) {
		ivs = append(ivs, fn(pr))
	}

	return &fixer{
		shardID:          shardID,
		itr:              store.NewBlobstoreIterator(blobstoreClient, keys, scanType.ToBlobstoreEntity()),
		skippedWriter:    store.NewBlobstoreWriter(id, store.SkippedExtension, blobstoreClient, blobstoreFlushThreshold),
		failedWriter:     store.NewBlobstoreWriter(id, store.FailedExtension, blobstoreClient, blobstoreFlushThreshold),
		fixedWriter:      store.NewBlobstoreWriter(id, store.FixedExtension, blobstoreClient, blobstoreFlushThreshold),
		invariantManager: invariant.NewInvariantManager(ivs),
		progressReportFn: progressReportFn,
	}
}

// Fix scans over all executions in shard and runs invariant fixes per execution.
func (f *fixer) Fix() FixReport {
	result := FixReport{
		ShardID: f.shardID,
	}
	for f.itr.HasNext() {
		f.progressReportFn()
		soe, err := f.itr.Next()
		if err != nil {
			result.Result.ControlFlowFailure = &ControlFlowFailure{
				Info:        "blobstore iterator returned error",
				InfoDetails: err.Error(),
			}
			return result
		}
		fixResult := f.invariantManager.RunFixes(soe.Execution)
		result.Stats.ExecutionCount++
		foe := store.FixOutputEntity{
			Execution: soe.Execution,
			Input:     *soe,
			Result:    fixResult,
		}
		switch fixResult.FixResultType {
		case invariant.FixResultTypeFixed:
			if err := f.fixedWriter.Add(foe); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for fixed execution fix",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.FixedCount++
		case invariant.FixResultTypeSkipped:
			if err := f.skippedWriter.Add(foe); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for skipped execution fix",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.SkippedCount++
		case invariant.FixResultTypeFailed:
			if err := f.failedWriter.Add(foe); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for failed execution fix",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.FailedCount++
		default:
			panic(fmt.Sprintf("unknown FixResultType: %v", fixResult.FixResultType))
		}
	}
	if err := f.fixedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for fixed execution fixes",
			InfoDetails: err.Error(),
		}
		return result
	}
	if err := f.skippedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for skipped execution fixes",
			InfoDetails: err.Error(),
		}
		return result
	}
	if err := f.failedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for failed execution fixes",
			InfoDetails: err.Error(),
		}
		return result
	}
	result.Result.ShardFixKeys = &FixKeys{
		Fixed:   f.fixedWriter.FlushedKeys(),
		Failed:  f.failedWriter.FlushedKeys(),
		Skipped: f.skippedWriter.FlushedKeys(),
	}
	return result
}
