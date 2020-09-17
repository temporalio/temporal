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
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/reconciliation/iterator"
	"github.com/uber/cadence/common/reconciliation/store"
)

const (
	// ConcreteExecutionType concrete execution entity
	ConcreteExecutionType ScanType = iota
	// CurrentExecutionType current execution entity
	CurrentExecutionType
)

type (
	// ScanType is the enum for representing different entity types to scan
	ScanType int
)

// The following are serializable types that represent the reports returns by Scan and Fix.
type (
	// ScanReport is the report of running Scan on a single shard.
	ScanReport struct {
		ShardID int
		Stats   ScanStats
		Result  ScanResult
	}

	// ScanStats indicates the stats of executions which were handled by shard Scan.
	ScanStats struct {
		ExecutionsCount             int64
		CorruptedCount              int64
		CheckFailedCount            int64
		CorruptionByType            map[invariant.Name]int64
		CorruptedOpenExecutionCount int64
	}

	// ScanResult indicates the result of running scan on a shard.
	// Exactly one of ControlFlowFailure or ScanKeys will be non-nil
	ScanResult struct {
		ShardScanKeys      *ScanKeys
		ControlFlowFailure *ControlFlowFailure
	}

	// ScanKeys are the keys to the blobs that were uploaded during scan.
	// Keys can be nil if there were no uploads.
	ScanKeys struct {
		Corrupt *store.Keys
		Failed  *store.Keys
	}

	// FixReport is the report of running Fix on a single shard
	FixReport struct {
		ShardID int
		Stats   FixStats
		Result  FixResult
	}

	// FixStats indicates the stats of executions that were handled by shard Fix.
	FixStats struct {
		ExecutionCount int64
		FixedCount     int64
		SkippedCount   int64
		FailedCount    int64
	}

	// FixResult indicates the result of running fix on a shard.
	// Exactly one of ControlFlowFailure or FixKeys will be non-nil.
	FixResult struct {
		ShardFixKeys       *FixKeys
		ControlFlowFailure *ControlFlowFailure
	}

	// FixKeys are the keys to the blobs that were uploaded during fix.
	// Keys can be nil if there were no uploads.
	FixKeys struct {
		Skipped *store.Keys
		Failed  *store.Keys
		Fixed   *store.Keys
	}

	// ControlFlowFailure indicates an error occurred which makes it impossible to
	// even attempt to check or fix one or more execution(s). Note that it is not a ControlFlowFailure
	// if a check or fix fails, it is only a ControlFlowFailure if
	// an error is encountered which makes even attempting to check or fix impossible.
	ControlFlowFailure struct {
		Info        string
		InfoDetails string
	}
)

// Scanner is used to scan over all executions in a shard. It is responsible for three things:
// 1. Checking invariants for each execution.
// 2. Recording corruption and failures to durable store.
// 3. Producing a ScanReport
type Scanner interface {
	Scan() ScanReport
}

// Fixer is used to fix all executions in a shard. It is responsible for three things:
// 1. Confirming that each execution it scans is corrupted.
// 2. Attempting to fix any confirmed corrupted executions.
// 3. Recording skipped executions, failed to fix executions and successfully fix executions to durable store.
// 4. Producing a FixReport
type Fixer interface {
	Fix() FixReport
}

// ToBlobstoreEntity picks struct depending on scanner type
func (st ScanType) ToBlobstoreEntity() entity.Entity {
	switch st {
	case ConcreteExecutionType:
		return &entity.ConcreteExecution{}
	case CurrentExecutionType:
		return &entity.CurrentExecution{}
	}
	panic("unknown scan type")
}

// ToIterator selects appropriate iterator. It will panic if scan type is unknown
func (st ScanType) ToIterator() func(retryer persistence.Retryer, pageSize int) pagination.Iterator {
	switch st {
	case ConcreteExecutionType:
		return iterator.ConcreteExecution
	case CurrentExecutionType:
		return iterator.CurrentExecution
	default:
		panic("unknown scan type")
	}
}

// ToInvariants returns list of invariants to be checked
func (st ScanType) ToInvariants(collections []invariant.Collection) []func(retryer persistence.Retryer) invariant.Invariant {
	var fns []func(retryer persistence.Retryer) invariant.Invariant
	switch st {
	case ConcreteExecutionType:
		for _, collection := range collections {
			switch collection {
			case invariant.CollectionHistory:
				fns = append(fns, invariant.NewHistoryExists)
			case invariant.CollectionMutableState:
				fns = append(fns, invariant.NewOpenCurrentExecution)
			}
		}
		return fns
	case CurrentExecutionType:
		for _, collection := range collections {
			switch collection {
			case invariant.CollectionMutableState:
				fns = append(fns, invariant.NewConcreteExecutionExists)
			}
		}
		return fns
	default:
		panic("unknown scan type")
	}
}
