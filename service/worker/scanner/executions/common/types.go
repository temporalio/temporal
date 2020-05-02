// The MIT License (MIT)
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

package common

import (
	"github.com/uber/cadence/common/persistence"
)

type (
	// CheckResultType is the result type of running an invariant check
	CheckResultType string
	// FixResultType is the result type of running an invariant fix
	FixResultType string
	// InvariantType is the type of an invariant
	InvariantType string
)

const (
	// CheckResultTypeFailed indicates a failure occurred while attempting to run check
	CheckResultTypeFailed CheckResultType = "failed"
	// CheckResultTypeCorrupted indicates check successfully ran and detected a corruption
	CheckResultTypeCorrupted = "corrupted"
	// CheckResultTypeHealthy indicates check successfully ran and detected no corruption
	CheckResultTypeHealthy = "healthy"

	// FixResultTypeSkipped indicates that fix skipped execution
	FixResultTypeSkipped FixResultType = "skipped"
	// FixResultTypeFixed indicates that fix successfully fixed an execution
	FixResultTypeFixed = "fixed"
	// FixResultTypeFailed indicates that fix attempted to fix an execution but failed to do so
	FixResultTypeFailed = "failed"

	// HistoryExistsInvariantType asserts that history must exist if concrete execution exists
	HistoryExistsInvariantType InvariantType = "history_exists"
	// ValidFirstEventInvariantType asserts that the first event in a history must be of a specific form
	ValidFirstEventInvariantType = "valid_first_event"
	// PairedWithCurrentInvariantType asserts that an open concrete execution must have a valid current execution
	PairedWithCurrentInvariantType = "paired_with_current"
)

// The following are types related to Invariant.
type (
	// Execution is an execution which should be checked or fixed.
	Execution struct {
		ShardID     int
		DomainID    string
		WorkflowID  string
		RunID       string
		BranchToken []byte
		State       int
	}

	// InvariantResourceBag is a union of resources used to pass results from one Invariant to another Invariant.
	InvariantResourceBag struct {
		History *persistence.InternalReadHistoryBranchResponse
	}

	// CheckResult is the result of running Check.
	CheckResult struct {
		CheckResultType CheckResultType
		Info            string
		InfoDetails     string
	}

	// FixResult is the result of running Fix.
	FixResult struct {
		FixResultType FixResultType
		Info          string
		InfoDetails   string
	}
)

// The following are serializable types that represent the reports returns by Scan and Fix.
type (
	// ShardScanReport is the report of running Scan on a single shard.
	ShardScanReport struct {
		ShardID int
		Stats   ShardScanStats
		Result  ShardScanResult
	}

	// ShardScanStats indicates the stats of executions which were handled by shard Scan.
	ShardScanStats struct {
		ExecutionsCount             int64
		CorruptedCount              int64
		CheckFailedCount            int64
		CorruptionByType            map[InvariantType]int64
		CorruptedOpenExecutionCount int64
	}

	// ShardScanResult indicates the result of running scan on a shard.
	// Exactly one of ControlFlowFailure or ShardScanKeys will be non-nil
	ShardScanResult struct {
		ShardScanKeys      *ShardScanKeys
		ControlFlowFailure *ControlFlowFailure
	}

	// ShardScanKeys are the keys to the blobs that were uploaded during scan.
	ShardScanKeys struct {
		Corrupt Keys
		Failed  Keys
	}

	// ShardFixReport is the report of running Fix on a single shard
	ShardFixReport struct {
		ShardID int
		Handled ShardFixHandled
		Result  ShardFixResult
	}

	// ShardFixHandled indicates the executions which were handled by fix.
	ShardFixHandled struct {
		ExecutionCount int64
		FixedCount     int64
		SkippedCount   int64
		FailedCount    int64
	}

	// ShardFixResult indicates the result of running fix on a shard.
	// Exactly one of ControlFlowFailure or ShardFixKeys will be non-nil.
	ShardFixResult struct {
		ShardFixKeys       *ShardFixKeys
		ControlFlowFailure *ControlFlowFailure
	}

	// ShardFixKeys are the keys to the blobs that were uploaded during fix.
	ShardFixKeys struct {
		Skipped Keys
		Failed  Keys
		Fixed   Keys
	}

	// ControlFlowFailure indicates an error occurred which makes it impossible to
	// even attempt to check or fix one or more execution(s). Note that it is not a ControlFlowFailure
	// if a check or fix fails, it is only a ControlFlowFailure if
	// an error is encountered which makes even attempting to check or fix impossible.
	ControlFlowFailure struct {
		Info        string
		InfoDetails string
	}

	// Keys indicate the keys which were uploaded during a scan or fix.
	// Keys are constructed as uuid_page.extension. MinPage and MaxPage are
	// both inclusive and pages are sequential, meaning from this struct all pages can be deterministically constructed.
	Keys struct {
		UUID      string
		MinPage   int
		MaxPage   int
		Extension string
	}
)

// The following are serializable types which get output by Scan and Fix to durable sinks.
type (
	// ScanOutputEntity represents a single execution that should be durably recorded by Scan.
	ScanOutputEntity struct {
		Execution Execution
		Result    CheckResult
	}

	// FixOutputEntity represents a single execution that should be durably recorded by fix.
	// It contains the ScanOutputEntity that was given as input to fix.
	FixOutputEntity struct {
		ScanOutputEntity ScanOutputEntity
		Result           FixResult
	}
)
