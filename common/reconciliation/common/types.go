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
	"time"
)

var (
	// BlobstoreSeparatorToken is used to separate entries written to blobstore
	BlobstoreSeparatorToken = []byte("\r\n")
	// BlobstoreTimeout is the timeout used for blobstore requests
	BlobstoreTimeout = time.Second * 10
)

type (
	// CheckResultType is the result type of running an invariant check
	CheckResultType string
	// FixResultType is the result type of running an invariant fix
	FixResultType string
	// InvariantType is the type of an invariant
	InvariantType string
	// InvariantCollection is a type which indicates a collection of invariants
	InvariantCollection int
	// Extension is the type which indicates the file extension type
	Extension string
)

const (
	// CheckResultTypeFailed indicates a failure occurred while attempting to run check
	CheckResultTypeFailed CheckResultType = "failed"
	// CheckResultTypeCorrupted indicates check successfully ran and detected a corruption
	CheckResultTypeCorrupted CheckResultType = "corrupted"
	// CheckResultTypeHealthy indicates check successfully ran and detected no corruption
	CheckResultTypeHealthy CheckResultType = "healthy"

	// FixResultTypeSkipped indicates that fix skipped execution
	FixResultTypeSkipped FixResultType = "skipped"
	// FixResultTypeFixed indicates that fix successfully fixed an execution
	FixResultTypeFixed FixResultType = "fixed"
	// FixResultTypeFailed indicates that fix attempted to fix an execution but failed to do so
	FixResultTypeFailed FixResultType = "failed"

	// HistoryExistsInvariantType asserts that history must exist if concrete execution exists
	HistoryExistsInvariantType InvariantType = "history_exists"
	// OpenCurrentExecutionInvariantType asserts that an open concrete execution must have a valid current execution
	OpenCurrentExecutionInvariantType InvariantType = "open_current_execution"

	// InvariantCollectionMutableState is the collection of invariants relating to mutable state
	InvariantCollectionMutableState InvariantCollection = 0
	// InvariantCollectionHistory is the collection  of invariants relating to history
	InvariantCollectionHistory InvariantCollection = 1

	// SkippedExtension is the extension for files which contain skips
	SkippedExtension Extension = "skipped"
	// FailedExtension is the extension for files which contain failures
	FailedExtension Extension = "failed"
	// FixedExtension is the extension for files which contain fixes
	FixedExtension Extension = "fixed"
	// CorruptedExtension is the extension for files which contain corruptions
	CorruptedExtension Extension = "corrupted"
)

// The following are types related to Invariant.
type (
	// Execution is a base type for executions which should be checked or fixed.
	Execution struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
		State      int
	}

	// ConcreteExecution is a concrete execution.
	ConcreteExecution struct {
		BranchToken []byte
		TreeID      string
		BranchID    string
		Execution
	}

	// CheckResult is the result of running Check.
	CheckResult struct {
		CheckResultType CheckResultType
		InvariantType   InvariantType
		Info            string
		InfoDetails     string
	}

	// FixResult is the result of running Fix.
	FixResult struct {
		FixResultType FixResultType
		InvariantType InvariantType
		CheckResult   CheckResult
		Info          string
		InfoDetails   string
	}

	// ManagerCheckResult is the result of running a list of checks
	ManagerCheckResult struct {
		CheckResultType          CheckResultType
		DeterminingInvariantType *InvariantType
		CheckResults             []CheckResult
	}

	// ManagerFixResult is the result of running a list of fixes
	ManagerFixResult struct {
		FixResultType            FixResultType
		DeterminingInvariantType *InvariantType
		FixResults               []FixResult
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
	// Keys can be nil if there were no uploads.
	ShardScanKeys struct {
		Corrupt *Keys
		Failed  *Keys
	}

	// ShardFixReport is the report of running Fix on a single shard
	ShardFixReport struct {
		ShardID int
		Stats   ShardFixStats
		Result  ShardFixResult
	}

	// ShardFixStats indicates the stats of executions that were handled by shard Fix.
	ShardFixStats struct {
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
	// Keys can be nil if there were no uploads.
	ShardFixKeys struct {
		Skipped *Keys
		Failed  *Keys
		Fixed   *Keys
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
		Extension Extension
	}
)

// The following are serializable types which get output by Scan and Fix to durable sinks.
type (
	// ScanOutputEntity represents a single execution that should be durably recorded by Scan.
	ScanOutputEntity struct {
		Execution interface{}
		Result    ManagerCheckResult
	}

	// FixOutputEntity represents a single execution that should be durably recorded by fix.
	// It contains the ScanOutputEntity that was given as input to fix.
	FixOutputEntity struct {
		Execution interface{}
		Input     ScanOutputEntity
		Result    ManagerFixResult
	}
)

type (
	// ScanType is the enum for representing different entity types to scan
	ScanType int
)

const (
	// ConcreteExecutionType concrete execution entity
	ConcreteExecutionType ScanType = iota
	// CurrentExecutionType current execution entity
	CurrentExecutionType
)
