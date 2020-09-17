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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mocks.go -self_package github.com/uber/cadence/common/reconciliation/store

package store

import (
	"time"

	"github.com/uber/cadence/common/reconciliation/invariant"
)

const (
	// SkippedExtension is the extension for files which contain skips
	SkippedExtension Extension = "skipped"
	// FailedExtension is the extension for files which contain failures
	FailedExtension Extension = "failed"
	// FixedExtension is the extension for files which contain fixes
	FixedExtension Extension = "fixed"
	// CorruptedExtension is the extension for files which contain corruptions
	CorruptedExtension Extension = "corrupted"
)

var (
	// SeparatorToken is used to separate entries written to blobstore
	SeparatorToken = []byte("\r\n")
	// Timeout is the timeout used for blobstore requests
	Timeout = time.Second * 10
)

type (
	// Extension is the type which indicates the file extension type
	Extension string

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
		Result    invariant.ManagerCheckResult
	}

	// FixOutputEntity represents a single execution that should be durably recorded by fix.
	// It contains the ScanOutputEntity that was given as input to fix.
	FixOutputEntity struct {
		Execution interface{}
		Input     ScanOutputEntity
		Result    invariant.ManagerFixResult
	}
)

type (
	// ScanOutputIterator gets ScanOutputEntities from underlying store
	ScanOutputIterator interface {
		// Next returns the next ScanOutputEntity found. Any error reading from underlying store
		// or converting store entry to ScanOutputEntity will result in an error after which iterator cannot be used.
		Next() (*ScanOutputEntity, error)
		// HasNext indicates if the iterator has a next element. If HasNext is true it is
		// guaranteed that Next will return a nil error and non-nil ScanOutputEntity.
		HasNext() bool
	}

	// ExecutionWriter is used to write entities (FixOutputEntity or ScanOutputEntity) to blobstore
	ExecutionWriter interface {
		Add(interface{}) error
		Flush() error
		FlushedKeys() *Keys
	}
)
