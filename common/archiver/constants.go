// Copyright (c) 2017 Uber Technologies, Inc.
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

package archiver

import (
	"errors"

	"github.com/uber/cadence/.gen/go/shared"
)

const (
	// ArchiveNonRetriableErrorMsg is the log message when the Archive() method encounters a non-retriable error
	ArchiveNonRetriableErrorMsg = "Archive method encountered an non-retriable error."

	// ErrInvalidURI is the error reason for invalid URI
	ErrInvalidURI = "URI is invalid"
	// ErrInvalidArchiveRequest is the error reason for invalid archive request
	ErrInvalidArchiveRequest = "archive request is invalid"
	// ErrConstructHistoryIterator is the error reason for failing to construct history iterator
	ErrConstructHistoryIterator = "failed to construct history iterator"
	// ErrReadHistory is the error reason for failing to read history
	ErrReadHistory = "failed to read history batches"
	// ErrHistoryMutated is the error reason for mutated history
	ErrHistoryMutated = "history was mutated"
)

var (
	// ErrInvalidURIScheme is the error for invalid URI
	ErrInvalidURIScheme = errors.New("URI scheme is invalid")
	// ErrContextTimeout is the error for context timeout
	ErrContextTimeout = errors.New("archive aborted because context timed out")
	// ErrInvalidGetHistoryRequest is the error for invalid GetHistory request
	ErrInvalidGetHistoryRequest = &shared.BadRequestError{Message: "Get archived history request is invalid"}
	// ErrGetHistoryTokenCorrupted is the error for corrupted GetHistory token
	ErrGetHistoryTokenCorrupted = &shared.BadRequestError{Message: "Next page token is corrupted."}
	// ErrHistoryNotExist is the error for non-exist history
	ErrHistoryNotExist = &shared.BadRequestError{Message: "Requested workflow history does not exist."}
)
