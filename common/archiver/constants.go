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

package archiver

import (
	"errors"
)

const (
	// ArchiveNonRetryableErrorMsg is the log message when the Archive() method encounters a non-retryable error
	ArchiveNonRetryableErrorMsg = "Archive method encountered an non-retryable error."
	// ArchiveTransientErrorMsg is the log message when the Archive() method encounters a transient error
	ArchiveTransientErrorMsg = "Archive method encountered a transient error."
	// ArchiveSkippedInfoMsg is the log messsage when the Archive() method encounter an not found error
	ArchiveSkippedInfoMsg = "Archive method encountered not found error and skipped the archival"

	// ErrReasonInvalidURI is the error reason for invalid URI
	ErrReasonInvalidURI = "URI is invalid"
	// ErrReasonInvalidArchiveRequest is the error reason for invalid archive request
	ErrReasonInvalidArchiveRequest = "archive request is invalid"
	// ErrReasonConstructHistoryIterator is the error reason for failing to construct history iterator
	ErrReasonConstructHistoryIterator = "failed to construct history iterator"
	// ErrReasonReadHistory is the error reason for failing to read history
	ErrReasonReadHistory = "failed to read history batches"
	// ErrReasonHistoryMutated is the error reason for mutated history
	ErrReasonHistoryMutated = "history was mutated"
)

var (
	// ErrInvalidURI is the error for invalid URI
	ErrInvalidURI = errors.New("URI is invalid")
	// ErrURISchemeMismatch is the error for mismatch between URI scheme and archiver
	ErrURISchemeMismatch = errors.New("URI scheme does not match the archiver")
	// ErrHistoryMutated is the error for mutated history
	ErrHistoryMutated = errors.New("history was mutated")
	// ErrContextTimeout is the error for context timeout
	ErrContextTimeout = errors.New("archive aborted because context timed out")
	// ErrInvalidGetHistoryRequest is the error for invalid GetHistory request
	ErrInvalidGetHistoryRequest = errors.New("get archived history request is invalid")
	// ErrInvalidQueryVisibilityRequest is the error for invalid Query Visibility request
	ErrInvalidQueryVisibilityRequest = errors.New("query visiblity request is invalid")
	// ErrNextPageTokenCorrupted is the error for corrupted GetHistory token
	ErrNextPageTokenCorrupted = errors.New("next page token is corrupted")
	// ErrHistoryNotExist is the error for non-exist history
	ErrHistoryNotExist = errors.New("requested workflow history does not exist")
)
