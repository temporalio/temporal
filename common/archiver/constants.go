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
	// ErrInvalidGetHistoryRequest is the error for invalid GetHistory request
	ErrInvalidGetHistoryRequest = errors.New("get archived history request is invalid")
	// ErrInvalidQueryVisibilityRequest is the error for invalid Query Visibility request
	ErrInvalidQueryVisibilityRequest = errors.New("query visiblity request is invalid")
	// ErrNextPageTokenCorrupted is the error for corrupted GetHistory token
	ErrNextPageTokenCorrupted = errors.New("next page token is corrupted")
	// ErrHistoryNotExist is the error for non-exist history
	ErrHistoryNotExist = errors.New("requested workflow history does not exist")
)
