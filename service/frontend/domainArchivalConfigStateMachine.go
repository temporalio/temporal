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

package frontend

import (
	"context"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/blobstore"
)

// domainArchivalConfigStateMachine is only used by workflowHandler.
// It is simply meant to simplify the logic around archival domain state changes.
// Logically this class can be thought of as part of workflowHandler.

type (
	// archivalState represents the state of archival config
	// the only invalid state is {bucket="", status=enabled}
	// once bucket is set it is immutable
	// the initial state is {bucket="", status=disabled}, this is what all domains initially default to
	archivalState struct {
		bucket string
		status shared.ArchivalStatus
	}

	// archivalEvent represents a change request to archival config state
	// the only restriction placed on events is that defaultBucket is not empty
	// setting requestedBucket to empty string means user is not attempting to set bucket
	// status can be nil, enabled, or disabled (nil indicates no update by user is being attempted)
	archivalEvent struct {
		defaultBucket string
		bucket        string
		status        *shared.ArchivalStatus
	}
)

// the following errors represents impossible code states that should never occur
var (
	errInvalidState            = &shared.BadRequestError{Message: "Encountered illegal state: archival is enabled but bucket is not set (should be impossible)"}
	errInvalidEvent            = &shared.BadRequestError{Message: "Encountered illegal event: default bucket is not set (should be impossible)"}
	errCannotHandleStateChange = &shared.BadRequestError{Message: "Encountered current state and event that cannot be handled (should be impossible)"}
)

// the following errors represents bad user input
var (
	errDisallowedBucketMetadata = &shared.BadRequestError{Message: "Cannot set bucket owner or bucket retention (must update bucket manually)"}
	errBucketNameUpdate         = &shared.BadRequestError{Message: "Cannot update existing bucket name"}
	errBucketDoesNotExist       = &shared.BadRequestError{Message: "Bucket does not exist"}
)

func neverEnabledState() *archivalState {
	return &archivalState{
		bucket: "",
		status: shared.ArchivalStatusDisabled,
	}
}

func (wh *WorkflowHandler) toArchivalRegisterEvent(request *shared.RegisterDomainRequest, defaultBucket string) (*archivalEvent, error) {
	event := &archivalEvent{
		defaultBucket: defaultBucket,
		bucket:        request.GetArchivalBucketName(),
		status:        request.ArchivalStatus,
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (wh *WorkflowHandler) toArchivalUpdateEvent(request *shared.UpdateDomainRequest, defaultBucket string) (*archivalEvent, error) {
	event := &archivalEvent{
		defaultBucket: defaultBucket,
	}
	if request.Configuration != nil {
		cfg := request.GetConfiguration()
		if cfg.ArchivalBucketOwner != nil || cfg.ArchivalRetentionPeriodInDays != nil {
			return nil, errDisallowedBucketMetadata
		}
		event.bucket = cfg.GetArchivalBucketName()
		event.status = cfg.ArchivalStatus
	}
	if err := event.validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *archivalEvent) validate() error {
	if len(e.defaultBucket) == 0 {
		return errInvalidEvent
	}
	return nil
}

func (s *archivalState) validate() error {
	if s.status == shared.ArchivalStatusEnabled && len(s.bucket) == 0 {
		return errInvalidState
	}
	return nil
}

func (s *archivalState) getNextState(ctx context.Context, blobstoreClient blobstore.Client, e *archivalEvent) (nextState *archivalState, changed bool, err error) {
	defer func() {
		// ensure that any existing bucket name was not mutated
		if nextState != nil && len(s.bucket) != 0 && s.bucket != nextState.bucket {
			nextState = nil
			changed = false
			err = errCannotHandleStateChange
		}

		// ensure that next state is valid
		if nextState != nil {
			if nextStateErr := nextState.validate(); nextStateErr != nil {
				nextState = nil
				changed = false
				err = nextStateErr
			}
		}

		// ensure the bucket exists
		if nextState != nil && nextState.bucket != "" {
			exists, bucketExistsErr := blobstoreClient.BucketExists(ctx, nextState.bucket)
			if bucketExistsErr != nil {
				nextState = nil
				changed = false
				err = bucketExistsErr
			} else if !exists {
				nextState = nil
				changed = false
				err = errBucketDoesNotExist
			}
		}
	}()

	if s == nil || e == nil {
		return nil, false, errCannotHandleStateChange
	}
	if err := s.validate(); err != nil {
		return nil, false, err
	}
	if err := e.validate(); err != nil {
		return nil, false, err
	}

	/**
	At this point state and event are both non-nil and valid.

	State can be any one of the following:
	{status=enabled,  bucket="foo"}
	{status=disabled, bucket="foo"}
	{status=disabled, bucket=""}

	Event can be any one of the following:
	{status=enabled,  bucket="foo", defaultBucket="bar"}
	{status=enabled,  bucket="",    defaultBucket="bar"}
	{status=disabled, bucket="foo", defaultBucket="bar"}
	{status=disabled, bucket="",    defaultBucket="bar"}
	{status=nil,      bucket="foo", defaultBucket="bar"}
	{status=nil,      bucket="",    defaultBucket="bar"}
	*/

	stateBucketSet := len(s.bucket) != 0
	eventBucketSet := len(e.bucket) != 0

	// factor this case out to ensure that bucket name is immutable
	if stateBucketSet && eventBucketSet && s.bucket != e.bucket {
		return nil, false, errBucketNameUpdate
	}

	// state 1
	if s.status == shared.ArchivalStatusEnabled && stateBucketSet {
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && eventBucketSet {
			return s, false, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && !eventBucketSet {
			return s, false, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && eventBucketSet {
			return &archivalState{
				bucket: s.bucket,
				status: shared.ArchivalStatusDisabled,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && !eventBucketSet {
			return &archivalState{
				bucket: s.bucket,
				status: shared.ArchivalStatusDisabled,
			}, true, nil
		}
		if e.status == nil && eventBucketSet {
			return s, false, nil
		}
		if e.status == nil && !eventBucketSet {
			return s, false, nil
		}
	}

	// state 2
	if s.status == shared.ArchivalStatusDisabled && stateBucketSet {
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && eventBucketSet {
			return &archivalState{
				status: shared.ArchivalStatusEnabled,
				bucket: s.bucket,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && !eventBucketSet {
			return &archivalState{
				status: shared.ArchivalStatusEnabled,
				bucket: s.bucket,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && eventBucketSet {
			return s, false, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && !eventBucketSet {
			return s, false, nil
		}
		if e.status == nil && eventBucketSet {
			return s, false, nil
		}
		if e.status == nil && !eventBucketSet {
			return s, false, nil
		}
	}

	// state 3
	if s.status == shared.ArchivalStatusDisabled && !stateBucketSet {
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && eventBucketSet {
			return &archivalState{
				status: shared.ArchivalStatusEnabled,
				bucket: e.bucket,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && !eventBucketSet {
			return &archivalState{
				status: shared.ArchivalStatusEnabled,
				bucket: e.defaultBucket,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && eventBucketSet {
			return &archivalState{
				status: shared.ArchivalStatusDisabled,
				bucket: e.bucket,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && !eventBucketSet {
			return s, false, nil
		}
		if e.status == nil && eventBucketSet {
			return &archivalState{
				status: shared.ArchivalStatusDisabled,
				bucket: e.bucket,
			}, true, nil
		}
		if e.status == nil && !eventBucketSet {
			return s, false, nil
		}
	}
	return nil, false, errCannotHandleStateChange
}
