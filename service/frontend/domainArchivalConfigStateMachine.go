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
	"fmt"
	"github.com/uber/cadence/.gen/go/shared"
)

type (
	archivalConfigState struct {
		bucket string
		status shared.ArchivalStatus
	}

	archivalConfigUpdate struct {
		defaultBucket   string
		requestedBucket *string
		requestedStatus *shared.ArchivalStatus
	}
)

var (
	errCannotProvideBucket       = &shared.BadRequestError{Message: "Request can only provide bucket name if request also gives status of enabled or disabled."}
	errDisallowedBucketMetadata  = &shared.BadRequestError{Message: "Cannot set bucket owner or bucket retention (must update bucket manually)."}
	errStateChangeToNeverEnabled = &shared.BadRequestError{Message: "Cannot change state to never enabled."}
	errBucketNameUpdate          = &shared.BadRequestError{Message: "Cannot update bucket name after archival status has been moved out of never_enabled for the first time."}
	errCannotProvideEmptyBucket  = &shared.BadRequestError{Message: "Cannot provide empty bucket name."}
	errNoDefaultBucket           = &shared.BadRequestError{Message: "No default bucket provided, is cluster correctly configured for archival?"}
	errCannotHandleStateChange   = &shared.BadRequestError{Message: "Encountered current state and update that cannot be handled (this should be impossible)"}
)

func neverEnabledState() *archivalConfigState {
	return &archivalConfigState{
		bucket: "",
		status: shared.ArchivalStatusNeverEnabled,
	}
}

func registerRequestToArchivalConfig(request *shared.RegisterDomainRequest, defaultBucket string) (*archivalConfigUpdate, error) {
	requestArchivalConfig := &archivalConfigUpdate{
		defaultBucket:   defaultBucket,
		requestedBucket: request.ArchivalBucketName,
		requestedStatus: request.ArchivalStatus,
	}
	if err := requestArchivalConfig.validate(); err != nil {
		return nil, err
	}
	return requestArchivalConfig, nil
}

func updateRequestToArchivalConfig(request *shared.UpdateDomainRequest, defaultBucket string) (*archivalConfigUpdate, error) {
	requestArchivalConfig := &archivalConfigUpdate{
		defaultBucket: defaultBucket,
	}
	if request.Configuration != nil {
		cfg := request.GetConfiguration()
		if cfg.ArchivalBucketOwner != nil || cfg.ArchivalRetentionPeriodInDays != nil {
			return nil, errDisallowedBucketMetadata
		}
		requestArchivalConfig.requestedBucket = cfg.ArchivalBucketName
		requestArchivalConfig.requestedStatus = cfg.ArchivalStatus
	}
	if err := requestArchivalConfig.validate(); err != nil {
		return nil, err
	}
	return requestArchivalConfig, nil
}

func (u *archivalConfigUpdate) validate() error {
	if len(u.defaultBucket) == 0 {
		return errNoDefaultBucket
	}
	if u.requestedBucket != nil && len(*u.requestedBucket) == 0 {
		return errCannotProvideEmptyBucket
	}
	if (u.requestedStatus == nil || *u.requestedStatus == shared.ArchivalStatusNeverEnabled) && u.requestedBucket != nil {
		return errCannotProvideBucket
	}
	return nil
}

func (s *archivalConfigState) validate() error {
	neverEnabledValid := s.status == shared.ArchivalStatusNeverEnabled && len(s.bucket) == 0
	disabledValid := s.status == shared.ArchivalStatusDisabled && len(s.bucket) != 0
	enabledValid := s.status == shared.ArchivalStatusEnabled && len(s.bucket) != 0
	if !neverEnabledValid && !disabledValid && !enabledValid {
		return &shared.BadRequestError{Message: fmt.Sprintf("Encountered invalid current state (this is bad it means database is in invalid state): %+v", *s)}
	}
	return nil
}

func (s *archivalConfigState) updateState(update *archivalConfigUpdate) (nextState *archivalConfigState, changed bool, err error) {
	defer func() {
		if nextState != nil {
			if nextStateErr := nextState.validate(); nextStateErr != nil {
				nextState = nil
				changed = false
				err = errCannotHandleStateChange
			}
		}
	}()

	if s == nil || update == nil {
		return nil, false, errCannotHandleStateChange
	}
	if err := s.validate(); err != nil {
		return nil, false, err
	}
	if err := update.validate(); err != nil {
		return nil, false, err
	}

	/**

	At this point the following invariants are met:
	- state and update are both not nil
	- update has a valid default bucket name
	- state is one of the following
		- {status:never_enabled, bucket:""}
		- {status:disabled, bucket:"some_bucket"}
		- {status:enabled, bucket:"some_bucket"}
	- update is one of the following
		- {requestStatus:nil, requestBucket:nil, defaultBucket:"some_bucket"}
		- {requestStatus:never_enabled, requestBucket:nil, defaultBucket:"some_bucket"}
		- {requestStatus:disabled, requestBucket:nil, defaultBucket:"some_bucket"}
		- {requestStatus:disabled, requestBucket:"some_bucket", defaultBucket:"some_bucket"}
		- {requestStatus:enabled, requestBucket:nil, defaultBucket:"some_bucket"}
		- {requestStatus:enabled, requestBucket:"some_bucket", defaultBucket:"some_bucket"}

	*/

	switch s.status {
	case shared.ArchivalStatusNeverEnabled:
		if update.requestedStatus == nil {
			return s, false, nil
		} else if *update.requestedStatus == shared.ArchivalStatusNeverEnabled {
			return s, false, nil
		} else if *update.requestedStatus == shared.ArchivalStatusDisabled {
			if update.requestedBucket == nil {
				return &archivalConfigState{
					bucket: update.defaultBucket,
					status: shared.ArchivalStatusDisabled,
				}, true, nil
			}
			return &archivalConfigState{
				bucket: *update.requestedBucket,
				status: shared.ArchivalStatusDisabled,
			}, true, nil
		} else if *update.requestedStatus == shared.ArchivalStatusEnabled {
			if update.requestedBucket == nil {
				return &archivalConfigState{
					bucket: update.defaultBucket,
					status: shared.ArchivalStatusEnabled,
				}, true, nil
			}
			return &archivalConfigState{
				bucket: *update.requestedBucket,
				status: shared.ArchivalStatusEnabled,
			}, true, nil
		}
	case shared.ArchivalStatusDisabled:
		if update.requestedStatus == nil {
			return s, false, nil
		} else if *update.requestedStatus == shared.ArchivalStatusNeverEnabled {
			return nil, false, errStateChangeToNeverEnabled
		} else if *update.requestedStatus == shared.ArchivalStatusDisabled {
			if update.requestedBucket == nil {
				return s, false, nil
			}
			if *update.requestedBucket != s.bucket {
				return nil, false, errBucketNameUpdate
			}
			return s, false, nil
		} else if *update.requestedStatus == shared.ArchivalStatusEnabled {
			if update.requestedBucket == nil {
				return &archivalConfigState{
					bucket: s.bucket,
					status: shared.ArchivalStatusEnabled,
				}, true, nil
			}
			if *update.requestedBucket != s.bucket {
				return nil, false, errBucketNameUpdate
			}
			return &archivalConfigState{
				bucket: s.bucket,
				status: shared.ArchivalStatusEnabled,
			}, true, nil
		}
	case shared.ArchivalStatusEnabled:
		if update.requestedStatus == nil {
			return s, false, nil
		} else if *update.requestedStatus == shared.ArchivalStatusNeverEnabled {
			return nil, false, errStateChangeToNeverEnabled
		} else if *update.requestedStatus == shared.ArchivalStatusDisabled {
			if update.requestedBucket == nil {
				return &archivalConfigState{
					bucket: s.bucket,
					status: shared.ArchivalStatusDisabled,
				}, true, nil
			}
			if *update.requestedBucket != s.bucket {
				return nil, false, errBucketNameUpdate
			}
			return &archivalConfigState{
				bucket: s.bucket,
				status: shared.ArchivalStatusDisabled,
			}, true, nil
		} else if *update.requestedStatus == shared.ArchivalStatusEnabled {
			if update.requestedBucket == nil {
				return s, false, nil
			}
			if *update.requestedBucket != s.bucket {
				return nil, false, errBucketNameUpdate
			}
			return s, false, nil
		}
	}
	return nil, false, errCannotHandleStateChange
}
