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

package domain

import (
	"github.com/uber/cadence/.gen/go/shared"
)

// domainArchivalConfigStateMachine is only used by domainHandler.
// It is simply meant to simplify the logic around archival domain state changes.
// Logically this class can be thought of as part of domainHandler.

type (
	// ArchivalState represents the state of archival config
	// the only invalid state is {URI="", status=enabled}
	// once URI is set it is immutable
	ArchivalState struct {
		Status shared.ArchivalStatus
		URI    string
	}

	// ArchivalEvent represents a change request to archival config state
	// the only restriction placed on events is that defaultURI is not empty
	// status can be nil, enabled, or disabled (nil indicates no update by user is being attempted)
	ArchivalEvent struct {
		defaultURI string
		URI        string
		status     *shared.ArchivalStatus
	}
)

// the following errors represents impossible code states that should never occur
var (
	errInvalidState            = &shared.BadRequestError{Message: "Encountered illegal state: archival is enabled but URI is not set (should be impossible)"}
	errInvalidEvent            = &shared.BadRequestError{Message: "Encountered illegal event: default URI is not set (should be impossible)"}
	errCannotHandleStateChange = &shared.BadRequestError{Message: "Encountered current state and event that cannot be handled (should be impossible)"}
	errURIUpdate               = &shared.BadRequestError{Message: "Cannot update existing archival URI"}
)

func neverEnabledState() *ArchivalState {
	return &ArchivalState{
		URI:    "",
		Status: shared.ArchivalStatusDisabled,
	}
}

func (e *ArchivalEvent) validate() error {
	if len(e.defaultURI) == 0 {
		return errInvalidEvent
	}
	return nil
}

func (s *ArchivalState) validate() error {
	if s.Status == shared.ArchivalStatusEnabled && len(s.URI) == 0 {
		return errInvalidState
	}
	return nil
}

func (s *ArchivalState) getNextState(
	e *ArchivalEvent,
	URIValidationFunc func(URI string) error,
) (nextState *ArchivalState, changed bool, err error) {
	defer func() {
		// ensure that any existing URI name was not mutated
		if nextState != nil && len(s.URI) != 0 && s.URI != nextState.URI {
			nextState = nil
			changed = false
			err = errCannotHandleStateChange
			return
		}

		// ensure that next state is valid
		if nextState != nil {
			if nextStateErr := nextState.validate(); nextStateErr != nil {
				nextState = nil
				changed = false
				err = nextStateErr
				return
			}
		}

		if nextState != nil && nextState.URI != "" {
			if validateURIErr := URIValidationFunc(nextState.URI); validateURIErr != nil {
				nextState = nil
				changed = false
				err = validateURIErr
				return
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
	{status=enabled,  URI="foo"}
	{status=disabled, URI="foo"}
	{status=disabled, URI=""}

	Event can be any one of the following:
	{status=enabled,  URI="foo", defaultURI="bar"}
	{status=enabled,  URI="",    defaultURI="bar"}
	{status=disabled, URI="foo", defaultURI="bar"}
	{status=disabled, URI="",    defaultURI="bar"}
	{status=nil,      URI="foo", defaultURI="bar"}
	{status=nil,      URI="",    defaultURI="bar"}
	*/

	stateURISet := len(s.URI) != 0
	eventURISet := len(e.URI) != 0

	// factor this case out to ensure that URI is immutable
	if stateURISet && eventURISet && s.URI != e.URI {
		return nil, false, errURIUpdate
	}

	// state 1
	if s.Status == shared.ArchivalStatusEnabled && stateURISet {
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && eventURISet {
			return s, false, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && !eventURISet {
			return s, false, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && eventURISet {
			return &ArchivalState{
				Status: shared.ArchivalStatusDisabled,
				URI:    s.URI,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && !eventURISet {
			return &ArchivalState{
				Status: shared.ArchivalStatusDisabled,
				URI:    s.URI,
			}, true, nil
		}
		if e.status == nil && eventURISet {
			return s, false, nil
		}
		if e.status == nil && !eventURISet {
			return s, false, nil
		}
	}

	// state 2
	if s.Status == shared.ArchivalStatusDisabled && stateURISet {
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && eventURISet {
			return &ArchivalState{
				URI:    s.URI,
				Status: shared.ArchivalStatusEnabled,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && !eventURISet {
			return &ArchivalState{
				Status: shared.ArchivalStatusEnabled,
				URI:    s.URI,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && eventURISet {
			return s, false, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && !eventURISet {
			return s, false, nil
		}
		if e.status == nil && eventURISet {
			return s, false, nil
		}
		if e.status == nil && !eventURISet {
			return s, false, nil
		}
	}

	// state 3
	if s.Status == shared.ArchivalStatusDisabled && !stateURISet {
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && eventURISet {
			return &ArchivalState{
				Status: shared.ArchivalStatusEnabled,
				URI:    e.URI,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusEnabled && !eventURISet {
			return &ArchivalState{
				Status: shared.ArchivalStatusEnabled,
				URI:    e.defaultURI,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && eventURISet {
			return &ArchivalState{
				Status: shared.ArchivalStatusDisabled,
				URI:    e.URI,
			}, true, nil
		}
		if e.status != nil && *e.status == shared.ArchivalStatusDisabled && !eventURISet {
			return s, false, nil
		}
		if e.status == nil && eventURISet {
			return &ArchivalState{
				Status: shared.ArchivalStatusDisabled,
				URI:    e.URI,
			}, true, nil
		}
		if e.status == nil && !eventURISet {
			return s, false, nil
		}
	}
	return nil, false, errCannotHandleStateChange
}
