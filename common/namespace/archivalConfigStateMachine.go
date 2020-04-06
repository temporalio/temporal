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

package namespace

import (
	namespacepb "go.temporal.io/temporal-proto/namespace"
	"go.temporal.io/temporal-proto/serviceerror"
)

// namespaceArchivalConfigStateMachine is only used by namespaceHandler.
// It is simply meant to simplify the logic around archival namespace state changes.
// Logically this class can be thought of as part of namespaceHandler.

type (
	// ArchivalState represents the state of archival config
	// the only invalid state is {URI="", status=enabled}
	// once URI is set it is immutable
	ArchivalState struct {
		Status namespacepb.ArchivalStatus
		URI    string
	}

	// ArchivalEvent represents a change request to archival config state
	// the only restriction placed on events is that defaultURI is not empty
	// status can be nil, enabled, or disabled (nil indicates no update by user is being attempted)
	ArchivalEvent struct {
		defaultURI string
		URI        string
		status     namespacepb.ArchivalStatus
	}
)

// the following errors represents impossible code states that should never occur
var (
	errInvalidState            = serviceerror.NewInvalidArgument("Encountered illegal state: archival is enabled but URI is not set (should be impossible)")
	errInvalidEvent            = serviceerror.NewInvalidArgument("Encountered illegal event: default URI is not set (should be impossible)")
	errCannotHandleStateChange = serviceerror.NewInvalidArgument("Encountered current state and event that cannot be handled (should be impossible)")
	errURIUpdate               = serviceerror.NewInvalidArgument("Cannot update existing archival URI")
)

func neverEnabledState() *ArchivalState {
	return &ArchivalState{
		URI:    "",
		Status: namespacepb.ArchivalStatus_Disabled,
	}
}

func (e *ArchivalEvent) validate() error {
	if len(e.defaultURI) == 0 {
		return errInvalidEvent
	}
	return nil
}

func (s *ArchivalState) validate() error {
	if s.Status == namespacepb.ArchivalStatus_Enabled && len(s.URI) == 0 {
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
	if s.Status == namespacepb.ArchivalStatus_Enabled && stateURISet {
		if e.status == namespacepb.ArchivalStatus_Enabled && eventURISet {
			return s, false, nil
		}
		if e.status == namespacepb.ArchivalStatus_Enabled && !eventURISet {
			return s, false, nil
		}
		if e.status == namespacepb.ArchivalStatus_Disabled && eventURISet {
			return &ArchivalState{
				Status: namespacepb.ArchivalStatus_Disabled,
				URI:    s.URI,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Disabled && !eventURISet {
			return &ArchivalState{
				Status: namespacepb.ArchivalStatus_Disabled,
				URI:    s.URI,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Default && eventURISet {
			return s, false, nil
		}
		if e.status == namespacepb.ArchivalStatus_Default && !eventURISet {
			return s, false, nil
		}
	}

	// state 2
	if s.Status == namespacepb.ArchivalStatus_Disabled && stateURISet {
		if e.status == namespacepb.ArchivalStatus_Enabled && eventURISet {
			return &ArchivalState{
				URI:    s.URI,
				Status: namespacepb.ArchivalStatus_Enabled,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Enabled && !eventURISet {
			return &ArchivalState{
				Status: namespacepb.ArchivalStatus_Enabled,
				URI:    s.URI,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Disabled && eventURISet {
			return s, false, nil
		}
		if e.status == namespacepb.ArchivalStatus_Disabled && !eventURISet {
			return s, false, nil
		}
		if e.status == namespacepb.ArchivalStatus_Default && eventURISet {
			return s, false, nil
		}
		if e.status == namespacepb.ArchivalStatus_Default && !eventURISet {
			return s, false, nil
		}
	}

	// state 3
	if s.Status == namespacepb.ArchivalStatus_Disabled && !stateURISet {
		if e.status == namespacepb.ArchivalStatus_Enabled && eventURISet {
			return &ArchivalState{
				Status: namespacepb.ArchivalStatus_Enabled,
				URI:    e.URI,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Enabled && !eventURISet {
			return &ArchivalState{
				Status: namespacepb.ArchivalStatus_Enabled,
				URI:    e.defaultURI,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Disabled && eventURISet {
			return &ArchivalState{
				Status: namespacepb.ArchivalStatus_Disabled,
				URI:    e.URI,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Disabled && !eventURISet {
			return s, false, nil
		}
		if e.status == namespacepb.ArchivalStatus_Default && eventURISet {
			return &ArchivalState{
				Status: namespacepb.ArchivalStatus_Disabled,
				URI:    e.URI,
			}, true, nil
		}
		if e.status == namespacepb.ArchivalStatus_Default && !eventURISet {
			return s, false, nil
		}
	}
	return nil, false, errCannotHandleStateChange
}
