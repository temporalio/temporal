package namespace

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// namespaceArchivalConfigStateMachine is only used by namespaceHandler.
// It is simply meant to simplify the logic around archival namespace state changes.
// Logically this class can be thought of as part of namespaceHandler.

type (
	// ArchivalConfigState represents the state of archival config
	// the only invalid state is {URI="", state=enabled}
	// once URI is set it is immutable
	ArchivalConfigState struct {
		State enumspb.ArchivalState
		URI   string
	}

	// ArchivalConfigEvent represents a change request to archival config state
	// the only restriction placed on events is that defaultURI is not empty
	// state can be nil, enabled, or disabled (nil indicates no update by user is being attempted)
	ArchivalConfigEvent struct {
		DefaultURI string
		URI        string
		State      enumspb.ArchivalState
	}
)

// the following errors represents impossible code states that should never occur
var (
	errInvalidState            = serviceerror.NewInvalidArgument("Encountered illegal state: archival is enabled but URI is not set (should be impossible)")
	errInvalidEvent            = serviceerror.NewInvalidArgument("Encountered illegal event: default URI is not set (should be impossible)")
	errCannotHandleStateChange = serviceerror.NewInvalidArgument("Encountered current state and event that cannot be handled (should be impossible)")
	errURIUpdate               = serviceerror.NewInvalidArgument("Cannot update existing archival URI")
)

func NeverEnabledState() *ArchivalConfigState {
	return &ArchivalConfigState{
		URI:   "",
		State: enumspb.ARCHIVAL_STATE_DISABLED,
	}
}

func (e *ArchivalConfigEvent) Validate() error {
	if len(e.DefaultURI) == 0 {
		return errInvalidEvent
	}
	return nil
}

func (s *ArchivalConfigState) validate() error {
	if s.State == enumspb.ARCHIVAL_STATE_ENABLED && len(s.URI) == 0 {
		return errInvalidState
	}
	return nil
}

func (s *ArchivalConfigState) GetNextState(
	e *ArchivalConfigEvent,
	URIValidationFunc func(URI string) error,
) (nextState *ArchivalConfigState, changed bool, err error) {
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
	if err := e.Validate(); err != nil {
		return nil, false, err
	}

	/**
	At this point state and event are both non-nil and valid.

	State can be any one of the following:
	{state=enabled,  URI="foo"}
	{state=disabled, URI="foo"}
	{state=disabled, URI=""}

	Event can be any one of the following:
	{state=enabled,  URI="foo", defaultURI="bar"}
	{state=enabled,  URI="",    defaultURI="bar"}
	{state=disabled, URI="foo", defaultURI="bar"}
	{state=disabled, URI="",    defaultURI="bar"}
	{state=nil,      URI="foo", defaultURI="bar"}
	{state=nil,      URI="",    defaultURI="bar"}
	*/

	stateURISet := len(s.URI) != 0
	eventURISet := len(e.URI) != 0

	// factor this case out to ensure that URI is immutable
	if stateURISet && eventURISet && s.URI != e.URI {
		return nil, false, errURIUpdate
	}

	// state 1
	if s.State == enumspb.ARCHIVAL_STATE_ENABLED && stateURISet {
		if e.State == enumspb.ARCHIVAL_STATE_ENABLED && eventURISet {
			return s, false, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_ENABLED && !eventURISet {
			return s, false, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_DISABLED && eventURISet {
			return &ArchivalConfigState{
				State: enumspb.ARCHIVAL_STATE_DISABLED,
				URI:   s.URI,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_DISABLED && !eventURISet {
			return &ArchivalConfigState{
				State: enumspb.ARCHIVAL_STATE_DISABLED,
				URI:   s.URI,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED && eventURISet {
			return s, false, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED && !eventURISet {
			return s, false, nil
		}
	}

	// state 2
	if s.State == enumspb.ARCHIVAL_STATE_DISABLED && stateURISet {
		if e.State == enumspb.ARCHIVAL_STATE_ENABLED && eventURISet {
			return &ArchivalConfigState{
				URI:   s.URI,
				State: enumspb.ARCHIVAL_STATE_ENABLED,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_ENABLED && !eventURISet {
			return &ArchivalConfigState{
				State: enumspb.ARCHIVAL_STATE_ENABLED,
				URI:   s.URI,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_DISABLED && eventURISet {
			return s, false, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_DISABLED && !eventURISet {
			return s, false, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED && eventURISet {
			return s, false, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED && !eventURISet {
			return s, false, nil
		}
	}

	// state 3
	if s.State == enumspb.ARCHIVAL_STATE_DISABLED && !stateURISet {
		if e.State == enumspb.ARCHIVAL_STATE_ENABLED && eventURISet {
			return &ArchivalConfigState{
				State: enumspb.ARCHIVAL_STATE_ENABLED,
				URI:   e.URI,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_ENABLED && !eventURISet {
			return &ArchivalConfigState{
				State: enumspb.ARCHIVAL_STATE_ENABLED,
				URI:   e.DefaultURI,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_DISABLED && eventURISet {
			return &ArchivalConfigState{
				State: enumspb.ARCHIVAL_STATE_DISABLED,
				URI:   e.URI,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_DISABLED && !eventURISet {
			return s, false, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED && eventURISet {
			return &ArchivalConfigState{
				State: enumspb.ARCHIVAL_STATE_DISABLED,
				URI:   e.URI,
			}, true, nil
		}
		if e.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED && !eventURISet {
			return s, false, nil
		}
	}
	return nil, false, errCannotHandleStateChange
}
