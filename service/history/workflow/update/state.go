package update

import (
	"fmt"
)

type (
	state    uint32
	stateSet uint32
)

const (
	stateCreated state = 1 << iota
	stateProvisionallyAdmitted
	stateAdmitted
	stateSent
	stateProvisionallyAccepted
	stateAccepted
	stateProvisionallyCompleted
	stateProvisionallyCompletedAfterAccepted
	stateCompleted
	stateProvisionallyAborted
	stateAborted
	lastState
)

func (s state) String() string {
	switch s {
	case stateCreated:
		return "Created"
	case stateProvisionallyAdmitted:
		return "ProvisionallyAdmitted"
	case stateAdmitted:
		return "Admitted"
	case stateSent:
		return "Sent"
	case stateProvisionallyAccepted:
		return "ProvisionallyAccepted"
	case stateAccepted:
		return "Accepted"
	case stateProvisionallyCompleted:
		return "ProvisionallyCompleted"
	case stateProvisionallyCompletedAfterAccepted:
		return "ProvisionallyCompletedAfterAccepted"
	case stateCompleted:
		return "Completed"
	case stateProvisionallyAborted:
		return "ProvisionallyAborted"
	case stateAborted:
		return "Aborted"
	case lastState:
		return fmt.Sprintf("invalid state %d", s)
	}
	return fmt.Sprintf("unrecognized state %d", s)
}

func (s state) Matches(mask stateSet) bool {
	return uint32(s)&uint32(mask) == uint32(s)
}
