package definition

import (
	"fmt"
)

const (
	resourceIDTemplate       = "%v::%v"
	eventReappliedIDTemplate = "%v::%v::%v"
)

type (
	// DeduplicationID uses to generate id for deduplication
	DeduplicationID interface {
		GetID() string
	}
)

// Deduplication id type
const (
	eventReappliedID = iota
)

// Deduplication resource struct
type (
	// EventReappliedID is the deduplication resource for reapply event
	EventReappliedID struct {
		id string
	}
)

// NewEventReappliedID returns EventReappliedID resource
func NewEventReappliedID(
	runID string,
	eventID int64,
	version int64,
) EventReappliedID {

	newID := fmt.Sprintf(
		eventReappliedIDTemplate,
		runID,
		eventID,
		version,
	)
	return EventReappliedID{
		id: newID,
	}
}

// GetID returns id of EventReappliedID
func (e EventReappliedID) GetID() string {
	return e.id
}

// GenerateDeduplicationKey generates deduplication key
func GenerateDeduplicationKey(
	resource DeduplicationID,
) string {

	switch resource.(type) {
	case EventReappliedID:
		return generateKey(eventReappliedID, resource.GetID())
	default:
		panic("unsupported deduplication key")
	}
}

func generateKey(resourceType int32, id string) string {
	return fmt.Sprintf(resourceIDTemplate, resourceType, id)
}
