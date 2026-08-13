package fact

import "time"

// EventTimeFact is implemented by facts whose source supplies an authoritative event time.
type EventTimeFact interface {
	EventTime() time.Time
	SetEventTime(time.Time)
}

// EventTimeCarrier carries the authoritative source time for a fact.
type EventTimeCarrier struct {
	ObservedEventTime time.Time
}

func (e *EventTimeCarrier) EventTime() time.Time { return e.ObservedEventTime }

func (e *EventTimeCarrier) SetEventTime(eventTime time.Time) { e.ObservedEventTime = eventTime }
