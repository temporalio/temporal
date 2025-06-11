package tasks

import (
	"strconv"

	"go.temporal.io/server/common/headers"
)

type (
	Priority int
)

const (
	numBitsPerLevel = 3
)

const (
	highPriorityClass Priority = iota << numBitsPerLevel
	mediumPriorityClass
	lowPriorityClass
)

const (
	highPrioritySubclass Priority = iota
	mediumPrioritySubclass
	lowPrioritySubclass
)

var (
	PriorityHigh       = getPriority(highPriorityClass, mediumPrioritySubclass)
	PriorityLow        = getPriority(highPriorityClass, lowPrioritySubclass)
	PriorityBackground = getPriority(lowPriorityClass, mediumPrioritySubclass)
)

var (
	PriorityName = map[Priority]string{
		PriorityHigh:       "high",
		PriorityLow:        "low",
		PriorityBackground: "background",
	}

	PriorityValue = map[string]Priority{
		"high":       PriorityHigh,
		"low":        PriorityLow,
		"background": PriorityBackground,
	}

	CallerTypeToPriority = map[string]Priority{
		headers.CallerTypeBackgroundHigh: PriorityHigh,
		headers.CallerTypeBackgroundLow:  PriorityLow,
		headers.CallerTypePreemptable:    PriorityBackground,
	}

	PriorityToCallerType = map[Priority]string{
		PriorityHigh:       headers.CallerTypeBackgroundHigh,
		PriorityLow:        headers.CallerTypeBackgroundLow,
		PriorityBackground: headers.CallerTypePreemptable,
	}
)

func (p Priority) String() string {
	s, ok := PriorityName[p]
	if ok {
		return s
	}
	return strconv.Itoa(int(p))
}

func (p Priority) CallerType() string {
	s, ok := PriorityToCallerType[p]
	if ok {
		return s
	}
	return headers.CallerTypePreemptable
}

func getPriority(
	class, subClass Priority,
) Priority {
	return class | subClass
}
