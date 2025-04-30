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
	PriorityHigh = getPriority(highPriorityClass, mediumPrioritySubclass)
	PriorityLow  = getPriority(lowPriorityClass, mediumPrioritySubclass)
)

var (
	PriorityName = map[Priority]string{
		PriorityHigh: "high",
		PriorityLow:  "low",
	}

	PriorityValue = map[string]Priority{
		"high": PriorityHigh,
		"low":  PriorityLow,
	}

	CallerTypeToPriority = map[string]Priority{
		headers.CallerTypeBackground:  PriorityHigh,
		headers.CallerTypePreemptable: PriorityLow,
	}

	PriorityToCallerType = map[Priority]string{
		PriorityHigh: headers.CallerTypeBackground,
		PriorityLow:  headers.CallerTypePreemptable,
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
