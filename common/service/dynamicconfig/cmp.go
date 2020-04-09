package dynamicconfig

import (
	"math"
	"time"
)

func float64CompareEquals(a, b interface{}) bool {
	aVal := a.(float64)
	bVal := b.(float64)
	return (aVal == bVal) || math.Nextafter(aVal, bVal) == aVal
}

func intCompareEquals(a, b interface{}) bool {
	aVal := a.(int)
	bVal := b.(int)
	return aVal == bVal
}

func boolCompareEquals(a, b interface{}) bool {
	aVal := a.(bool)
	bVal := b.(bool)
	return aVal == bVal
}

func stringCompareEquals(a, b interface{}) bool {
	aVal := a.(string)
	bVal := b.(string)
	return aVal == bVal
}

func durationCompareEquals(a, b interface{}) bool {
	aVal := a.(time.Duration)
	bVal := b.(time.Duration)
	return aVal.Nanoseconds() == bVal.Nanoseconds()
}
