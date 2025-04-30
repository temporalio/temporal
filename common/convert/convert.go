package convert

import (
	"math"
	"strconv"
)

// Int32Ceil return the int32 ceil of a float64
func Int32Ceil(v float64) int32 {
	return int32(math.Ceil(v))
}

// Int64Ceil return the int64 ceil of a float64
func Int64Ceil(v float64) int64 {
	return int64(math.Ceil(v))
}

func IntToString(v int) string {
	return Int64ToString(int64(v))
}

func Uint64ToString(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func Int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

func Int32ToString(v int32) string {
	return Int64ToString(int64(v))
}

func Uint16ToString(v uint16) string {
	return strconv.FormatUint(uint64(v), 10)
}

func Int64SetToSlice(
	inputs map[int64]struct{},
) []int64 {
	outputs := make([]int64, len(inputs))
	i := 0
	for item := range inputs {
		outputs[i] = item
		i++
	}
	return outputs
}

func Int64SliceToSet(
	inputs []int64,
) map[int64]struct{} {
	outputs := make(map[int64]struct{}, len(inputs))
	for _, item := range inputs {
		outputs[item] = struct{}{}
	}
	return outputs
}

func StringSetToSlice(
	inputs map[string]struct{},
) []string {
	outputs := make([]string, len(inputs))
	i := 0
	for item := range inputs {
		outputs[i] = item
		i++
	}
	return outputs
}
func StringSliceToSet(
	inputs []string,
) map[string]struct{} {
	outputs := make(map[string]struct{}, len(inputs))
	for _, item := range inputs {
		outputs[item] = struct{}{}
	}
	return outputs
}
