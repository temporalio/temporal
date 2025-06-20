package number

import (
	"fmt"
)

const (
	TypeUnknown Type = iota
	TypeFloat
	TypeInt
	TypeUint
)

type (
	Type   int
	Number struct {
		numberType Type
		value      interface{}
	}
)

func NewNumber(
	value interface{},
) Number {

	var numberType Type
	var number interface{}
	switch n := value.(type) {
	case int8:
		numberType = TypeInt
		number = int(n)
	case int16:
		numberType = TypeInt
		number = int(n)
	case int32:
		numberType = TypeInt
		number = int(n)
	case int64:
		numberType = TypeInt
		number = int(n)
	case int:
		numberType = TypeInt
		number = n

	case uint8:
		numberType = TypeUint
		number = uint(n)
	case uint16:
		numberType = TypeUint
		number = uint(n)
	case uint32:
		numberType = TypeUint
		number = uint(n)
	case uint64:
		numberType = TypeUint
		number = uint(n)
	case uint:
		numberType = TypeUint
		number = n

	case float32:
		numberType = TypeFloat
		number = float64(n)
	case float64:
		numberType = TypeFloat
		number = n

	default:
		// DO NOT panic here
		// the value is provided during runtime
		// the logic cannot just panic if input is not a number
		numberType = TypeUnknown
		number = nil
	}

	return Number{
		numberType: numberType,
		value:      number,
	}
}

func (n Number) GetIntOrDefault(
	defaultValue int,
) int {
	switch n.numberType {
	case TypeFloat:
		return int(n.value.(float64))
	case TypeInt:
		return n.value.(int)
	case TypeUint:
		return int(n.value.(uint))
	case TypeUnknown:
		return defaultValue
	default:
		panic(fmt.Sprintf("unknown number type: %v", n.numberType))
	}
}

func (n Number) GetUintOrDefault(
	defaultValue uint,
) uint {
	switch n.numberType {
	case TypeFloat:
		return uint(n.value.(float64))
	case TypeInt:
		return uint(n.value.(int))
	case TypeUint:
		return n.value.(uint)
	case TypeUnknown:
		return defaultValue
	default:
		panic(fmt.Sprintf("unknown number type: %v", n.numberType))
	}
}

func (n Number) GetFloatOrDefault(
	defaultValue float64,
) float64 {
	switch n.numberType {
	case TypeFloat:
		return n.value.(float64)
	case TypeInt:
		return float64(n.value.(int))
	case TypeUint:
		return float64(n.value.(uint))
	case TypeUnknown:
		return defaultValue
	default:
		panic(fmt.Sprintf("unknown number type: %v", n.numberType))
	}
}
