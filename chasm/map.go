package chasm

// mapKeyTypes must match actual Map key type definition.
const mapKeyTypes = "string | int | int8 | int32 | int64 | uint | uint8 | uint32 | uint64 | bool"

type Map[K string | int | int8 | int32 | int64 | uint | uint8 | uint32 | uint64 | bool, T any] map[K]Field[T]
