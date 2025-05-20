package chasm

// collectionKeyTypes must match actual Collection key type definition.
const collectionKeyTypes = "string | int | int8 | int32 | int64 | uint | uint8 | uint32 | uint64 | bool"

type Collection[K string | int | int8 | int32 | int64 | uint | uint8 | uint32 | uint64 | bool, T any] map[K]Field[T]
