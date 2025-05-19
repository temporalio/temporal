package chasm

type Collection[K comparable, T any] map[K]Field[T]
