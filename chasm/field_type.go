package chasm

type fieldType int

const (
	fieldTypeUnspecified fieldType = iota
	fieldTypeComponent
	fieldTypePointer
	fieldTypeData
)
