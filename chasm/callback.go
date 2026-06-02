package chasm

const (
	CallbackLibraryName   = "callback"
	CallbackComponentName = "callback"
)

var (
	CallbackComponentID = GenerateTypeID(FullyQualifiedName(CallbackLibraryName, CallbackComponentName))
)
