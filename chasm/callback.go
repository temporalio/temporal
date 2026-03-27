package chasm

const (
	CallbackLibraryName            = "callback"
	CallbackComponentName          = "callback"
	CallbackExecutionComponentName = "callback_execution"
)

var (
	CallbackComponentID          = GenerateTypeID(FullyQualifiedName(CallbackLibraryName, CallbackComponentName))
	CallbackExecutionComponentID = GenerateTypeID(FullyQualifiedName(CallbackLibraryName, CallbackExecutionComponentName))
)
