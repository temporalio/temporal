package chasm

const (
	CallbackLibraryName   = "callback"
	CallbackComponentName = "callback"
	// TODO(chrsmith): Unresolved comment: https://github.com/temporalio/temporal/pull/9805/changes#r3097273857
	// @bergundy: There shouldn't be a separate component for the execution. We can reuse the same component.
	// @quinn: It could be one component, I don't think that is a good idea.
	// @bergundy: That is what we have done with other standalone + embedded components. There are tradeoffs to both approaches.
	// 			  I would prefer consistency.
	// ^-- @chrsmith: What does this mean? Unifying everything into a single, monster Callback component?
	//			      What would that entail?
	CallbackExecutionComponentName = "callback_execution"
)

var (
	CallbackComponentID          = GenerateTypeID(FullyQualifiedName(CallbackLibraryName, CallbackComponentName))
	CallbackExecutionComponentID = GenerateTypeID(FullyQualifiedName(CallbackLibraryName, CallbackExecutionComponentName))
)
