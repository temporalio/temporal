package chasm

const (
	testLibraryName              = "TestLibrary"
	testComponentName            = "test_component"
	testSubComponent1Name        = "test_sub_component_1"
	testSubComponent11Name       = "test_sub_component_11"
	testSubComponent2Name        = "test_sub_component_2"
	testSkipIfCleanComponentName = "test_skip_if_clean_component"

	testSideEffectTaskName            = "test_side_effect_task"
	testDiscardableSideEffectTaskName = "test_discardable_side_effect_task"
	testOutboundSideEffectTaskName    = "test_outbound_side_effect_task"
	testPureTaskName                  = "test_pure_task"
)

var (
	testComponentFQN            = FullyQualifiedName(testLibraryName, testComponentName)
	testSubComponent1FQN        = FullyQualifiedName(testLibraryName, testSubComponent1Name)
	testSubComponent11FQN       = FullyQualifiedName(testLibraryName, testSubComponent11Name)
	testSubComponent2FQN        = FullyQualifiedName(testLibraryName, testSubComponent2Name)
	testSkipIfCleanComponentFQN = FullyQualifiedName(testLibraryName, testSkipIfCleanComponentName)

	testSideEffectTaskFQN            = FullyQualifiedName(testLibraryName, testSideEffectTaskName)
	testDiscardableSideEffectTaskFQN = FullyQualifiedName(testLibraryName, testDiscardableSideEffectTaskName)
	testOutboundSideEffectTaskFQN    = FullyQualifiedName(testLibraryName, testOutboundSideEffectTaskName)
	testPureTaskFQN                  = FullyQualifiedName(testLibraryName, testPureTaskName)
)

var (
	testComponentTypeID            = GenerateTypeID(testComponentFQN)
	testSubComponent1TypeID        = GenerateTypeID(testSubComponent1FQN)
	testSubComponent11TypeID       = GenerateTypeID(testSubComponent11FQN)
	testSubComponent2TypeID        = GenerateTypeID(testSubComponent2FQN)
	testSkipIfCleanComponentTypeID = GenerateTypeID(testSkipIfCleanComponentFQN)

	testSideEffectTaskTypeID            = GenerateTypeID(testSideEffectTaskFQN)
	testDiscardableSideEffectTaskTypeID = GenerateTypeID(testDiscardableSideEffectTaskFQN)
	testOutboundSideEffectTaskTypeID    = GenerateTypeID(testOutboundSideEffectTaskFQN)
	testPureTaskTypeID                  = GenerateTypeID(testPureTaskFQN)
)
