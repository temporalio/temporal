package chasm

const (
	testLibraryName        = "TestLibrary"
	testComponentName      = "test_component"
	testSubComponent1Name  = "test_sub_component_1"
	testSubComponent11Name = "test_sub_component_11"
	testSubComponent2Name  = "test_sub_component_2"

	testSideEffectTaskName         = "test_side_effect_task"
	testOutboundSideEffectTaskName = "test_outbound_side_effect_task"
	testPureTaskName               = "test_pure_task"
)

var (
	testComponentFQN      = FullyQualifiedName(testLibraryName, testComponentName)
	testSubComponent1FQN  = FullyQualifiedName(testLibraryName, testSubComponent1Name)
	testSubComponent11FQN = FullyQualifiedName(testLibraryName, testSubComponent11Name)
	testSubComponent2FQN  = FullyQualifiedName(testLibraryName, testSubComponent2Name)

	testSideEffectTaskFQN         = FullyQualifiedName(testLibraryName, testSideEffectTaskName)
	testOutboundSideEffectTaskFQN = FullyQualifiedName(testLibraryName, testOutboundSideEffectTaskName)
	testPureTaskFQN               = FullyQualifiedName(testLibraryName, testPureTaskName)
)

var (
	testComponentTypeID      = GenerateTypeID(testComponentFQN)
	testSubComponent1TypeID  = GenerateTypeID(testSubComponent1FQN)
	testSubComponent11TypeID = GenerateTypeID(testSubComponent11FQN)
	testSubComponent2TypeID  = GenerateTypeID(testSubComponent2FQN)

	testSideEffectTaskTypeID         = GenerateTypeID(testSideEffectTaskFQN)
	testOutboundSideEffectTaskTypeID = GenerateTypeID(testOutboundSideEffectTaskFQN)
	testPureTaskTypeID               = GenerateTypeID(testPureTaskFQN)
)
