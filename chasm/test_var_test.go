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
	testComponentFQN      = fullyQualifiedName(testLibraryName, testComponentName)
	testSubComponent1FQN  = fullyQualifiedName(testLibraryName, testSubComponent1Name)
	testSubComponent11FQN = fullyQualifiedName(testLibraryName, testSubComponent11Name)
	testSubComponent2FQN  = fullyQualifiedName(testLibraryName, testSubComponent2Name)

	testSideEffectTaskFQN         = fullyQualifiedName(testLibraryName, testSideEffectTaskName)
	testOutboundSideEffectTaskFQN = fullyQualifiedName(testLibraryName, testOutboundSideEffectTaskName)
	testPureTaskFQN               = fullyQualifiedName(testLibraryName, testPureTaskName)
)

var (
	testComponentTypeID      = generateTypeID(testComponentFQN)
	testSubComponent1TypeID  = generateTypeID(testSubComponent1FQN)
	testSubComponent11TypeID = generateTypeID(testSubComponent11FQN)
	testSubComponent2TypeID  = generateTypeID(testSubComponent2FQN)

	testSideEffectTaskTypeID         = generateTypeID(testSideEffectTaskFQN)
	testOutboundSideEffectTaskTypeID = generateTypeID(testOutboundSideEffectTaskFQN)
	testPureTaskTypeID               = generateTypeID(testPureTaskFQN)
)
