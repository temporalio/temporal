package chasm

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	// TestComponent is a sample CHASM component used in tests.
	// It would be nice to move it another package, but this creates a circular dependency.

	protoMessageType = *persistencespb.ActivityInfo // Random proto message.
	TestComponent    struct {
		UnimplementedComponent

		ComponentData protoMessageType
		SubComponent1 *Field[*TestSubComponent1]
		SubComponent2 *Field[*TestSubComponent2]
		SubData1      *Field[protoMessageType]
	}

	TestSubComponent1 struct {
		UnimplementedComponent

		SubComponent1Data protoMessageType
		SubComponent11    *Field[*TestSubComponent11]
		SubData11         *Field[protoMessageType] // Random proto message.
	}

	TestSubComponent11 struct {
		UnimplementedComponent

		SubComponent11Data protoMessageType
	}

	TestSubComponent2 struct {
		UnimplementedComponent
		SubComponent2Data protoMessageType
	}
)
