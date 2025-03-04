package chasm

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
)

type (
	TestComponent struct {
		UnimplementedComponent
		ComponentData proto.Message
		SubComponent1 *Field[TestSubComponent1]
		SubComponent2 *Field[TestSubComponent2]
		SubData1      *Field[*persistencespb.ActivityInfo]
	}

	TestSubComponent1 struct {
		UnimplementedComponent
		SubComponent1Data proto.Message
	}

	TestSubComponent2 struct {
		UnimplementedComponent
		SubComponent2Data proto.Message
	}
)
