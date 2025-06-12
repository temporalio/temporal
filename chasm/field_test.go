package chasm

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

type fieldSuite struct {
	suite.Suite
	*require.Assertions
	protorequire.ProtoAssertions

	controller  *gomock.Controller
	nodeBackend *MockNodeBackend

	registry        *Registry
	timeSource      *clock.EventTimeSource
	nodePathEncoder NodePathEncoder
	logger          log.Logger
}

func TestFieldSuite(t *testing.T) {
	suite.Run(t, new(fieldSuite))
}

func (s *fieldSuite) SetupTest() {
	s.initAssertions()
	s.controller = gomock.NewController(s.T())
	s.nodeBackend = NewMockNodeBackend(s.controller)

	s.registry = NewRegistry()
	err := s.registry.Register(newTestLibrary(s.controller))
	s.NoError(err)

	s.timeSource = clock.NewEventTimeSource()
	s.nodePathEncoder = &testNodePathEncoder{}
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
}

func (s *fieldSuite) SetupSubTest() {
	s.initAssertions()
}

func (s *fieldSuite) initAssertions() {
	// `s.Assertions` (as well as other test helpers which depends on `s.T()`) must be initialized on
	// both test and subtest levels (but not suite level, where `s.T()` is `nil`).
	//
	// If these helpers are not reinitialized on subtest level, any failed `assert` in
	// subtest will fail the entire test (not subtest) immediately without running other subtests.

	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

func (s *fieldSuite) TestInternalFieldName() {
	f := Field[any]{}
	fT := reflect.TypeOf(f)

	_, ok := fT.FieldByName(internalFieldName)
	s.True(ok, "expected field %s not found", internalFieldName)
}

func (s *fieldSuite) TestFieldGetSimple() {
	tests := []struct {
		name     string
		field    Field[*TestSubComponent1]
		expected *TestSubComponent1
	}{
		{
			name: "Get with non-nil value",
			field: Field[*TestSubComponent1]{
				Internal: newFieldInternalWithValue(
					fieldTypeComponent,
					&TestSubComponent1{SubComponent1Data: &protoMessageType{
						CreateRequestId: "component-data",
					}},
				)},
			expected: &TestSubComponent1{SubComponent1Data: &protoMessageType{
				CreateRequestId: "component-data",
			}},
		},
		{
			name: "Get with nil value and nil node",
			field: Field[*TestSubComponent1]{
				Internal: newFieldInternalWithNode(nil),
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result, err := tt.field.Get(nil)
			s.NoError(err)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *fieldSuite) TestFieldGetComponent() {
	serializedNodes := testComponentSerializedNodes()

	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	chasmContext := NewMutableContext(context.Background(), node)

	c, err := node.Component(chasmContext, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	s.NotNil(c)

	tc := c.(*TestComponent)

	sc1, err := tc.SubComponent1.Get(chasmContext)
	s.NoError(err)
	s.NotNil(sc1)
	s.ProtoEqual(&protoMessageType{
		CreateRequestId: "sub-component1-data",
	}, sc1.SubComponent1Data)

	sd1, err := tc.SubData1.Get(chasmContext)
	s.NoError(err)
	s.NotNil(sd1)
	s.ProtoEqual(&protoMessageType{
		CreateRequestId: "sub-data1",
	}, sd1)
}
