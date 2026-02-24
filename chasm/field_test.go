package chasm

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
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
	metricsHandler  metrics.Handler
}

func TestFieldSuite(t *testing.T) {
	suite.Run(t, new(fieldSuite))
}

func (s *fieldSuite) SetupTest() {
	s.initAssertions()
	s.controller = gomock.NewController(s.T())
	s.nodeBackend = &MockNodeBackend{}

	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.registry = NewRegistry(s.logger)
	err := s.registry.Register(newTestLibrary(s.controller))
	s.NoError(err)

	s.timeSource = clock.NewEventTimeSource()
	s.nodePathEncoder = &testNodePathEncoder{}
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
			result, _ := tt.field.TryGet(nil)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *fieldSuite) TestFieldGetComponent() {
	serializedNodes := testComponentSerializedNodes()

	node, err := s.newTestTree(serializedNodes)
	s.NoError(err)

	chasmContext := NewMutableContext(context.Background(), node)

	c, err := node.Component(chasmContext, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	s.NotNil(c)

	tc := c.(*TestComponent)

	sc1 := tc.SubComponent1.Get(chasmContext)
	s.NotNil(sc1)
	s.ProtoEqual(&protoMessageType{
		CreateRequestId: "sub-component1-data",
	}, sc1.SubComponent1Data)

	sd1 := tc.SubData1.Get(chasmContext)
	s.NotNil(sd1)
	s.ProtoEqual(&protoMessageType{
		CreateRequestId: "sub-data1",
	}, sd1)
}

func (s *fieldSuite) newTestTree(
	serializedNodes map[string]*persistencespb.ChasmNode,
) (*Node, error) {
	if len(serializedNodes) == 0 {
		return NewEmptyTree(
			s.registry,
			s.timeSource,
			s.nodeBackend,
			s.nodePathEncoder,
			s.logger,
			s.metricsHandler,
		), nil
	}
	return NewTreeFromDB(
		serializedNodes,
		s.registry,
		s.timeSource,
		s.nodeBackend,
		s.nodePathEncoder,
		s.logger,
		s.metricsHandler,
	)
}

// setupComponentWithTree creates a basic component structure and attaches it to the tree.
func (s *fieldSuite) setupComponentWithTree(rootComponent *TestComponent) (*Node, MutableContext, error) {
	rootNode := NewEmptyTree(
		s.registry,
		s.timeSource,
		s.nodeBackend,
		s.nodePathEncoder,
		s.logger,
		s.metricsHandler,
	)
	if err := rootNode.SetRootComponent(rootComponent); err != nil {
		return nil, nil, err
	}

	return rootNode, NewMutableContext(context.Background(), rootNode), nil
}

func (s *fieldSuite) TestDeferredPointerResolution() {
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	s.nodeBackend = &MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return workflowKey },
	}

	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			CreateRequestId: "sub-component1-data",
		},
	}

	rootComponent := &TestComponent{
		ComponentData: &protoMessageType{
			CreateRequestId: "component-data",
		},
		SubComponent1: NewComponentField(nil, sc1),
	}

	rootNode, ctx, err := s.setupComponentWithTree(rootComponent)
	s.NoError(err)

	// Get components from tree to mark nodes as needing sync.
	rootComponentInterface, err := rootNode.Component(ctx, ComponentRef{})
	s.NoError(err)
	rootComponent = rootComponentInterface.(*TestComponent)
	sc1 = rootComponent.SubComponent1.Get(ctx)

	// sc1 (child) points to rootComponent (parent) via component pointer.
	sc1.RootPointer = ComponentPointerTo(ctx, rootComponent)

	// Verify deferred state.
	s.Equal(fieldTypeDeferredPointer, sc1.RootPointer.Internal.fieldType())
	s.Equal(rootComponent, sc1.RootPointer.Internal.v)

	// CloseTransaction should resolve the deferred pointer.
	mutations, err := rootNode.CloseTransaction()
	s.NoError(err)
	s.NotEmpty(mutations.UpdatedNodes)

	// Verify the pointer was resolved to a regular pointer with path.
	s.Equal(fieldTypePointer, sc1.RootPointer.Internal.fieldType())

	cResolvedPath, ok := sc1.RootPointer.Internal.v.([]string)
	s.True(ok)
	s.Equal([]string{}, cResolvedPath)

	// Verify we can dereference the component pointer.
	resolvedComponent := sc1.RootPointer.Get(ctx)
	s.Equal(rootComponent, resolvedComponent)
}

func (s *fieldSuite) TestMixedPointerScenario() {
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	s.nodeBackend = &MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return workflowKey },
	}

	sc11 := &TestSubComponent11{
		SubComponent11Data: &protoMessageType{CreateRequestId: "sub-component11-data"},
	}

	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{CreateRequestId: "sub-component1-data"},
		SubComponent11:    NewComponentField(nil, sc11),
	}

	rootComponent := &TestComponent{
		ComponentData: &protoMessageType{CreateRequestId: "component-data"},
		SubComponent1: NewComponentField(nil, sc1),
	}

	rootNode, ctx, err := s.setupComponentWithTree(rootComponent)
	s.NoError(err)

	// Get components from tree to mark nodes as needing sync.
	rootComponentInterface, err := rootNode.Component(ctx, ComponentRef{})
	s.NoError(err)
	rootComponent = rootComponentInterface.(*TestComponent)
	sc1 = rootComponent.SubComponent1.Get(ctx)
	sc11 = sc1.SubComponent11.Get(ctx)

	// Transaction 1: sc11 points to root (grandparent).
	sc11.GrandparentPointer = ComponentPointerTo(ctx, rootComponent)

	_, err = rootNode.CloseTransaction()
	s.NoError(err)
	s.Equal(fieldTypePointer, sc11.GrandparentPointer.Internal.fieldType())

	// Transaction 2: sc1 points to root (parent).
	ctx2 := NewMutableContext(context.Background(), rootNode)
	rootComponentInterface, err = rootNode.Component(ctx2, ComponentRef{})
	s.NoError(err)

	rootComponent = rootComponentInterface.(*TestComponent)
	sc1 = rootComponent.SubComponent1.Get(ctx2)
	sc11 = sc1.SubComponent11.Get(ctx2)

	sc1.RootPointer = ComponentPointerTo(ctx2, rootComponent)

	s.Equal(fieldTypePointer, sc11.GrandparentPointer.Internal.fieldType())
	s.Equal(fieldTypeDeferredPointer, sc1.RootPointer.Internal.fieldType())

	_, err = rootNode.CloseTransaction()
	s.NoError(err)

	// Ensure both pointers have been resolved.
	s.Equal(fieldTypePointer, sc11.GrandparentPointer.Internal.fieldType())
	s.Equal(fieldTypePointer, sc1.RootPointer.Internal.fieldType())

	resolved1 := sc11.GrandparentPointer.Get(ctx2)
	s.Equal(rootComponent, resolved1)

	resolved2 := sc1.RootPointer.Get(ctx2)
	s.Equal(rootComponent, resolved2)
}

func (s *fieldSuite) TestUnresolvableDeferredPointerError() {
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	s.nodeBackend = &MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return workflowKey },
	}

	s.logger.(*testlogger.TestLogger).
		Expect(testlogger.Error, "failed to resolve deferred pointer during transaction close")

	orphanComponent := &TestSubComponent11{
		SubComponent11Data: &protoMessageType{
			CreateRequestId: "orphan-component",
		},
	}

	rootComponent := &TestComponent{
		ComponentData: &protoMessageType{
			CreateRequestId: "component-data",
		},
	}

	rootNode, ctx, err := s.setupComponentWithTree(rootComponent)
	s.NoError(err)

	// Get component from tree to mark node as needing sync.
	rootComponentInterface, err := rootNode.Component(ctx, ComponentRef{})
	s.NoError(err)
	rootComponent = rootComponentInterface.(*TestComponent)

	rootComponent.SubComponent11Pointer = ComponentPointerTo(ctx, orphanComponent)
	s.Equal(fieldTypeDeferredPointer, rootComponent.SubComponent11Pointer.Internal.fieldType())

	_, err = rootNode.CloseTransaction()
	s.Error(err)
	s.Contains(err.Error(), "failed to resolve deferred pointer during transaction close")
}

func (s *fieldSuite) TestNonAncestorComponentPointerRejected() {
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	s.nodeBackend = &MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return workflowKey },
	}

	s.logger.(*testlogger.TestLogger).
		Expect(testlogger.Error, "failed to resolve deferred pointer during transaction close")

	sc11 := &TestSubComponent11{
		SubComponent11Data: &protoMessageType{CreateRequestId: "sub-component11-data"},
	}

	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{CreateRequestId: "sub-component1-data"},
		SubComponent11:    NewComponentField(nil, sc11),
	}

	rootComponent := &TestComponent{
		ComponentData: &protoMessageType{CreateRequestId: "component-data"},
		SubComponent1: NewComponentField(nil, sc1),
	}

	rootNode, ctx, err := s.setupComponentWithTree(rootComponent)
	s.NoError(err)

	rootComponentInterface, err := rootNode.Component(ctx, ComponentRef{})
	s.NoError(err)
	rootComponent = rootComponentInterface.(*TestComponent)
	sc1 = rootComponent.SubComponent1.Get(ctx)
	sc11 = sc1.SubComponent11.Get(ctx)

	// Root pointing to descendant sc11 should be rejected.
	rootComponent.SubComponent11Pointer = ComponentPointerTo(ctx, sc11)

	_, err = rootNode.CloseTransaction()
	s.Error(err)
	s.Contains(err.Error(), "is not an ancestor of component")
}

func (s *fieldSuite) TestChildComponentPointerRejected() {
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	s.nodeBackend = &MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return workflowKey },
	}

	s.logger.(*testlogger.TestLogger).
		Expect(testlogger.Error, "failed to resolve deferred pointer during transaction close")

	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{CreateRequestId: "sub-component1-data"},
	}

	rootComponent := &TestComponent{
		ComponentData: &protoMessageType{CreateRequestId: "component-data"},
		SubComponent1: NewComponentField(nil, sc1),
	}

	rootNode, ctx, err := s.setupComponentWithTree(rootComponent)
	s.NoError(err)

	rootComponentInterface, err := rootNode.Component(ctx, ComponentRef{})
	s.NoError(err)
	rootComponent = rootComponentInterface.(*TestComponent)
	sc1 = rootComponent.SubComponent1.Get(ctx)

	// Root pointing to child sc1 via interface pointer should be rejected.
	rootComponent.SubComponentInterfacePointer = ComponentPointerTo[Component](ctx, sc1)

	_, err = rootNode.CloseTransaction()
	s.Error(err)
	s.Contains(err.Error(), "is not an ancestor of component")
}
