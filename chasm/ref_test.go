package chasm

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

type componentRefSuite struct {
	suite.Suite
	*require.Assertions
	protorequire.ProtoAssertions

	controller *gomock.Controller

	registry *Registry
}

func TestComponentRefSuite(t *testing.T) {
	suite.Run(t, new(componentRefSuite))
}

func (s *componentRefSuite) SetupTest() {
	// Do this in SetupSubTest() as well, if we have sub tests in this suite.
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.registry = NewRegistry(log.NewTestLogger())
	err := s.registry.Register(newTestLibrary(s.controller))
	s.NoError(err)
}

func (s *componentRefSuite) TestArchetypeID() {
	executionKey := ExecutionKey{
		NamespaceID: primitives.NewUUID().String(),
		BusinessID:  primitives.NewUUID().String(),
		RunID:       primitives.NewUUID().String(),
	}
	ref := NewComponentRef[*TestComponent](executionKey)

	archetypeID, err := ref.ArchetypeID(s.registry)
	s.NoError(err)

	rc, ok := s.registry.componentOf(reflect.TypeFor[*TestComponent]())
	s.True(ok)

	s.Equal(rc.componentID, archetypeID)
}

func (s *componentRefSuite) TestNewComponentRefByArchetypeID() {
	executionKey := ExecutionKey{
		NamespaceID: primitives.NewUUID().String(),
		BusinessID:  primitives.NewUUID().String(),
		RunID:       primitives.NewUUID().String(),
	}
	expectArchetypeID := WorkflowArchetypeID
	ref := NewComponentRefByArchetypeID(executionKey, expectArchetypeID)

	s.Equal(executionKey, ref.ExecutionKey)

	archetypeID, err := ref.ArchetypeID(s.registry)
	s.NoError(err)
	s.Equal(expectArchetypeID, archetypeID)
}

func (s *componentRefSuite) TestSerializeDeserialize() {
	_, err := DeserializeComponentRef(nil)
	s.ErrorIs(err, ErrInvalidComponentRef)
	_, err = DeserializeComponentRef([]byte{})
	s.ErrorIs(err, ErrInvalidComponentRef)

	executionKey := ExecutionKey{
		NamespaceID: primitives.NewUUID().String(),
		BusinessID:  primitives.NewUUID().String(),
		RunID:       primitives.NewUUID().String(),
	}
	ref := ComponentRef{
		ExecutionKey:    executionKey,
		executionGoType: reflect.TypeFor[*TestComponent](),
		executionLastUpdateVT: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		componentPath: []string{primitives.NewUUID().String(), primitives.NewUUID().String()},
		componentInitialVT: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
	}

	serializedRef, err := ref.Serialize(s.registry)
	s.NoError(err)

	deserializedRef, err := DeserializeComponentRef(serializedRef)
	s.NoError(err)

	s.ProtoEqual(ref.executionLastUpdateVT, deserializedRef.executionLastUpdateVT)
	s.ProtoEqual(ref.componentInitialVT, deserializedRef.componentInitialVT)

	rootRc, ok := s.registry.ComponentFor(&TestComponent{})
	s.True(ok)
	s.Equal(rootRc.componentID, deserializedRef.archetypeID)

	s.Equal(ref.ExecutionKey, deserializedRef.ExecutionKey)
	s.Equal(ref.componentPath, deserializedRef.componentPath)
}

func (s *componentRefSuite) TestForConsistencyLevel() {
	newRef := func() ComponentRef {
		return ComponentRef{
			ExecutionKey: ExecutionKey{
				NamespaceID: primitives.NewUUID().String(),
				BusinessID:  primitives.NewUUID().String(),
				RunID:       primitives.NewUUID().String(),
			},
			archetypeID:     WorkflowArchetypeID,
			executionGoType: reflect.TypeFor[*TestComponent](),
			executionLastUpdateVT: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: rand.Int63(),
				TransitionCount:          rand.Int63(),
			},
			componentPath: []string{primitives.NewUUID().String()},
			componentInitialVT: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: rand.Int63(),
				TransitionCount:          rand.Int63(),
			},
		}
	}

	testCases := []struct {
		name  string
		level RefConsistencyLevel
		// archetypeID overrides the ref's archetype; the zero value keeps newRef's WorkflowArchetypeID.
		archetypeID ArchetypeID
		// wantErr, when set, is the error forConsistencyLevel must return (verify is then skipped).
		wantErr error
		// verify asserts the level-specific expectations on the original (orig) and adjusted refs.
		verify func(orig, adjusted ComponentRef)
	}{
		{
			name:  "ExecutionLastUpdate keeps everything",
			level: RefConsistencyLevelExecutionLastUpdate,
			verify: func(orig, adjusted ComponentRef) {
				// Staleness keyed off the execution's last-update transition; nothing relaxed.
				s.ProtoEqual(orig.executionLastUpdateVT, adjusted.executionLastUpdateVT)
				s.ProtoEqual(orig.componentInitialVT, adjusted.componentInitialVT)
				s.Equal(orig.RunID, adjusted.RunID)
			},
		},
		{
			name:  "ComponentCreation keys staleness off the component creation VT",
			level: RefConsistencyLevelComponentCreation,
			verify: func(orig, adjusted ComponentRef) {
				// The staleness check (Node.IsStale keys off executionLastUpdateVT) now uses the
				// component's creation transition, so a behind mutable state is still reloaded.
				s.ProtoEqual(orig.componentInitialVT, adjusted.executionLastUpdateVT)
				// The creation transition is preserved for the component-identity check, run ID kept.
				s.ProtoEqual(orig.componentInitialVT, adjusted.componentInitialVT)
				s.Equal(orig.RunID, adjusted.RunID)
			},
		},
		{
			name:  "CurrentRun drops both VTs and the run ID",
			level: RefConsistencyLevelCurrentRun,
			verify: func(orig, adjusted ComponentRef) {
				s.Nil(adjusted.executionLastUpdateVT)
				s.Nil(adjusted.componentInitialVT)
				s.Empty(adjusted.RunID)
			},
		},
		{
			// CurrentRun only makes sense for workflow executions (they have a run chain), so it is
			// rejected for other archetypes.
			name:        "CurrentRun is rejected for a non-workflow archetype",
			level:       RefConsistencyLevelCurrentRun,
			archetypeID: WorkflowArchetypeID + 1,
			wantErr:     ErrInvalidRefConsistencyLevel,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			ref := newRef()
			if tc.archetypeID != UnspecifiedArchetypeID {
				ref.archetypeID = tc.archetypeID
			}
			adjusted, err := ref.forConsistencyLevel(tc.level)
			if tc.wantErr != nil {
				s.ErrorIs(err, tc.wantErr)
				return
			}
			s.NoError(err)
			tc.verify(ref, adjusted)

			// Invariants for every level: path/archetype identity is preserved, and the receiver
			// is never mutated (value semantics).
			s.Equal(ref.componentPath, adjusted.componentPath)
			s.Equal(ref.archetypeID, adjusted.archetypeID)
			s.NotNil(ref.executionLastUpdateVT)
			s.NotNil(ref.componentInitialVT)
			s.NotEmpty(ref.RunID)
		})
	}
}
