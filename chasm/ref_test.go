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

func (s *componentRefSuite) TestShardingKey() {
	executionKey := ExecutionKey{
		NamespaceID: primitives.NewUUID().String(),
		BusinessID:  primitives.NewUUID().String(),
		RunID:       primitives.NewUUID().String(),
	}
	ref := NewComponentRef[*TestComponent](executionKey)

	shardingKey, err := ref.ShardingKey(s.registry)
	s.NoError(err)

	rc, ok := s.registry.componentOf(reflect.TypeFor[*TestComponent]())
	s.True(ok)

	s.Equal(rc.shardingFn(executionKey), shardingKey)
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
