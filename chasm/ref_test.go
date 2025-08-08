package chasm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
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

func (s *componentRefSuite) TestArchetype() {
	tv := testvars.New(s.T())
	entityKey := EntityKey{
		tv.NamespaceID().String(),
		tv.WorkflowID(),
		tv.RunID(),
	}
	ref := NewComponentRef[*TestComponent](entityKey)

	archetype, err := ref.Archetype(s.registry)
	s.NoError(err)

	rc, ok := s.registry.ComponentOf(reflect.TypeFor[*TestComponent]())
	s.True(ok)

	s.Equal(rc.FqType(), archetype.String())
}

func (s *componentRefSuite) TestShardingKey() {
	tv := testvars.New(s.T())
	entityKey := EntityKey{
		tv.NamespaceID().String(),
		tv.WorkflowID(),
		tv.RunID(),
	}
	ref := NewComponentRef[*TestComponent](entityKey)

	shardingKey, err := ref.ShardingKey(s.registry)
	s.NoError(err)

	rc, ok := s.registry.ComponentOf(reflect.TypeFor[*TestComponent]())
	s.True(ok)

	s.Equal(rc.shardingFn(entityKey), shardingKey)
}

func (s *componentRefSuite) TestSerializeDeserialize() {
	tv := testvars.New(s.T())
	entityKey := EntityKey{
		tv.NamespaceID().String(),
		tv.WorkflowID(),
		tv.RunID(),
	}
	ref := ComponentRef{
		EntityKey:    entityKey,
		entityGoType: reflect.TypeFor[*TestComponent](),
		entityLastUpdateVT: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: tv.Namespace().FailoverVersion(),
			TransitionCount:          tv.Any().Int64(),
		},
		componentPath: []string{tv.Any().String(), tv.Any().String()},
		componentInitialVT: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: tv.Namespace().FailoverVersion(),
			TransitionCount:          tv.Any().Int64(),
		},
	}

	serializedRef, err := ref.Serialize(s.registry)
	s.NoError(err)

	deserializedRef, err := DeserializeComponentRef(serializedRef)
	s.NoError(err)

	s.ProtoEqual(ref.entityLastUpdateVT, deserializedRef.entityLastUpdateVT)
	s.ProtoEqual(ref.componentInitialVT, deserializedRef.componentInitialVT)

	rootRc, ok := s.registry.ComponentFor(&TestComponent{})
	s.True(ok)
	s.Equal(rootRc.FqType(), deserializedRef.archetype.String())

	s.Equal(ref.EntityKey, deserializedRef.EntityKey)
	s.Equal(ref.componentPath, deserializedRef.componentPath)
}
