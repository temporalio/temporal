package chasm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
)

type componentRefSuite struct {
	suite.Suite
	*require.Assertions

	controller *gomock.Controller

	registry *Registry
}

func TestComponentRefSuite(t *testing.T) {
	suite.Run(t, new(componentRefSuite))
}

func (s *componentRefSuite) SetupTest() {
	// Do this in SetupSubTest() as well, if we have sub tests in this suite.
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.registry = NewRegistry()
	err := s.registry.Register(newTestLibrary(s.controller))
	s.NoError(err)
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
	s.Equal(rc.FqType(), ref.archetype)
}

func (s *componentRefSuite) TestShardID() {
	tv := testvars.New(s.T())
	entityKey := EntityKey{
		tv.NamespaceID().String(),
		tv.WorkflowID(),
		tv.RunID(),
	}
	ref := NewComponentRef[*TestComponent](entityKey)

	shardID, err := ref.ShardID(s.registry, 10)
	s.NoError(err)

	// Here we are just checking that the shardID field is populated in the ref.
	// The actual shardID value is not that important.
	s.Equal(shardID, ref.shardID)
}
