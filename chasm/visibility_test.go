package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
)

type (
	visibilitySuite struct {
		suite.Suite
		*require.Assertions

		mockContext        *MockContext
		mockMutableContext *MockMutableContext

		registry *Registry

		visibility *Visibility
	}
)

func TestVisibilitySuite(t *testing.T) {
	suite.Run(t, new(visibilitySuite))
}

func (s *visibilitySuite) SetupTest() {
	s.initAssertions()
	s.mockContext = &MockContext{}
	s.mockMutableContext = &MockMutableContext{}

	s.registry = NewRegistry(log.NewTestLogger())
	err := s.registry.Register(&CoreLibrary{})
	s.NoError(err)

	s.visibility = NewVisibility(s.mockMutableContext)
	s.Len(s.mockMutableContext.Tasks, 1)
	s.mockMutableContext.Tasks = nil // Clear tasks added during creation
}

func (s *visibilitySuite) SetupSubTest() {
	s.initAssertions()
}

func (s *visibilitySuite) initAssertions() {
	s.Assertions = require.New(s.T())
}

func (s *visibilitySuite) TestComponentFqType() {
	rc, ok := s.registry.ComponentFor(&Visibility{})
	s.True(ok)
	s.Equal(visibilityComponentType, rc.FqType())
}

func (s *visibilitySuite) TestTaskFqType() {
	rc, ok := s.registry.TaskFor(&persistencespb.ChasmVisibilityTaskData{})
	s.True(ok)
	s.Equal(visibilityTaskType, rc.FqType())
}

func (s *visibilitySuite) TestLifeCycleState() {
	s.Equal(LifecycleStateRunning, s.visibility.LifecycleState(s.mockMutableContext))
}

func (s *visibilitySuite) TestUpsertSearchAttributes() {
	sa := s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Empty(sa)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add SA via Visibility struct method.
	s.visibility.UpsertCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    s.mustEncode(intVal),
			floatKey:  s.mustEncode(floatVal),
		},
	)
	s.Len(s.mockMutableContext.Tasks, 1)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}, s.mockMutableContext.Tasks[0].Payload.(*persistencespb.ChasmVisibilityTaskData))

	sa = s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Len(sa, 3)

	var actualStringVal string
	err := payload.Decode(sa[stringKey], &actualStringVal)
	s.NoError(err)
	s.Equal(stringVal, actualStringVal)

	var actualIntVal int
	err = payload.Decode(sa[intKey], &actualIntVal)
	s.NoError(err)
	s.Equal(intVal, actualIntVal)

	var actualFloatVal float64
	err = payload.Decode(sa[floatKey], &actualFloatVal)
	s.NoError(err)
	s.Equal(floatVal, actualFloatVal)

	// Test remove search attributes by setting payload to nil.
	s.visibility.UpsertCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		intKey:   s.mustEncode(intVal),
		floatKey: nil,
	})
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 2)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}, s.mockMutableContext.Tasks[1].Payload.(*persistencespb.ChasmVisibilityTaskData))

	sa = s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.NoError(err)
	s.Len(sa, 2, "intKey and stringKey should remain")

	// Test removing all search attributes also removes the node.
	s.visibility.UpsertCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		stringKey: nil,
		intKey:    nil,
	})
	s.Len(s.mockMutableContext.Tasks, 3)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 4}, s.mockMutableContext.Tasks[2].Payload.(*persistencespb.ChasmVisibilityTaskData))
	_, ok := s.visibility.SA.TryGet(s.mockContext)
	s.False(ok)
}

func (s *visibilitySuite) TestSetSearchAttributes() {
	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14
	byteKey, byteVal := "byteKey", []byte{0x01, 0x02, 0x03}

	// Set up some initial SA.
	s.visibility.SetCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    s.mustEncode(intVal),
			floatKey:  s.mustEncode(floatVal),
		},
	)
	s.Len(s.mockMutableContext.Tasks, 1)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}, s.mockMutableContext.Tasks[0].Payload.(*persistencespb.ChasmVisibilityTaskData))

	sa := s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Len(sa, 3)

	// Set to a new set of SA, non-existing keys should be removed.
	s.visibility.SetCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			floatKey: s.mustEncode(floatVal),
			byteKey:  s.mustEncode(byteVal),
		},
	)
	s.Len(s.mockMutableContext.Tasks, 2)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}, s.mockMutableContext.Tasks[1].Payload.(*persistencespb.ChasmVisibilityTaskData))

	sa = s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Len(sa, 2)

	// Setting to an empty map should remove the node.
	s.visibility.SetCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{},
	)
	s.Len(s.mockMutableContext.Tasks, 3)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 4}, s.mockMutableContext.Tasks[2].Payload.(*persistencespb.ChasmVisibilityTaskData))
	_, ok := s.visibility.SA.TryGet(s.mockContext)
	s.False(ok)
}

func (s *visibilitySuite) TestUpsertMemo() {
	memo := s.visibility.CustomMemo(s.mockMutableContext)
	s.Empty(memo)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add memo via Visibility struct method.
	s.visibility.UpsertCustomMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		stringKey: s.mustEncode(stringVal),
		intKey:    s.mustEncode(intVal),
		floatKey:  s.mustEncode(floatVal),
	})
	s.Len(s.mockMutableContext.Tasks, 1)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}, s.mockMutableContext.Tasks[0].Payload.(*persistencespb.ChasmVisibilityTaskData))

	memo = s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 3)

	var actualStringVal string
	err := payload.Decode(memo[stringKey], &actualStringVal)
	s.NoError(err)
	s.Equal(stringVal, actualStringVal)

	var actualIntVal int
	err = payload.Decode(memo[intKey], &actualIntVal)
	s.NoError(err)
	s.Equal(intVal, actualIntVal)

	var actualFloatVal float64
	err = payload.Decode(memo[floatKey], &actualFloatVal)
	s.NoError(err)
	s.Equal(floatVal, actualFloatVal)

	// Test remove memo by setting payload to nil.
	s.visibility.UpsertCustomMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		intKey:   s.mustEncode(intVal),
		floatKey: nil,
	})
	s.Len(s.mockMutableContext.Tasks, 2)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}, s.mockMutableContext.Tasks[1].Payload.(*persistencespb.ChasmVisibilityTaskData))

	memo = s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 2, "intKey and stringKey should remain")

	// Test removing all search attributes also removes the node.
	s.visibility.UpsertCustomMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		stringKey: nil,
		intKey:    nil,
	})
	s.Len(s.mockMutableContext.Tasks, 3)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 4}, s.mockMutableContext.Tasks[2].Payload.(*persistencespb.ChasmVisibilityTaskData))
	_, ok := s.visibility.Memo.TryGet(s.mockContext)
	s.False(ok)
}

func (s *visibilitySuite) TestSetMemo() {
	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14
	byteKey, byteVal := "byteKey", []byte{0x01, 0x02, 0x03}

	// Set up some initial SA.
	s.visibility.SetMemo(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    s.mustEncode(intVal),
			floatKey:  s.mustEncode(floatVal),
		},
	)
	s.Len(s.mockMutableContext.Tasks, 1)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}, s.mockMutableContext.Tasks[0].Payload.(*persistencespb.ChasmVisibilityTaskData))

	memo := s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 3)

	// Set to a new set of SA, non-existing keys should be removed.
	s.visibility.SetMemo(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			floatKey: s.mustEncode(floatVal),
			byteKey:  s.mustEncode(byteVal),
		},
	)
	s.Len(s.mockMutableContext.Tasks, 2)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}, s.mockMutableContext.Tasks[1].Payload.(*persistencespb.ChasmVisibilityTaskData))

	memo = s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 2)

	// Setting to an empty map should remove the node.
	s.visibility.SetMemo(
		s.mockMutableContext,
		map[string]*commonpb.Payload{},
	)
	s.Len(s.mockMutableContext.Tasks, 3)
	protorequire.ProtoEqual(s.T(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 4}, s.mockMutableContext.Tasks[2].Payload.(*persistencespb.ChasmVisibilityTaskData))
	_, ok := s.visibility.Memo.TryGet(s.mockContext)
	s.False(ok)
}

func (s *visibilitySuite) mustEncode(v any) *commonpb.Payload {
	p, err := payload.Encode(v)
	s.NoError(err)
	return p
}
