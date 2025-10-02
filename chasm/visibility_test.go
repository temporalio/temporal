package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.uber.org/mock/gomock"
)

type (
	visibilitySuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
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
	s.controller = gomock.NewController(s.T())
	s.mockContext = NewMockContext(s.controller)
	s.mockMutableContext = NewMockMutableContext(s.controller)

	s.registry = NewRegistry(log.NewTestLogger())
	err := s.registry.Register(&CoreLibrary{})
	s.NoError(err)

	s.mockMutableContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	s.visibility = NewVisibility(s.mockMutableContext)
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
	s.Equal(visibilityComponentFqType, rc.FqType())
}

func (s *visibilitySuite) TestTaskFqType() {
	rc, ok := s.registry.TaskFor(&persistencespb.ChasmVisibilityTaskData{})
	s.True(ok)
	s.Equal(visibilityTaskFqType, rc.FqType())
}

func (s *visibilitySuite) TestLifeCycleState() {
	s.Equal(LifecycleStateRunning, s.visibility.LifecycleState(s.mockMutableContext))
}

func (s *visibilitySuite) TestSearchAttributes() {
	sa, err := s.visibility.GetSearchAttributes(s.mockMutableContext)
	s.NoError(err)
	s.Empty(sa)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add SA via Visibility struct method.
	s.mockMutableContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}).Times(1)
	err = s.visibility.SetSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    s.mustEncode(intVal),
			floatKey:  s.mustEncode(floatVal),
		},
	)
	s.NoError(err)

	sa, err = s.visibility.GetSearchAttributes(s.mockMutableContext)
	s.NoError(err)
	s.Len(sa, 3)

	var actualStringVal string
	err = payload.Decode(sa[stringKey], &actualStringVal)
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
	s.mockMutableContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}).Times(1)
	err = s.visibility.SetSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		intKey:   s.mustEncode(intVal),
		floatKey: nil,
	})
	s.NoError(err)
	sa, err = s.visibility.GetSearchAttributes(s.mockMutableContext)
	s.NoError(err)
	s.Len(sa, 2, "intKey and stringKey should remain")
}

func (s *visibilitySuite) TestMemo() {
	memo, err := s.visibility.GetMemo(s.mockMutableContext)
	s.NoError(err)
	s.Empty(memo)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add memo via Visibility struct method.
	s.mockMutableContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}).Times(1)
	err = s.visibility.SetMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		stringKey: s.mustEncode(stringVal),
		intKey:    s.mustEncode(intVal),
		floatKey:  s.mustEncode(floatVal),
	})
	s.NoError(err)

	memo, err = s.visibility.GetMemo(s.mockMutableContext)
	s.NoError(err)
	s.Len(memo, 3)

	var actualStringVal string
	err = payload.Decode(memo[stringKey], &actualStringVal)
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
	s.mockMutableContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}).Times(1)
	err = s.visibility.SetMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		intKey:   s.mustEncode(intVal),
		floatKey: nil,
	})
	s.NoError(err)
	memo, err = s.visibility.GetMemo(s.mockMutableContext)
	s.NoError(err)
	s.Len(memo, 2, "intKey and stringKey should remain")
}

func (s *visibilitySuite) TestTaskValidator() {
	task := &persistencespb.ChasmVisibilityTaskData{
		TransitionCount: 3,
	}

	s.visibility.Data.TransitionCount = 1
	valid, err := defaultVisibilityTaskHandler.Validate(s.mockMutableContext, s.visibility, TaskAttributes{}, task)
	s.NoError(err)
	s.False(valid)

	s.visibility.Data.TransitionCount = task.TransitionCount
	valid, err = defaultVisibilityTaskHandler.Validate(s.mockMutableContext, s.visibility, TaskAttributes{}, task)
	s.NoError(err)
	s.True(valid)
}

func (s *visibilitySuite) mustEncode(v any) *commonpb.Payload {
	p, err := payload.Encode(v)
	s.NoError(err)
	return p
}
