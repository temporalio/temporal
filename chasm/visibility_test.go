package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

type (
	visibilitySuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockChasmContext *MockMutableContext

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
	s.mockChasmContext = NewMockMutableContext(s.controller)

	s.registry = NewRegistry(log.NewTestLogger())
	err := s.registry.Register(&CoreLibrary{})
	s.NoError(err)

	s.mockChasmContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	s.visibility = NewVisibility(s.mockChasmContext)
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
	s.Equal(LifecycleStateRunning, s.visibility.LifecycleState(s.mockChasmContext))
}

func (s *visibilitySuite) TestSearchAttributes() {
	sa, err := s.visibility.GetSearchAttributes(s.mockChasmContext)
	s.NoError(err)
	s.Empty(sa)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add SA via Visibility struct method.
	s.mockChasmContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}).Times(1)
	err = s.visibility.UpsertSearchAttributes(s.mockChasmContext, map[string]any{
		stringKey: stringVal,
		intKey:    intVal,
	})
	s.NoError(err)
	s.Equal(int64(2), s.visibility.Data.TransitionCount)

	// Add SA via generic UpdateSearchAttribute helper function.
	s.mockChasmContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}).Times(1)
	UpsertSearchAttribute(s.mockChasmContext, s.visibility, floatKey, floatVal)
	s.Equal(int64(3), s.visibility.Data.TransitionCount)

	sa, err = s.visibility.GetSearchAttributes(s.mockChasmContext)
	s.NoError(err)
	s.Len(sa, 3)
	actualStringVal, err := GetSearchAttribute[string](s.mockChasmContext, s.visibility, stringKey)
	s.NoError(err)
	s.Equal(stringVal, actualStringVal)
	actualIntVal, err := GetSearchAttribute[int](s.mockChasmContext, s.visibility, intKey)
	s.NoError(err)
	s.Equal(intVal, actualIntVal)
	actualFloatVal, err := GetSearchAttribute[float64](s.mockChasmContext, s.visibility, floatKey)
	s.NoError(err)
	s.Equal(floatVal, actualFloatVal)

	s.mockChasmContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 4}).Times(1)
	s.visibility.RemoveSearchAttributes(s.mockChasmContext, stringKey)
	s.Equal(int64(4), s.visibility.Data.TransitionCount)
	sa, err = s.visibility.GetSearchAttributes(s.mockChasmContext)
	s.NoError(err)
	s.Len(sa, 2)
}

func (s *visibilitySuite) TestMemo() {
	sa, err := s.visibility.GetMemo(s.mockChasmContext)
	s.NoError(err)
	s.Empty(sa)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add memo via Visibility struct method.
	s.mockChasmContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 2}).Times(1)
	err = s.visibility.UpsertMemo(s.mockChasmContext, map[string]any{
		stringKey: stringVal,
		intKey:    intVal,
	})
	s.NoError(err)
	s.Equal(int64(2), s.visibility.Data.TransitionCount)

	// Add memo via generic UpdateSearchAttribute helper function.
	s.mockChasmContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 3}).Times(1)
	UpsertMemo(s.mockChasmContext, s.visibility, floatKey, floatVal)
	s.Equal(int64(3), s.visibility.Data.TransitionCount)

	sa, err = s.visibility.GetMemo(s.mockChasmContext)
	s.NoError(err)
	s.Len(sa, 3)
	actualStringVal, err := GetMemo[string](s.mockChasmContext, s.visibility, stringKey)
	s.NoError(err)
	s.Equal(stringVal, actualStringVal)
	actualIntVal, err := GetMemo[int](s.mockChasmContext, s.visibility, intKey)
	s.NoError(err)
	s.Equal(intVal, actualIntVal)
	actualFloatVal, err := GetMemo[float64](s.mockChasmContext, s.visibility, floatKey)
	s.NoError(err)
	s.Equal(floatVal, actualFloatVal)

	s.mockChasmContext.EXPECT().AddTask(gomock.Any(), gomock.Any(), &persistencespb.ChasmVisibilityTaskData{TransitionCount: 4}).Times(1)
	s.visibility.RemoveMemo(s.mockChasmContext, stringKey)
	s.Equal(int64(4), s.visibility.Data.TransitionCount)
	sa, err = s.visibility.GetMemo(s.mockChasmContext)
	s.NoError(err)
	s.Len(sa, 2)
}

func (s *visibilitySuite) TestTaskValidator() {
	validator := &visibilityTaskValidator{}

	task := &persistencespb.ChasmVisibilityTaskData{
		TransitionCount: 3,
	}

	s.visibility.Data.TransitionCount = 1
	valid, err := validator.Validate(s.mockChasmContext, s.visibility, TaskAttributes{}, task)
	s.NoError(err)
	s.False(valid)

	s.visibility.Data.TransitionCount = task.TransitionCount
	valid, err = validator.Validate(s.mockChasmContext, s.visibility, TaskAttributes{}, task)
	s.NoError(err)
	s.True(valid)
}
