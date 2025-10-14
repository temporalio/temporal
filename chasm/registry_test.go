package chasm_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

type (
	RegistryTestSuite struct {
		suite.Suite
		logger log.Logger
	}

	testTask1                  struct{}
	testTask2                  struct{}
	testTaskComponentInterface interface {
		DoSomething()
	}
)

func TestRegistryTestSuite(t *testing.T) {
	suite.Run(t, new(RegistryTestSuite))
}

func (s *RegistryTestSuite) SetupTest() {
	s.logger = log.NewTestLogger()
}

func (s *RegistryTestSuite) TestRegistry_RegisterComponents_Success() {
	r := chasm.NewRegistry(s.logger)
	ctrl := gomock.NewController(s.T())
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()
	lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
	})

	lib.EXPECT().Tasks().Return(nil)

	err := r.Register(lib)
	require.NoError(s.T(), err)

	rc1, ok := r.Component("TestLibrary.Component1")
	require.True(s.T(), ok)
	require.Equal(s.T(), "TestLibrary.Component1", rc1.FqType())

	missingRC, ok := r.Component("TestLibrary.Component2")
	require.False(s.T(), ok)
	require.Nil(s.T(), missingRC)

	cInstance1 := chasm.NewMockComponent(ctrl)
	rc2, ok := r.ComponentFor(cInstance1)
	require.True(s.T(), ok)
	require.Equal(s.T(), "TestLibrary.Component1", rc2.FqType())

	rc2, ok = r.ComponentOf(reflect.TypeOf(cInstance1))
	require.True(s.T(), ok)
	require.Equal(s.T(), "TestLibrary.Component1", rc2.FqType())

	cInstance2 := "invalid component instance"
	rc3, ok := r.ComponentFor(cInstance2)
	require.False(s.T(), ok)
	require.Nil(s.T(), rc3)
}

func (s *RegistryTestSuite) TestRegistry_RegisterTasks_Success() {
	r := chasm.NewRegistry(s.logger)
	ctrl := gomock.NewController(s.T())
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()
	lib.EXPECT().Components().Return(nil)

	lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask[*chasm.MockComponent, testTask1](
			"Task1",
			chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
			chasm.NewMockSideEffectTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
		),
		chasm.NewRegistrablePureTask[testTaskComponentInterface, testTask2](
			"Task2",
			chasm.NewMockTaskValidator[testTaskComponentInterface, testTask2](ctrl),
			chasm.NewMockPureTaskExecutor[testTaskComponentInterface, testTask2](ctrl),
		),
	})

	err := r.Register(lib)
	require.NoError(s.T(), err)

	rt1, ok := r.Task("TestLibrary.Task1")
	require.True(s.T(), ok)
	require.Equal(s.T(), "TestLibrary.Task1", rt1.FqType())

	missingRT, ok := r.Task("TestLibrary.TaskMissing")
	require.False(s.T(), ok)
	require.Nil(s.T(), missingRT)

	tInstance1 := testTask2{}
	rt2, ok := r.TaskFor(tInstance1)
	require.True(s.T(), ok)
	require.Equal(s.T(), "TestLibrary.Task2", rt2.FqType())

	rt2, ok = r.TaskOf(reflect.TypeOf(tInstance1))
	require.True(s.T(), ok)
	require.Equal(s.T(), "TestLibrary.Task2", rt2.FqType())

	tInstance2 := "invalid task instance"
	rt3, ok := r.TaskFor(tInstance2)
	require.False(s.T(), ok)
	require.Nil(s.T(), rt3)
}

func (s *RegistryTestSuite) TestRegistry_Register_LibraryError() {
	ctrl := gomock.NewController(s.T())
	lib := chasm.NewMockLibrary(ctrl)

	s.T().Run("library name must not be empty", func(t *testing.T) {
		lib.EXPECT().Name().Return("")
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must not be empty")
	})

	s.T().Run("library name must follow rules", func(t *testing.T) {
		lib.EXPECT().Name().Return("bad.lib.name")
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must follow golang identifier rules")
	})
}

func (s *RegistryTestSuite) TestRegistry_RegisterComponents_Error() {
	ctrl := gomock.NewController(s.T())
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()

	s.T().Run("component name must not be empty", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent](""),
		})
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must not be empty")
	})

	s.T().Run("component name must follow rules", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent]("bad.component.name"),
		})
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must follow golang identifier rules")
	})

	s.T().Run("component is already registered by name", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
		})
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	s.T().Run("component is already registered by type", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component2"),
		})
		r := chasm.NewRegistry(s.logger)

		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	s.T().Run("component is already registered in another library", func(t *testing.T) {
		lib2 := chasm.NewMockLibrary(ctrl)
		lib2.EXPECT().Name().Return("TestLibrary2").AnyTimes()

		component := chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1")
		lib2.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			component,
		})
		lib2.EXPECT().Tasks().Return(nil)
		r2 := chasm.NewRegistry(s.logger)
		err := r2.Register(lib2)
		require.NoError(t, err)

		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			component,
		})
		r := chasm.NewRegistry(s.logger)

		err = r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered in library TestLibrary2")
	})

	s.T().Run("component must be a struct", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[chasm.Component]("Component1"),
		})
		r := chasm.NewRegistry(s.logger)

		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be struct or pointer to struct")
	})

}

func (s *RegistryTestSuite) TestRegistry_RegisterTasks_Error() {
	ctrl := gomock.NewController(s.T())
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()
	lib.EXPECT().Components().Return(nil).AnyTimes()

	s.T().Run("task name must not be empty", func(t *testing.T) {
		r := chasm.NewRegistry(s.logger)
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrablePureTask[*chasm.MockComponent, testTask1](
				"",
				chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
				chasm.NewMockPureTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
			),
		})
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must not be empty")
	})

	s.T().Run("task name must follow rules", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrablePureTask[*chasm.MockComponent, testTask1](
				"bad.task.name",
				chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
				chasm.NewMockPureTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
			),
		})
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must follow golang identifier rules")
	})

	s.T().Run("task is already registered by name", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrablePureTask[*chasm.MockComponent, testTask1](
				"Task1",
				chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
				chasm.NewMockPureTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
			),
			chasm.NewRegistrableSideEffectTask[*chasm.MockComponent, testTask1](
				"Task1",
				chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
				chasm.NewMockSideEffectTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
			),
		})
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	s.T().Run("task is already registered by type", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrablePureTask[*chasm.MockComponent, testTask1](
				"Task1",
				chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
				chasm.NewMockPureTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
			),
			chasm.NewRegistrablePureTask[*chasm.MockComponent, testTask1](
				"Task2",
				chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
				chasm.NewMockPureTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
			),
		})
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	s.T().Run("component is already registered in another library", func(t *testing.T) {
		lib2 := chasm.NewMockLibrary(ctrl)
		lib2.EXPECT().Name().Return("TestLibrary2").AnyTimes()

		lib2.EXPECT().Components().Return(nil)
		task := chasm.NewRegistrablePureTask[*chasm.MockComponent, testTask1](
			"Task1",
			chasm.NewMockTaskValidator[*chasm.MockComponent, testTask1](ctrl),
			chasm.NewMockPureTaskExecutor[*chasm.MockComponent, testTask1](ctrl),
		)
		lib2.EXPECT().Tasks().Return([]*chasm.RegistrableTask{task})
		r2 := chasm.NewRegistry(s.logger)
		err := r2.Register(lib2)
		require.NoError(t, err)

		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{task})
		r := chasm.NewRegistry(s.logger)

		err = r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered in library TestLibrary2")
	})

	s.T().Run("task must be struct", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrablePureTask[*chasm.MockComponent, string](
				"Task1",
				chasm.NewMockTaskValidator[*chasm.MockComponent, string](ctrl),
				chasm.NewMockPureTaskExecutor[*chasm.MockComponent, string](ctrl),
			),
		})
		r := chasm.NewRegistry(s.logger)
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be struct or pointer to struct")
	})
}
