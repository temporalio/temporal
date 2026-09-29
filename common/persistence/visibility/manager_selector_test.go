package visibility

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/mock/gomock"
)

func TestWriteManagers_ModeOff(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeOff),
	)

	managers, err := s.writeManagers()
	require.NoError(t, err)
	require.Equal(t, []manager.VisibilityManager{primary}, managers)
}

func TestWriteManagers_ModeOn(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeOn),
	)

	managers, err := s.writeManagers()
	require.NoError(t, err)
	require.Equal(t, []manager.VisibilityManager{secondary}, managers)
}

func TestWriteManagers_ModeDual(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeDual),
	)

	managers, err := s.writeManagers()
	require.NoError(t, err)
	require.Equal(t, []manager.VisibilityManager{primary, secondary}, managers)
}

func TestWriteManagers_UnknownMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetStringPropertyFn("invalid"),
	)

	managers, err := s.writeManagers()
	require.Error(t, err)
	require.Nil(t, managers)
	require.ErrorContains(t, err, "unknown secondary visibility writing mode: invalid")
}

func TestReadManager_SecondaryEnabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeOff),
	)

	require.Equal(t, secondary, s.readManager("test-ns"))
}

func TestReadManager_SecondaryDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeOff),
	)

	require.Equal(t, primary, s.readManager("test-ns"))
}

func TestReadManagers_SecondaryEnabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeOff),
	)

	managers, err := s.readManagers("test-ns")
	require.NoError(t, err)
	require.Equal(t, []manager.VisibilityManager{secondary, primary}, managers)
}

func TestReadManagers_SecondaryDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeOff),
	)

	managers, err := s.readManagers("test-ns")
	require.NoError(t, err)
	require.Equal(t, []manager.VisibilityManager{primary, secondary}, managers)
}
