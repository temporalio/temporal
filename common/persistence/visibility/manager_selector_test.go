package visibility

import (
	"testing"

	"github.com/stretchr/testify/require"
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
		func(string) bool { return false },
		func() string { return SecondaryVisibilityWritingModeOff },
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
		func(string) bool { return false },
		func() string { return SecondaryVisibilityWritingModeOn },
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
		func(string) bool { return false },
		func() string { return SecondaryVisibilityWritingModeDual },
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
		func(string) bool { return false },
		func() string { return "invalid" },
	)

	managers, err := s.writeManagers()
	require.Error(t, err)
	require.Nil(t, managers)
	require.Contains(t, err.Error(), "unknown secondary visibility writing mode: invalid")
}

func TestReadManager_SecondaryEnabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)

	s := newDefaultManagerSelector(
		primary,
		secondary,
		func(string) bool { return true },
		func() string { return SecondaryVisibilityWritingModeOff },
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
		func(string) bool { return false },
		func() string { return SecondaryVisibilityWritingModeOff },
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
		func(string) bool { return true },
		func() string { return SecondaryVisibilityWritingModeOff },
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
		func(string) bool { return false },
		func() string { return SecondaryVisibilityWritingModeOff },
	)

	managers, err := s.readManagers("test-ns")
	require.NoError(t, err)
	require.Equal(t, []manager.VisibilityManager{primary, secondary}, managers)
}
