package visibility

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination manager_selector_mock.go

import (
	"sync"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	managerSelector interface {
		readManager(nsName namespace.Name) manager.VisibilityManager
		readManagers(nsName namespace.Name) ([]manager.VisibilityManager, error)
		writeManagers() ([]manager.VisibilityManager, error)
	}

	defaultManagerSelector struct {
		visibilityManager                 manager.VisibilityManager
		secondaryVisibilityManager        manager.VisibilityManager
		enableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
		secondaryVisibilityWritingMode    dynamicconfig.StringPropertyFn
		logger                            log.Logger

		unrecognizedWritingModeOnce sync.Once
	}
)

var _ managerSelector = (*defaultManagerSelector)(nil)

func newDefaultManagerSelector(
	visibilityManager manager.VisibilityManager,
	secondaryVisibilityManager manager.VisibilityManager,
	enableSecondaryVisibilityRead dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	secondaryVisibilityWritingMode dynamicconfig.StringPropertyFn,
	logger log.Logger,
) *defaultManagerSelector {
	return &defaultManagerSelector{
		visibilityManager:                 visibilityManager,
		secondaryVisibilityManager:        secondaryVisibilityManager,
		enableReadFromSecondaryVisibility: enableSecondaryVisibilityRead,
		secondaryVisibilityWritingMode:    secondaryVisibilityWritingMode,
		logger:                            logger,
	}
}

func (v *defaultManagerSelector) writeManagers() ([]manager.VisibilityManager, error) {
	mode := v.secondaryVisibilityWritingMode()
	switch mode {
	case SecondaryVisibilityWritingModeOff:
		return []manager.VisibilityManager{v.visibilityManager}, nil
	case SecondaryVisibilityWritingModeOn:
		return []manager.VisibilityManager{v.secondaryVisibilityManager}, nil
	case SecondaryVisibilityWritingModeDual:
		return []manager.VisibilityManager{v.visibilityManager, v.secondaryVisibilityManager}, nil
	default:
		v.unrecognizedWritingModeOnce.Do(func() {
			v.logger.Warn(
				"Unknown secondary visibility writing mode, treating as off",
				tag.Value(mode),
			)
		})
		return []manager.VisibilityManager{v.visibilityManager}, nil
	}
}

func (v *defaultManagerSelector) readManager(nsName namespace.Name) manager.VisibilityManager {
	if v.enableReadFromSecondaryVisibility(nsName.String()) {
		return v.secondaryVisibilityManager
	}
	return v.visibilityManager
}

func (v *defaultManagerSelector) readManagers(nsName namespace.Name) ([]manager.VisibilityManager, error) {
	if v.enableReadFromSecondaryVisibility(nsName.String()) {
		return []manager.VisibilityManager{v.secondaryVisibilityManager, v.visibilityManager}, nil
	}
	return []manager.VisibilityManager{v.visibilityManager, v.secondaryVisibilityManager}, nil
}
