package visibility

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination manager_selector_mock.go

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
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
	}
)

var _ managerSelector = (*defaultManagerSelector)(nil)

func newDefaultManagerSelector(
	visibilityManager manager.VisibilityManager,
	secondaryVisibilityManager manager.VisibilityManager,
	enableSecondaryVisibilityRead dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	secondaryVisibilityWritingMode dynamicconfig.StringPropertyFn,
) *defaultManagerSelector {
	return &defaultManagerSelector{
		visibilityManager:                 visibilityManager,
		secondaryVisibilityManager:        secondaryVisibilityManager,
		enableReadFromSecondaryVisibility: enableSecondaryVisibilityRead,
		secondaryVisibilityWritingMode:    secondaryVisibilityWritingMode,
	}
}

func (v *defaultManagerSelector) writeManagers() ([]manager.VisibilityManager, error) {
	switch v.secondaryVisibilityWritingMode() {
	case SecondaryVisibilityWritingModeOff:
		return []manager.VisibilityManager{v.visibilityManager}, nil
	case SecondaryVisibilityWritingModeOn:
		return []manager.VisibilityManager{v.secondaryVisibilityManager}, nil
	case SecondaryVisibilityWritingModeDual:
		return []manager.VisibilityManager{v.visibilityManager, v.secondaryVisibilityManager}, nil
	default:
		return nil, serviceerror.NewInternalf(
			"Unknown secondary visibility writing mode: %s",
			v.secondaryVisibilityWritingMode(),
		)
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
