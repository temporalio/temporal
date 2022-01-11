package visibility

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	managerSelector interface {
		readManager(namespace namespace.Name) manager.VisibilityManager
		writeManagers() ([]manager.VisibilityManager, error)
	}

	sqlToESManagerSelector struct {
		enableAdvancedVisibilityRead  dynamicconfig.BoolPropertyFnWithNamespaceFilter
		advancedVisibilityWritingMode dynamicconfig.StringPropertyFn
		stdVisibilityManager          manager.VisibilityManager
		advVisibilityManager          manager.VisibilityManager
	}

	esManagerSelector struct {
		enableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
		enableWriteToSecondaryVisibility  dynamicconfig.BoolPropertyFn
		visibilityManager                 manager.VisibilityManager
		secondaryVisibilityManager        manager.VisibilityManager
	}
)

var _ managerSelector = (*sqlToESManagerSelector)(nil)
var _ managerSelector = (*esManagerSelector)(nil)

func NewSQLToESManagerSelector(
	stdVisibilityManager manager.VisibilityManager,
	advVisibilityManager manager.VisibilityManager,
	enableAdvancedVisibilityRead dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	advancedVisibilityWritingMode dynamicconfig.StringPropertyFn,
) *sqlToESManagerSelector {
	return &sqlToESManagerSelector{
		stdVisibilityManager:          stdVisibilityManager,
		advVisibilityManager:          advVisibilityManager,
		enableAdvancedVisibilityRead:  enableAdvancedVisibilityRead,
		advancedVisibilityWritingMode: advancedVisibilityWritingMode,
	}
}

func NewESManagerSelector(
	visibilityManager manager.VisibilityManager,
	secondaryVisibilityManager manager.VisibilityManager,
	enableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	enableWriteToSecondaryVisibility dynamicconfig.BoolPropertyFn,
) *esManagerSelector {
	return &esManagerSelector{
		visibilityManager:                 visibilityManager,
		secondaryVisibilityManager:        secondaryVisibilityManager,
		enableReadFromSecondaryVisibility: enableReadFromSecondaryVisibility,
		enableWriteToSecondaryVisibility:  enableWriteToSecondaryVisibility,
	}
}

func (v *sqlToESManagerSelector) writeManagers() ([]manager.VisibilityManager, error) {
	switch v.advancedVisibilityWritingMode() {
	case AdvancedVisibilityWritingModeOff:
		return []manager.VisibilityManager{v.stdVisibilityManager}, nil
	case AdvancedVisibilityWritingModeOn:
		return []manager.VisibilityManager{v.advVisibilityManager}, nil
	case AdvancedVisibilityWritingModeDual:
		return []manager.VisibilityManager{v.stdVisibilityManager, v.advVisibilityManager}, nil
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown advanced visibility writing mode: %s", v.advancedVisibilityWritingMode()))
	}
}

func (v *sqlToESManagerSelector) readManager(namespace namespace.Name) manager.VisibilityManager {
	if v.enableAdvancedVisibilityRead(namespace.String()) {
		return v.advVisibilityManager
	}
	return v.stdVisibilityManager
}

func (v *esManagerSelector) writeManagers() ([]manager.VisibilityManager, error) {
	managers := []manager.VisibilityManager{v.visibilityManager}
	if v.enableWriteToSecondaryVisibility() {
		managers = append(managers, v.secondaryVisibilityManager)
	}

	return managers, nil
}

func (v *esManagerSelector) readManager(namespace namespace.Name) manager.VisibilityManager {
	if v.enableReadFromSecondaryVisibility(namespace.String()) {
		return v.secondaryVisibilityManager
	}
	return v.visibilityManager
}
