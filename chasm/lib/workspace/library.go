package workspace

import (
	"go.temporal.io/server/chasm"
)

// componentOnlyLibrary provides component registration for frontend services.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func newComponentOnlyLibrary() *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workspace](
			componentName,
			chasm.WithBusinessIDAlias("WorkspaceId"),
		),
	}
}

// library is the full workspace library for history services.
type library struct {
	componentOnlyLibrary
	handler *Handler
}

func newLibrary(h *Handler) *library {
	return &library{
		componentOnlyLibrary: componentOnlyLibrary{},
		handler:              h,
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	// No tasks in initial implementation.
	// Compaction and cleanup tasks will be added later.
	return nil
}
