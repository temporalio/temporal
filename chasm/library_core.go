package chasm

// CoreLibrary contains built-in components maintained as part of the CHASM framework.
type CoreLibrary struct {
	UnimplementedLibrary
}

func (b *CoreLibrary) Name() string {
	return "core"
}

func (b *CoreLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
		NewRegistrableComponent[*Visibility]("vis"),
	}
}

func (b *CoreLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{
		NewRegistrableSideEffectTask(
			"visTask",
			defaultVisibilityTaskHandler,
			defaultVisibilityTaskHandler,
		),
	}
}
