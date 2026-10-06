package timer

import "go.temporal.io/server/chasm"

type library struct {
	chasm.UnimplementedLibrary
}

func NewLibrary() chasm.Library {
	return &library{}
}

func (l *library) Name() string {
	return "timer"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Timer]("timer"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("fire", &fireTaskHandler{}),
	}
}
