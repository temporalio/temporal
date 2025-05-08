//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination library_mock.go

package chasm

type (
	Library interface {
		Name() string
		Components() []*RegistrableComponent
		Tasks() []*RegistrableTask
		// Service()

		mustEmbedUnimplementedLibrary()
	}

	UnimplementedLibrary struct{}

	namer interface {
		Name() string
	}
)

func (UnimplementedLibrary) Components() []*RegistrableComponent {
	return nil
}

func (UnimplementedLibrary) Tasks() []*RegistrableTask {
	return nil
}

func (UnimplementedLibrary) mustEmbedUnimplementedLibrary() {}

func fullyQualifiedName(libName, name string) string {
	return libName + "." + name
}
