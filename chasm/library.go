//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination library_mock.go

package chasm

import "google.golang.org/grpc"

type (
	Library interface {
		Name() string
		Components() []*RegistrableComponent
		Tasks() []*RegistrableTask
		RegisterServices(server *grpc.Server)

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

// RegisterServices Registers the gRPC calls to the handlers of the library.
func (UnimplementedLibrary) RegisterServices(_ *grpc.Server) {
}

func (UnimplementedLibrary) mustEmbedUnimplementedLibrary() {}

func fullyQualifiedName(libName, name string) string {
	return libName + "." + name
}
