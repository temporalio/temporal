//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination library_mock.go

package chasm

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"google.golang.org/grpc"
)

type (
	Library interface {
		Name() string
		Components() []*RegistrableComponent
		Tasks() []*RegistrableTask
		RegisterServices(server *grpc.Server)
		// NexusServices returns a list of nexus.Service instances to register with the __temporal_system Nexus endpoint.
		NexusServices() []*nexus.Service
		// NexusServiceProcessors returns a list of NexusServiceProcessor instances to register with the __temporal_system
		// Nexus endpoint.
		NexusServiceProcessors() []*NexusServiceProcessor

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

func (UnimplementedLibrary) NexusServices() []*nexus.Service {
	return nil
}

func (UnimplementedLibrary) NexusServiceProcessors() []*NexusServiceProcessor {
	return nil
}

func (UnimplementedLibrary) mustEmbedUnimplementedLibrary() {}

// FullyQualifiedName creates a fully qualified name (FQN) by combining a library name
// and a component or task name. The FQN is used to uniquely identify components and
// tasks within the CHASM framework.
// The format of the returned FQN is: "libName.name"
func FullyQualifiedName(libName, name string) string {
	return libName + "." + name
}
