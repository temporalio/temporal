package tquserdata

import "go.temporal.io/server/chasm"

type library struct {
	chasm.UnimplementedLibrary
}

const (
	libraryName   = "tquserdata"
	componentName = "userData"
)

// Library is the CHASM library that hosts task queue user data.
var Library = &library{}

func (l *library) Name() string {
	return libraryName
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*UserData](
			componentName,
			chasm.WithBusinessIDAlias("TaskQueueName"),
			// Option A: the component is a cluster-local store. Cross-cluster
			// replication continues to run through Matching's namespace
			// replication queue, so CHASM must not replicate it as well.
			chasm.WithSingleCluster(),
		),
	}
}
