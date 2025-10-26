package queues

type (
	// Action is a set of operations that can be run on a ReaderGroup.
	// It is created and run by Mitigator upon receiving an Alert.
	Action interface {
		Name() string
		Run(*ReaderGroup) (actionTaken bool)
	}
)
