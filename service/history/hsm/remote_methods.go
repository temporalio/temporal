package hsm

// RemoteMethod can be defined for each state machine to handle external request, like RPCs, but as part of the HSM
// framework. See RemoteExecutor for how to define the handler for remote methods.
type RemoteMethod interface {
	// Name of the remote method. Must be unique per state machine.
	Name() string
	// SerializeOutput serializes output of the invocation to a byte array that is suitable for transport.
	SerializeOutput(output any) ([]byte, error)
	// DeserializeInput deserializes input from bytes that is then passed to the handler.
	DeserializeInput(data []byte) (any, error)
}

type remoteMethodDefinition struct {
	method   RemoteMethod
	executor RemoteExecutor
}
