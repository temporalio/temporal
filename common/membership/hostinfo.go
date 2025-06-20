package membership

// HostInfo represents the host of a Temporal service.
type HostInfo interface {
	// Identity returns the unique identifier of the host.
	// This may be the same as the address.
	Identity() string
	// GetAddress returns the socket address of the host (i.e. <ip>:<port>).
	// This must be a valid gRPC address.
	GetAddress() string
}

// NewHostInfoFromAddress creates a new HostInfo instance from a socket address.
func NewHostInfoFromAddress(address string) HostInfo {
	return hostAddress(address)
}

// hostAddress is a HostInfo implementation that uses a string as the address and identity.
type hostAddress string

// GetAddress returns the value of the hostAddress.
func (a hostAddress) GetAddress() string {
	return string(a)
}

// Identity returns the value of the hostAddress.
func (a hostAddress) Identity() string {
	return string(a)
}
