package membership

// HostInfo is a type that contains the info about a cadence host
type HostInfo struct {
	addr   string // ip:port
	labels map[string]string
}

// NewHostInfo creates a new HostInfo instance
func NewHostInfo(addr string, labels map[string]string) *HostInfo {
	if labels == nil {
		labels = make(map[string]string)
	}
	return &HostInfo{
		addr:   addr,
		labels: labels,
	}
}

// GetAddress returns the ip:port address
func (hi *HostInfo) GetAddress() string {
	return hi.addr
}

// Identity implements ringpop's Membership interface
func (hi *HostInfo) Identity() string {
	// for now we just use the address as the identity
	return hi.addr
}

// Label implements ringpop's Membership interface
func (hi *HostInfo) Label(key string) (value string, has bool) {
	value, has = hi.labels[key]
	return
}

// SetLabel sets the label.
func (hi *HostInfo) SetLabel(key string, value string) {
	hi.labels[key] = value
}
