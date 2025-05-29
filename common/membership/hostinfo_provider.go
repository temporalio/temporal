package membership

type (
	hostInfoProvider struct {
		hostInfo HostInfo
	}
)

func NewHostInfoProvider(hostInfo HostInfo) *hostInfoProvider {
	return &hostInfoProvider{
		hostInfo: hostInfo,
	}
}

func (hip *hostInfoProvider) HostInfo() HostInfo {
	return hip.hostInfo
}
