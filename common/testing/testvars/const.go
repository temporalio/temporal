package testvars

type Global struct {
}

func newGlobal() Global {
	return Global{}
}

func (c Global) ClusterName() string {
	return "active"
}

func (c Global) RemoteClusterName() string {
	return "standby"
}
