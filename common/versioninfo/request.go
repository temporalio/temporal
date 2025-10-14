package versioninfo

// SDKINfo is a tuple of (SDK name, SDK version)
type SDKInfo struct {
	Name    string `json:"sdkName"`
	Version string `json:"sdkVersion"`
}

// VersionCheckRequest provides basic info about the client and is used to produce VersionCheckResponse.
type VersionCheckRequest struct {
	Product   string `json:"product"`
	Version   string `json:"version"`
	Arch      string `json:"arch"`
	OS        string `json:"os"`
	DB        string `json:"db"`
	ClusterID string `json:"cluster"`
	// Encode as string for JS compatibility
	Timestamp int64     `json:"timestamp,string"`
	SDKInfo   []SDKInfo `json:"sdkInfo"`
}
