package testcore

import "time"

const (
	DefaultPageSize   = 5
	PprofTestPort     = 7000
	TlsCertCommonName = "my-common-name"
	ClientSuiteLimit  = 10
	// TODO (alex): replace all sleeps with WaitForESToSettle with s.Eventually()
	WaitForESToSettle = 4 * time.Second // wait es shards for some time ensure data consistent
)
