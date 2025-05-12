package testcore

import "time"

const (
	DefaultPageSize   = 5
	PprofTestPort     = 7000
	TlsCertCommonName = "my-common-name"
	ClientSuiteLimit  = 10
	WaitForESToSettle = 4 * time.Second // wait es shards for some time ensure data consistent

)
