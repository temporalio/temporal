package testcore

import "time"

const (
	DefaultPageSize   = 5
	PprofTestPort     = 7000
	TlsCertCommonName = "my-common-name"
	ClientSuiteLimit  = 10
	// 0x8f01 is invalid UTF-8
	InvalidUTF8       = "\n\x8f\x01\n\x0ejunk\x12data"
	WaitForESToSettle = 4 * time.Second // wait es shards for some time ensure data consistent

)
