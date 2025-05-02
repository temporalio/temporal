package tdbg

import (
	"time"
)

const (
	DefaultFrontendAddress = "127.0.0.1:7233"

	// regex expression for parsing time durations, shorter, longer notations and numeric value respectively
	defaultDateTimeRangeShortRE = "^[1-9][0-9]*[smhdwMy]$"                                // eg. 1s, 20m, 300h etc.
	defaultDateTimeRangeLongRE  = "^[1-9][0-9]*(second|minute|hour|day|week|month|year)$" // eg. 1second, 20minute, 300hour etc.
	defaultDateTimeRangeNum     = "^[1-9][0-9]*"                                          // eg. 1, 20, 300 etc.

	// time ranges
	day   = 24 * time.Hour
	week  = 7 * day
	month = 30 * day
	year  = 365 * day

	defaultTimeFormat              = "15:04:05"   // used for converting UnixNano to string like 16:16:36 (only time)
	defaultDateTimeFormat          = time.RFC3339 // used for converting UnixNano to string like 2018-02-15T16:16:36-08:00
	defaultContextTimeoutInSeconds = 5
	defaultContextTimeout          = defaultContextTimeoutInSeconds * time.Second

	showErrorStackEnv = `TEMPORAL_CLI_SHOW_STACKS`
)
