// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/testlogger.
package testlogger

import (
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/testx/testlogger"
)

type CapturedLog = testlogger.CapturedLog[tag.Tag]
type CapturedLogPattern = testlogger.CapturedLogPattern[tag.Tag]
type Capture = testlogger.Capture[tag.Tag]

// AnyTagValue matches any formatted value for a tag that must be present.
var AnyTagValue = testlogger.AnyTagValue
