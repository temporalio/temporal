// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/testlogger.
package testlogger

import (
	"go.temporal.io/server/common/log/tag"
	engine "go.temporal.io/testx/testlogger"
)

type CapturedLog = engine.CapturedLog[tag.Tag]
type CapturedLogPattern = engine.CapturedLogPattern[tag.Tag]
type Capture = engine.Capture[tag.Tag]

// AnyTagValue matches any formatted value for a tag that must be present.
var AnyTagValue = engine.AnyTagValue
