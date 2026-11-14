package testlogger

import (
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger/internal/engine"
)

type CapturedLog = engine.CapturedLog[tag.Tag]
type CapturedLogPattern = engine.CapturedLogPattern[tag.Tag]
type Capture = engine.Capture[tag.Tag]

// AnyTagValue matches any formatted value for a tag that must be present.
var AnyTagValue = engine.AnyTagValue
