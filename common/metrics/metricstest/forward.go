// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/metricstest.
package metricstest

import "go.temporal.io/testx/metricstest"

type Capture = metricstest.Capture[CapturedRecording]
type CaptureSnapshot = metricstest.CaptureSnapshot[CapturedRecording]
