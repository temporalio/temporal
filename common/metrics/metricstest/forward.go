// TODO: Remove this forwarding file once all callers use the go.temporal.io/testx/metricstest packages.
package metricstest

import (
	"go.temporal.io/testx/metricstest"
	testprometheus "go.temporal.io/testx/metricstest/prometheus"
)

type Capture = metricstest.Capture[CapturedRecording]
type CaptureSnapshot = metricstest.CaptureSnapshot[CapturedRecording]

type HistogramBucket = testprometheus.HistogramBucket

var (
	ErrMetricNotFound      = testprometheus.ErrMetricNotFound
	ErrMetricTypeMismatch  = testprometheus.ErrMetricTypeMismatch
	ErrMetricLabelMismatch = testprometheus.ErrMetricLabelMismatch
)
