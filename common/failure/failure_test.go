package failure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	failurepb "go.temporal.io/api/failure/v1"
)

func TestTruncate(t *testing.T) {
	assert := assert.New(t)

	f := failurepb.Failure_builder{
		Source:  "GoSDK",
		Message: "very long message",
		ServerFailureInfo: failurepb.ServerFailureInfo_builder{
			NonRetryable: true,
		}.Build(),
	}.Build()

	maxSize := 18
	newFailure := Truncate(f, maxSize)
	assert.LessOrEqual(newFailure.Size(), maxSize)
	assert.Len(newFailure.GetMessage(), 5)
	assert.Equal(f.GetSource(), newFailure.GetSource())
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	f.SetStackTrace("some stack trace")
	maxSize = len(f.GetMessage()) + len(f.GetSource()) + 19
	newFailure = Truncate(f, maxSize)
	assert.LessOrEqual(newFailure.Size(), maxSize)
	assert.Equal(f.GetMessage(), newFailure.GetMessage())
	assert.Equal(f.GetSource(), newFailure.GetSource())
	assert.Equal("some st", newFailure.GetStackTrace())
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	newFailure = Truncate(f, 1000)
	assert.Equal(f.GetMessage(), newFailure.GetMessage())
	assert.Equal(f.GetSource(), newFailure.GetSource())
	assert.Equal(f.GetStackTrace(), newFailure.GetStackTrace())
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())
}

func TestTruncateUTF8(t *testing.T) {
	f := failurepb.Failure_builder{
		Source:  "test",
		Message: "\u2299 very \u229a long \u25cd message",
		ServerFailureInfo: failurepb.ServerFailureInfo_builder{
			NonRetryable: true,
		}.Build(),
	}.Build()
	assert.Equal(t, "\u2299 very \u229a", Truncate(f, 24).GetMessage())
	assert.Equal(t, "\u2299 very ", Truncate(f, 23).GetMessage())
}

func TestTruncateDepth(t *testing.T) {
	f := failurepb.Failure_builder{
		Message: "level 1",
		Cause: failurepb.Failure_builder{
			Message: "level 2",
			Cause: failurepb.Failure_builder{
				Message: "level 3",
				Cause: failurepb.Failure_builder{
					Message: "level 4",
					Cause: failurepb.Failure_builder{
						Message: "level 5",
					}.Build(),
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	trunc := TruncateWithDepth(f, 1000, 3)
	assert.Nil(t, trunc.GetCause().GetCause().GetCause().GetCause())

	trunc = TruncateWithDepth(f, 33, 100)
	assert.LessOrEqual(t, trunc.Size(), 33)
	assert.Nil(t, trunc.GetCause().GetCause().GetCause())
	assert.Equal(t, "lev", trunc.GetCause().GetCause().GetMessage())

	trunc = TruncateWithDepth(f, 30, 100)
	assert.LessOrEqual(t, trunc.Size(), 30)
	assert.Nil(t, trunc.GetCause().GetCause())
}
