package failure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	failurepb "go.temporal.io/api/failure/v1"
)

func TestTruncate(t *testing.T) {
	assert := assert.New(t)

	f := &failurepb.Failure{
		Source:  "GoSDK",
		Message: "very long message",
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: true,
		}},
	}

	maxSize := 18
	newFailure := Truncate(f, maxSize)
	assert.LessOrEqual(newFailure.Size(), maxSize)
	assert.Len(newFailure.Message, 5)
	assert.Equal(f.Source, newFailure.Source)
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	f.StackTrace = "some stack trace"
	maxSize = len(f.Message) + len(f.Source) + 19
	newFailure = Truncate(f, maxSize)
	assert.LessOrEqual(newFailure.Size(), maxSize)
	assert.Equal(f.Message, newFailure.Message)
	assert.Equal(f.Source, newFailure.Source)
	assert.Equal("some st", newFailure.StackTrace)
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	newFailure = Truncate(f, 1000)
	assert.Equal(f.Message, newFailure.Message)
	assert.Equal(f.Source, newFailure.Source)
	assert.Equal(f.StackTrace, newFailure.StackTrace)
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())
}

func TestTruncateUTF8(t *testing.T) {
	f := &failurepb.Failure{
		Source:  "test",
		Message: "\u2299 very \u229a long \u25cd message",
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: true,
		}},
	}
	assert.Equal(t, "\u2299 very \u229a", Truncate(f, 24).Message)
	assert.Equal(t, "\u2299 very ", Truncate(f, 23).Message)
}

func TestTruncateDepth(t *testing.T) {
	f := &failurepb.Failure{
		Message: "level 1",
		Cause: &failurepb.Failure{
			Message: "level 2",
			Cause: &failurepb.Failure{
				Message: "level 3",
				Cause: &failurepb.Failure{
					Message: "level 4",
					Cause: &failurepb.Failure{
						Message: "level 5",
					},
				},
			},
		},
	}

	trunc := TruncateWithDepth(f, 1000, 3)
	assert.Nil(t, trunc.Cause.Cause.Cause.Cause)

	trunc = TruncateWithDepth(f, 33, 100)
	assert.LessOrEqual(t, trunc.Size(), 33)
	assert.Nil(t, trunc.Cause.Cause.Cause)
	assert.Equal(t, "lev", trunc.Cause.Cause.Message)

	trunc = TruncateWithDepth(f, 30, 100)
	assert.LessOrEqual(t, trunc.Size(), 30)
	assert.Nil(t, trunc.Cause.Cause)
}
