package nexusrpc

import (
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
)

func TestFailureConverter_GenericError(t *testing.T) {
	failure, err := defaultFailureConverter.ErrorToFailure(errors.New("test"))
	require.NoError(t, err)
	actual, err := defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	require.Equal(t, &nexus.FailureError{Failure: nexus.Failure{Message: "test"}}, actual)
}

func TestFailureConverter_FailureError(t *testing.T) {
	cause := &nexus.FailureError{
		Failure: nexus.Failure{Message: "cause"},
	}
	fe := &nexus.FailureError{
		Failure: nexus.Failure{
			Message: "foo",
		},
		Cause: cause,
	}
	failure, err := defaultFailureConverter.ErrorToFailure(fe)
	require.NoError(t, err)
	actual, err := defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	// The serialized failure cause is retained.
	fe.Failure.Cause = failure.Cause
	require.Equal(t, fe, actual)

	// Serialize again and verify the original failure is used.
	fe.Cause = errors.New("should be ignored")
	failure, err = defaultFailureConverter.ErrorToFailure(fe)
	require.NoError(t, err)
	actual, err = defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	// Reset back to the orignal cause before comparing.
	fe.Cause = cause
	require.Equal(t, fe, actual)
}

func TestFailureConverter_HandlerError(t *testing.T) {
	cause := &nexus.FailureError{Failure: nexus.Failure{Message: "cause"}}
	he := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "foo")
	he.StackTrace = "stack"
	he.Cause = cause
	failure, err := defaultFailureConverter.ErrorToFailure(he)
	require.NoError(t, err)
	// Verify that the original failure is retained.
	he.OriginalFailure = &failure
	actual, err := defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	require.Equal(t, he, actual)

	// Serialize again and verify the original failure is used.
	he.Cause = errors.New("should be ignored")
	failure, err = defaultFailureConverter.ErrorToFailure(he)
	require.NoError(t, err)
	actual, err = defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	// Reset back to the orignal cause before comparing.
	he.Cause = cause
	require.Equal(t, he, actual)
}

func TestFailureConverter_HandlerErrorRetryBehavior(t *testing.T) {
	he := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "foo")
	he.StackTrace = "stack"
	he.RetryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	failure, err := defaultFailureConverter.ErrorToFailure(he)
	require.NoError(t, err)
	// Verify that the original failure is retained.
	he.OriginalFailure = &failure
	actual, err := defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	require.Equal(t, he, actual)
}

func TestFailureConverter_OperationError(t *testing.T) {
	cause := &nexus.FailureError{Failure: nexus.Failure{Message: "cause"}}
	oe := nexus.NewOperationCanceledErrorf("foo")
	oe.StackTrace = "stack"
	oe.Cause = cause
	failure, err := defaultFailureConverter.ErrorToFailure(oe)
	require.NoError(t, err)
	// Verify that the original failure is retained.
	oe.OriginalFailure = &failure
	actual, err := defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	require.Equal(t, oe, actual)

	// Serialize again and verify the original failure is used.
	oe.Cause = errors.New("should be ignored")
	failure, err = defaultFailureConverter.ErrorToFailure(oe)
	require.NoError(t, err)
	actual, err = defaultFailureConverter.FailureToError(failure)
	require.NoError(t, err)
	// Reset back to the orignal cause before comparing.
	oe.Cause = cause
	require.Equal(t, oe, actual)
}

func TestDefaultFailureConverterArbitraryError(t *testing.T) {
	sourceErr := errors.New("test")
	conv := defaultFailureConverter

	f, err := conv.ErrorToFailure(sourceErr)
	require.NoError(t, err)
	convErr, err := conv.FailureToError(f)
	require.NoError(t, err)
	require.Equal(t, sourceErr.Error(), convErr.Error())
}

func TestDefaultFailureConverterFailureError(t *testing.T) {
	sourceErr := &nexus.FailureError{
		Failure: nexus.Failure{
			Message:  "test",
			Metadata: map[string]string{"key": "value"},
			Details:  []byte(`"details"`),
		},
	}
	conv := defaultFailureConverter

	f, err := conv.ErrorToFailure(sourceErr)
	require.NoError(t, err)
	convErr, err := conv.FailureToError(f)
	require.NoError(t, err)
	require.Equal(t, sourceErr, convErr)
}
