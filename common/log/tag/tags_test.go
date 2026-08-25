package tag

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func TestErrorType(t *testing.T) {
	testData := []struct {
		err            error
		expectedResult string
	}{
		{serviceerror.NewInvalidArgument(""), "serviceerror.InvalidArgument"},
		{errors.New("test"), "errors.errorString"},
		{fmt.Errorf("test"), "errors.errorString"},
	}

	for id, data := range testData {
		require.Equal(t, data.expectedResult, ServiceErrorType(data.err).Value().(string), "Unexpected error type in index %d", id)
	}
}

func TestNextPageToken(t *testing.T) {
	nextPageToken := []byte("next-page-token")

	actual := NextPageToken(nextPageToken)

	require.Equal(t, "next-page-token", actual.Key())
	require.Equal(t, nextPageToken, actual.Value())
}

func TestPageSize(t *testing.T) {
	actual := PageSize(42)

	require.Equal(t, "page-size", actual.Key())
	require.Equal(t, int64(42), actual.Value())
}
