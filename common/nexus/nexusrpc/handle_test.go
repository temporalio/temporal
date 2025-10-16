package nexusrpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewHandleFailureConditions(t *testing.T) {
	client, err := NewHTTPClient(HTTPClientOptions{BaseURL: "http://foo.com", Service: "test"})
	require.NoError(t, err)
	_, err = client.NewOperationHandle("", "token")
	require.ErrorIs(t, err, errEmptyOperationName)
	_, err = client.NewOperationHandle("name", "")
	require.ErrorIs(t, err, errEmptyOperationToken)
	_, err = client.NewOperationHandle("", "")
	require.ErrorIs(t, err, errEmptyOperationName)
	require.ErrorIs(t, err, errEmptyOperationToken)
}
