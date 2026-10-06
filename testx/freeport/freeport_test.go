package freeport

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMustGetFreePort(t *testing.T) {
	port := MustGetFreePort()

	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	require.NoError(t, err)
	require.NoError(t, l.Close())
}
