package serviceerror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func TestFromToStatus(t *testing.T) {
	err := &ShardOwnershipLost{
		Message:     "mess",
		OwnerHost:   "owner",
		CurrentHost: "current",
	}

	st := serviceerror.ToStatus(err)
	err1 := FromStatus(st)
	var solErr *ShardOwnershipLost
	if !errors.As(err1, &solErr) {
		assert.Fail(t, "Returned error is not of type *ShardOwnershipLost")
	}
	assert.Equal(t, err.Message, solErr.Message)
	assert.Equal(t, err.OwnerHost, solErr.OwnerHost)
}

func TestAbortedByServerRoundTrip(t *testing.T) {
	original := NewAbortedByServer("workflow update was aborted")
	st := serviceerror.ToStatus(original)
	converted := FromStatus(st)
	var result *AbortedByServer
	require.ErrorAs(t, converted, &result)
	require.Equal(t, original.Error(), result.Error())
}
