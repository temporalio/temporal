package serviceerror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
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
