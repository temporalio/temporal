package fakedata_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/fakedata"
)

type Failure struct {
	Cause   *Failure
	Message string
}

func TestCircularReference(t *testing.T) {
	var failure Failure
	err := fakedata.FakeStruct(&failure)
	require.NoError(t, err, "should not fail to generate fake data for struct with circular reference")
	assert.NotEmpty(t, failure.Message)
	require.NotNil(t, failure.Cause, "should fake data for reference to field of same type")
	assert.NotEmpty(t, failure.Cause.Message)
	require.Nil(t, failure.Cause.Cause, "should avoid infinite recursion")
}
