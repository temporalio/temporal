package encryption

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendError(t *testing.T) {
	assert := assert.New(t)
	err1 := errors.New("error1")
	err2 := errors.New("error2")

	err := appendError(nil, err1)
	assert.Equal(err1, err)
	assert.Equal("error1", err.Error())
	err = appendError(err1, nil)
	assert.Equal(err1, err)
	assert.Equal("error1", err.Error())

	err = appendError(err1, err2)
	assert.Equal(err2, errors.Unwrap(err))
	assert.Equal("error1, error2", err.Error())
}
