package tag

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
		assert.Equal(t, data.expectedResult, ServiceErrorType(data.err).Value().(string), "Unexpected error type in index %d", id)
	}
}
