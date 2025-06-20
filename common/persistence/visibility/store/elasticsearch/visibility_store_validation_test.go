package elasticsearch

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
)

func TestValidationDatetime(t *testing.T) {
	store := VisibilityStore{}

	// valid datetime
	_, err := store.ValidateCustomSearchAttributes(map[string]any{
		"CustomDatetimeField": time.Now(),
	})
	assert.NoError(t, err)

	// invalid out of range datetime
	_, err = store.ValidateCustomSearchAttributes(map[string]any{
		"CustomDatetimeField": time.Unix(0, math.MaxInt64).Add(time.Hour),
	})
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.InvalidArgument{}, err)
	assert.Contains(t, err.Error(), "Visibility store validation errors")
}
