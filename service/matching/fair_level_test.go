package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFairLevelLess(t *testing.T) {
	assert.True(t, fairLevelLess(fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 105}))
	assert.False(t, fairLevelLess(fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 100}))
	assert.True(t, fairLevelLess(fairLevel{pass: 10, id: 100}, fairLevel{pass: 11, id: 100}))
	assert.False(t, fairLevelLess(fairLevel{pass: 10, id: 100}, fairLevel{pass: 5, id: 105}))
}

func TestFairLevelComparator(t *testing.T) {
	assert.Equal(t, -1, fairLevelComparator(fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 105}))
	assert.Equal(t, 0, fairLevelComparator(fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 100}))
	assert.Equal(t, -1, fairLevelComparator(fairLevel{pass: 10, id: 100}, fairLevel{pass: 11, id: 100}))
	assert.Equal(t, 1, fairLevelComparator(fairLevel{pass: 10, id: 100}, fairLevel{pass: 5, id: 105}))
}

func TestFairLevelMax(t *testing.T) {
	assert.Equal(t, fairLevel{pass: 10, id: 105}, fairLevelMax(fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 105}))
	assert.Equal(t, fairLevel{pass: 10, id: 100}, fairLevelMax(fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 100}))
	assert.Equal(t, fairLevel{pass: 11, id: 100}, fairLevelMax(fairLevel{pass: 10, id: 100}, fairLevel{pass: 11, id: 100}))
	assert.Equal(t, fairLevel{pass: 10, id: 100}, fairLevelMax(fairLevel{pass: 10, id: 100}, fairLevel{pass: 5, id: 105}))
}
