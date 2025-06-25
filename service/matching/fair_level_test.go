package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFairLevelLess(t *testing.T) {
	assert.True(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 10, id: 105}))
	assert.False(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 10, id: 100}))
	assert.True(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 11, id: 100}))
	assert.False(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 5, id: 105}))
}

func TestFairLevelComparator(t *testing.T) {
	m := newFairLevelTreeMap()
	a, b, c, d := fairLevel{pass: 10, id: 105}, fairLevel{pass: 10, id: 100}, fairLevel{pass: 11, id: 100}, fairLevel{pass: 5, id: 105}
	m.Put(a, nil)
	m.Put(b, nil)
	m.Put(c, nil)
	m.Put(d, nil)
	assert.Equal(t, m.Keys(), []any{d, b, a, c})
}

func TestFairLevelMax(t *testing.T) {
	assert.Equal(t, fairLevel{pass: 10, id: 105}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 10, id: 105}))
	assert.Equal(t, fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 10, id: 100}))
	assert.Equal(t, fairLevel{pass: 11, id: 100}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 11, id: 100}))
	assert.Equal(t, fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 5, id: 105}))
}
