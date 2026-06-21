package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateUTF8(t *testing.T) {
	s := "hello \u2603!!!"
	assert.Equal(t, 12, len(s))
	assert.Equal(t, "hello \u2603!!!", TruncateUTF8(s, 20))
	assert.Equal(t, "hello \u2603!!!", TruncateUTF8(s, 12))
	assert.Equal(t, "hello \u2603!!", TruncateUTF8(s, 11))
	assert.Equal(t, "hello \u2603!", TruncateUTF8(s, 10))
	assert.Equal(t, "hello \u2603", TruncateUTF8(s, 9))
	assert.Equal(t, "hello ", TruncateUTF8(s, 8))
	assert.Equal(t, "hello ", TruncateUTF8(s, 7))
	assert.Equal(t, "hello ", TruncateUTF8(s, 6))
	assert.Equal(t, "hello", TruncateUTF8(s, 5))
	assert.Equal(t, "", TruncateUTF8(s, 0))
	assert.Equal(t, "", TruncateUTF8(s, -3))
}
