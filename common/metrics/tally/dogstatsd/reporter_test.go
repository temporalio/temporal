package dogstatsd

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCapabilities(t *testing.T) {
	r := NewReporter(nil)
	assert.True(t, r.Capabilities().Reporting())
	assert.True(t, r.Capabilities().Tagging())
}

func TestTagMarshaling(t *testing.T) {
	r := tallyDogstatsdReporter{
		dogstatsd: nil,
	}
	tags := map[string]string{
		"tag1": "123",
		"tag2": "456",
		"tag3": "789",
	}
	dogTags := r.marshalTags(tags)

	assert.Equal(t, "tag1:123", dogTags[0])
	assert.Equal(t, "tag2:456", dogTags[1])
	assert.Equal(t, "tag3:789", dogTags[2])
}

func TestTagMarshalingStability(t *testing.T) {
	r := tallyDogstatsdReporter{}

	tags := map[string]string{
		"tag1": "123",
		"tag2": "456",
		"tag3": "789",
	}

	for i := 1; i <= 16; i++ {
		assert.Equal(t, r.marshalTags(tags), r.marshalTags(tags))
	}
}
