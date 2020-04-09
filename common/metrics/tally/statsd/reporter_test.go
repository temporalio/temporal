package statsd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricNameWithTags(t *testing.T) {
	r := temporalTallyStatsdReporter{
		tallystatsd: nil,
	}
	tags := map[string]string{
		"tag1": "123",
		"tag2": "456",
		"tag3": "789",
	}
	name := "test-metric-name1"

	assert.Equal(t, "test-metric-name1.tag1.123.tag2.456.tag3.789", r.metricNameWithTags(name, tags))
}

func TestMetricNameWithTagsStability(t *testing.T) {
	r := temporalTallyStatsdReporter{
		tallystatsd: nil,
	}
	tags := map[string]string{
		"tag1": "123",
		"tag2": "456",
		"tag3": "789",
	}
	name := "test-metric-name2"

	//test the order is stable
	for i := 1; i <= 16; i++ {
		assert.Equal(t, r.metricNameWithTags(name, tags), r.metricNameWithTags(name, tags))
	}
}
