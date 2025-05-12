package statsd

import (
	"strconv"
	"strings"
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

	// test the order is stable
	for i := 1; i <= 16; i++ {
		assert.Equal(t, r.metricNameWithTags(name, tags), r.metricNameWithTags(name, tags))
	}
}

func TestMetricNameWithSeparatedTags(t *testing.T) {
	testCases := []string{
		"",
		",",
		"__",
		".__",
	}
	for i, tc := range testCases {
		sep := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := temporalTallyStatsdReporter{
				tagSeparator: sep,
			}
			tags := map[string]string{
				"tag1": "123",
				"tag2": "456",
				"tag3": "789",
			}
			name := "test-metric-name3"

			newName := r.metricNameWithTags(name, tags)

			if sep == "" {
				// Tags should be embedded.
				assert.Equal(t, newName, "test-metric-name3.tag1.123.tag2.456.tag3.789")
			} else {
				// Tags will be appended with the separator.
				assert.True(t, strings.HasPrefix(newName, name))

				ss := strings.Split(newName, sep)
				assert.Len(t, ss, 1+len(tags))
				assert.Equal(t, ss[0], name)
				assert.Contains(t, ss, "tag1=123")
				assert.Contains(t, ss, "tag2=456")
				assert.Contains(t, ss, "tag3=789")
			}
		})
	}

}
