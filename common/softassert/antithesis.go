//go:build with_antithesis_sdk

package softassert

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// Note that the `message` parameter cannot be changed before passing to `assert` the functions.
// It is required to be a static string.

func That(logger log.Logger, condition bool, message string, tags ...tag.Tag) bool {
	details := convert(message, tags...)
	assert.Always(condition, message, details)
	return condition
}

func ThatSometimes(logger log.Logger, condition bool, message string, tags ...tag.Tag) bool {
	details := convert(message, tags...)
	assert.SometimesInner(condition, message, details)
	return condition
}

func Sometimes(logger log.Logger, message string, tags ...tag.Tag) {
	ThatSometimes(logger, true, message, tags...)
}

func Fail(logger log.Logger, message string, tags ...tag.Tag) {
	details := convert(message, tags...)
	assert.UnreachableInner(message, details)
}

func convert(message string, tags ...tag.Tag) map[string]any {
	details := make(map[string]any, len(tags))
	for _, t := range tags {
		if zt, ok := t.(tag.ZapTag); ok {
			key := zt.Key()
			value := zt.Value()
			details[key] = value
		}
	}
	return details
}
