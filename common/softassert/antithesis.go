//go:build with_antithesis_sdk

package softassert

//nolint:depguard | Use of Antithesis SDK is allowed (only) here
import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

func That(logger log.Logger, condition bool, message string, tags ...tag.Tag) bool {
	details, formattedMessage := convert(message, tags...)
	assert.Always(condition, formattedMessage, details)
	return condition
}

func ThatSometimes(logger log.Logger, condition bool, message string, tags ...tag.Tag) bool {
	details, formattedMessage := convert(message, tags...)
	assert.SometimesInner(condition, formattedMessage, details)
	return condition
}

func Sometimes(logger log.Logger, message string, tags ...tag.Tag) {
	Sometimes(logger, true, message, tags...)
}

func Fail(logger log.Logger, message string, tags ...tag.Tag) {
	details, formattedMessage := convert(message, tags...)
	assert.UnreachableInner(formattedMessage, details)
}

func convert(message string, tags ...tag.Tag) (map[string]any, string) {
	details := make(map[string]any, len(tags))
	var component string

	for _, t := range tags {
		if zt, ok := t.(tag.ZapTag); ok {
			key := zt.Key()
			value := zt.Value()
			if key == "component" {
				if compStr, ok := value.(string); ok {
					component = compStr
				}
			} else {
				details[key] = value
			}
		}
	}

	formattedMessage := message
	if component != "" {
		formattedMessage = "[" + component + "] " + message
	}

	return details, formattedMessage
}
