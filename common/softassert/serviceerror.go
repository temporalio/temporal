package softassert

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	// using a unique tag name to avoid potential conflicts with other tags
	errorDetailsTagName = "softassert-error-details"
)

// UnexpectedDataLoss creates a serviceerror.DataLoss from a static message and optional error.
// The error message follows the format "<staticMessage>: <optionalErr>", if optionalErr is not nil.
//
// It also logs `staticMessage` using softassert.Fail.
// `optionalErr` is added as a tag with key "softassert-error-details".
func UnexpectedDataLoss(logger log.Logger, staticMessage string, optionalErr error, tags ...tag.Tag) error {
	// (1) trigger softassert
	combinedTags := tags
	if optionalErr != nil {
		combinedTags = append([]tag.Tag{tag.NewErrorTag(errorDetailsTagName, optionalErr)}, tags...)
	}
	Fail(logger, staticMessage, combinedTags...)

	// (2) return serviceerror
	message := staticMessage
	if optionalErr != nil {
		message = fmt.Sprintf("%s: %s", staticMessage, optionalErr)
	}
	return serviceerror.NewDataLoss(message)
}
