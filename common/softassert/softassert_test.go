package softassert

import (
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/mock/gomock"
)

func TestUnreachablePreservesFailMessageAndTags(t *testing.T) {
	logger := log.NewMockLogger(gomock.NewController(t))
	context := tag.NewStringTag("context", "value")
	logger.EXPECT().Error("failed assertion: unreachable", tag.FailedAssertion, context)
	Unreachable(logger, "unreachable", context)
}
