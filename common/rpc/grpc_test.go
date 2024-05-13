package rpc

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/serviceerror"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHideUnknownOrInternalErrors(t *testing.T) {
	mockLogger := log.NewMockLogger(gomock.NewController(t))

	statusOk := status.New(codes.OK, "OK")
	testHideUnknownOrInternalErrors(t, mockLogger, statusOk, false)

	statusUnknown := status.New(codes.Unknown, "Unknown")
	mockLogger.EXPECT().Error(gomock.Any())
	testHideUnknownOrInternalErrors(t, mockLogger, statusUnknown, true)

	statusInternal := status.New(codes.Internal, "Internal")
	mockLogger.EXPECT().Error(gomock.Any())
	testHideUnknownOrInternalErrors(t, mockLogger, statusInternal, true)
}

func testHideUnknownOrInternalErrors(t *testing.T, mockLogger *log.MockLogger, s *status.Status, expectRelpace bool) {
	err := serviceerror.FromStatus(s)
	errorMessage := HideUnknownOrInternalErrors(mockLogger, err)
	if expectRelpace {
		baseMassage := "Something went wrong, please retry."
		errorHash := errorWithHash(err)
		expectedMessage := fmt.Sprintf("%s (reference %s)", baseMassage, errorHash)

		assert.Equal(t, errorMessage.Error(), expectedMessage)
	} else {
		if err == nil {
			assert.Equal(t, errorMessage, nil)
		} else {
			assert.Equal(t, errorMessage.Error(), s.Message())
		}
	}
}
