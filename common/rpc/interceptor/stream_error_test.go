package interceptor

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	streamErrorSuite struct {
		suite.Suite
	}
)

func TestStreamErrorSuite(t *testing.T) {
	s := new(streamErrorSuite)
	suite.Run(t, s)
}

func (s *streamErrorSuite) TearDownSuite() {
}

func (s *streamErrorSuite) TearDownTest() {
}

func (s *streamErrorSuite) TestErrorConversion() {
	require.Equal(s.T(), nil, errorConvert(nil))
	require.Equal(s.T(), io.EOF, errorConvert(io.EOF))

	require.IsType(s.T(), nil, errorConvert(status.Error(codes.OK, "")))
	require.IsType(s.T(), &serviceerror.DeadlineExceeded{}, errorConvert(status.Error(codes.DeadlineExceeded, "")))
	require.IsType(s.T(), &serviceerror.Canceled{}, errorConvert(status.Error(codes.Canceled, "")))
	require.IsType(s.T(), &serviceerror.InvalidArgument{}, errorConvert(status.Error(codes.InvalidArgument, "")))
	require.IsType(s.T(), &serviceerror.FailedPrecondition{}, errorConvert(status.Error(codes.FailedPrecondition, "")))
	require.IsType(s.T(), &serviceerror.Unavailable{}, errorConvert(status.Error(codes.Unavailable, "")))
	require.IsType(s.T(), &serviceerror.Internal{}, errorConvert(status.Error(codes.Internal, "")))
	require.IsType(s.T(), &serviceerror.Internal{}, errorConvert(status.Error(codes.Unknown, "")))
}
