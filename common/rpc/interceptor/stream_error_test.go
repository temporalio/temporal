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
		*require.Assertions
		suite.Suite
	}
)

func TestStreamErrorSuite(t *testing.T) {
	s := new(streamErrorSuite)
	suite.Run(t, s)
}

func (s *streamErrorSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *streamErrorSuite) TearDownSuite() {
}

func (s *streamErrorSuite) SetupTest() {
}

func (s *streamErrorSuite) TearDownTest() {
}

func (s *streamErrorSuite) TestErrorConversion() {
	s.Equal(nil, errorConvert(nil))
	s.Equal(io.EOF, errorConvert(io.EOF))

	s.IsType(nil, errorConvert(status.Error(codes.OK, "")))
	s.IsType(&serviceerror.DeadlineExceeded{}, errorConvert(status.Error(codes.DeadlineExceeded, "")))
	s.IsType(&serviceerror.Canceled{}, errorConvert(status.Error(codes.Canceled, "")))
	s.IsType(&serviceerror.InvalidArgument{}, errorConvert(status.Error(codes.InvalidArgument, "")))
	s.IsType(&serviceerror.FailedPrecondition{}, errorConvert(status.Error(codes.FailedPrecondition, "")))
	s.IsType(&serviceerror.Unavailable{}, errorConvert(status.Error(codes.Unavailable, "")))
	s.IsType(&serviceerror.Internal{}, errorConvert(status.Error(codes.Internal, "")))
	s.IsType(&serviceerror.Internal{}, errorConvert(status.Error(codes.Unknown, "")))
}
