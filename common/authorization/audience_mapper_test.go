package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type audienceMapperSuite struct {
	suite.Suite
	controller *gomock.Controller
}

func TestAudienceMapperSuite(t *testing.T) {
	suite.Run(t, new(audienceMapperSuite))
}

func (s *audienceMapperSuite) SetupTest() {

	s.controller = gomock.NewController(s.T())
}

func (s *audienceMapperSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *audienceMapperSuite) TestNewAudienceMapper_Static() {
	mapper := NewAudienceMapper("foo-audience")
	audience := mapper.Audience(context.Background(), nil, &grpc.UnaryServerInfo{})
	require.Equal(s.T(), "foo-audience", audience)
}

func (s *audienceMapperSuite) TestGetAudienceMapperFromConfig() {
	cfg := &config.Authorization{Audience: "bar-audience"}
	mapper, _ := GetAudienceMapperFromConfig(cfg)
	audience := mapper.Audience(context.Background(), nil, &grpc.UnaryServerInfo{})
	require.Equal(s.T(), "bar-audience", audience)
}
