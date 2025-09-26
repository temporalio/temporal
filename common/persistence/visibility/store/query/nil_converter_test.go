package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

type nilConverterSuite struct {
	suite.Suite
	*require.Assertions
	ctrl *gomock.Controller

	queryConverter *nilStoreQueryConverter
}

func TestNilConverter(t *testing.T) {
	s := &nilConverterSuite{}
	suite.Run(t, s)
}

func (s *nilConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.queryConverter = &nilStoreQueryConverter{}
}

func (s *nilConverterSuite) TestGetDatetimeFormat() {
	s.Empty(s.queryConverter.GetDatetimeFormat())
}

func (s *nilConverterSuite) TestBuildParenExpr() {
	out, err := s.queryConverter.BuildParenExpr(struct{}{})
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestBuildNotExpr() {
	out, err := s.queryConverter.BuildNotExpr(struct{}{})
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestBuildAndExpr() {
	out, err := s.queryConverter.BuildAndExpr(struct{}{})
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestBuildOrExpr() {
	out, err := s.queryConverter.BuildOrExpr(struct{}{})
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestConvertComparisonExpr() {
	out, err := s.queryConverter.ConvertComparisonExpr("", nil, 123)
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestConvertKeywordComparisonExpr() {
	out, err := s.queryConverter.ConvertKeywordComparisonExpr("", nil, 123)
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestConvertKeywordListComparisonExpr() {
	out, err := s.queryConverter.ConvertKeywordListComparisonExpr("", nil, 123)
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestConvertTextComparisonExpr() {
	out, err := s.queryConverter.ConvertTextComparisonExpr("", nil, 123)
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestConvertRangeExpr() {
	out, err := s.queryConverter.ConvertRangeExpr("", nil, 123, 456)
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestConvertIsExpr() {
	out, err := s.queryConverter.ConvertIsExpr("", nil)
	s.Nil(out)
	s.NoError(err)
}

func (s *nilConverterSuite) TestNewNilQueryConverter() {
	c := NewNilQueryConverter("", "", searchattribute.TestNameTypeMap, nil)
	s.Equal(&nilStoreQueryConverter{}, c.storeQC)
}
