package tests

import (
	"testing"

	"github.com/stretchr/testify/suite"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
)

type ElasticsearchSuite struct {
	suite.Suite
	pluginName   string
	connectAttrs map[string]string
}

func (p *ElasticsearchSuite) TestVisibilityPersistenceSuite() {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithEs(
			persistencetests.GetEsTestClusterOption(),
		),
	}
	suite.Run(p.T(), s)
}

func TestElasticsearch(t *testing.T) {
	t.Parallel()
	s := &ElasticsearchSuite{}
	suite.Run(t, s)
}
