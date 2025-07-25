package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type (
	matchingTaskV2Suite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.MatchingTaskV2
	}
)

func NewMatchingTaskV2Suite(
	t *testing.T,
	store sqlplugin.MatchingTask,
) *matchingTaskSuite {
	return &matchingTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *matchingTaskV2Suite) SetupSuite() {
}

func (s *matchingTaskV2Suite) TearDownSuite() {
}

func (s *matchingTaskV2Suite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *matchingTaskV2Suite) TearDownTest() {
}

// FIXME: new tests here
