package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	executionMutableStateSuite struct {
		suite.Suite
		*require.Assertions

		store  p.ExecutionManager
		logger log.Logger
	}
)

func newExecutionMutableStateSuite(
	t *testing.T,
	store p.ExecutionStore,
	logger log.Logger,
) *executionMutableStateSuite {
	return &executionMutableStateSuite{
		Assertions: require.New(t),
		store: p.NewExecutionManager(
			store,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		logger: logger,
	}
}

func (s *executionMutableStateSuite) SetupSuite() {

}

func (s *executionMutableStateSuite) TearDownSuite() {

}

func (s *executionMutableStateSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *executionMutableStateSuite) TearDownTest() {

}

func (s *executionMutableStateSuite) TestCreate() {}

func (s *executionMutableStateSuite) TestUpdate() {}

func (s *executionMutableStateSuite) TestConflictResolve() {}

func (s *executionMutableStateSuite) TestDeleteCurrent() {}

func (s *executionMutableStateSuite) TestDelete() {}
