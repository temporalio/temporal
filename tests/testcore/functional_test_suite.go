package testcore

import (
	"go.temporal.io/server/common/testing/taskpoller"
)

type (
	// TODO (alex): merge this with FunctionalTestBase.
	FunctionalTestSuite struct {
		FunctionalTestBase

		TaskPoller *taskpoller.TaskPoller
	}
)

func (s *FunctionalTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.TaskPoller = taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
}
