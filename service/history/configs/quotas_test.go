package configs

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

type (
	quotasSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestQuotasSuite(t *testing.T) {
	s := new(quotasSuite)
	suite.Run(t, s)
}

func (s *quotasSuite) SetupSuite() {
}

func (s *quotasSuite) TearDownSuite() {
}

func (s *quotasSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *quotasSuite) TearDownTest() {
}

func (s *quotasSuite) TestCallerTypeToPriorityMapping() {
	for _, priority := range CallerTypeToPriority {
		index := slices.Index(APIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestAPIPrioritiesOrdered() {
	for idx := range APIPrioritiesOrdered[1:] {
		s.True(APIPrioritiesOrdered[idx] < APIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestOperatorPrioritized() {
	rateFn := func() float64 { return 5 }
	operatorRPSRatioFn := func() float64 { return 0.2 }
	limiter := NewPriorityRateLimiter(rateFn, operatorRPSRatioFn)

	operatorRequest := quotas.NewRequest(
		"/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution",
		1,
		"",
		headers.CallerTypeOperator,
		-1,
		"")

	apiRequest := quotas.NewRequest(
		"/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution",
		1,
		"",
		headers.CallerTypeAPI,
		-1,
		"")

	requestTime := time.Now()
	limitCount := 0

	for i := 0; i < 12; i++ {
		if !limiter.Allow(requestTime, apiRequest) {
			limitCount++
			s.True(limiter.Allow(requestTime, operatorRequest))
		}
	}
	s.Equal(2, limitCount)
}
