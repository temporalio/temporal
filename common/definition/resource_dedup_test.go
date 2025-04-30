package definition

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	resourceDeduplicationSuite struct {
		suite.Suite
	}
)

func TestResourceDeduplicationSuite(t *testing.T) {
	s := new(resourceDeduplicationSuite)
	suite.Run(t, s)
}

func (s *resourceDeduplicationSuite) TestGenerateKey() {
	resourceType := int32(1)
	id := "id"
	key := generateKey(resourceType, id)
	s.Equal(fmt.Sprintf("%v::%v", resourceType, id), key)
}

func (s *resourceDeduplicationSuite) TestGenerateDeduplicationKey() {
	runID := "runID"
	eventID := int64(1)
	version := int64(2)
	resource := NewEventReappliedID(runID, eventID, version)
	key := GenerateDeduplicationKey(resource)
	s.Equal(fmt.Sprintf("%v::%v::%v::%v", eventReappliedID, runID, eventID, version), key)
}

func (s *resourceDeduplicationSuite) TestEventReappliedID() {
	runID := "runID"
	eventID := int64(1)
	version := int64(2)
	resource := NewEventReappliedID(runID, eventID, version)
	s.Equal(fmt.Sprintf("%v::%v::%v", runID, eventID, version), resource.GetID())
}
