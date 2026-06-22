package definition

import (
	"fmt"
	"testing"

	"go.temporal.io/server/common/testing/parallelsuite"
)

type (
	resourceDeduplicationSuite struct {
		parallelsuite.Suite[*resourceDeduplicationSuite]
	}
)

func TestResourceDeduplicationSuite(t *testing.T) {
	parallelsuite.Run(t, new(resourceDeduplicationSuite))
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
