package quotastest

// NewFakeInstanceCounter returns a new fake quotas.InstanceCounter that always returns numInstances.
func NewFakeInstanceCounter(numInstances int) instanceCounter {
	return instanceCounter{numInstances: numInstances}
}

type instanceCounter struct {
	numInstances int
}

func (c instanceCounter) MemberCount() int {
	return c.numInstances
}
