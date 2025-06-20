package quotastest

// NewFakeMemberCounter returns a new fake quotas.MemberCounter that always returns numInstances.
func NewFakeMemberCounter(numInstances int) memberCounter {
	return memberCounter{numInstances: numInstances}
}

type memberCounter struct {
	numInstances int
}

func (c memberCounter) AvailableMemberCount() int {
	return c.numInstances
}
