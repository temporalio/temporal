package interfaces

type TransactionPolicy int

func (policy TransactionPolicy) Ptr() *TransactionPolicy {
	return &policy
}

const (
	TransactionPolicyActive  TransactionPolicy = 0
	TransactionPolicyPassive TransactionPolicy = 1
)
