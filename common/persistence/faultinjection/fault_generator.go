package faultinjection

type (
	faultGenerator interface {
		generate(methodName string) *fault
	}
)
