package faultinjection

type (
	faultGenerator interface {
		generate(methodName string, request ...any) *fault
	}
)
