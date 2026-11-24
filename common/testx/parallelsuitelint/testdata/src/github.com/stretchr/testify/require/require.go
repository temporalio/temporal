package require

type Assertions struct{}

type TestingT interface {
	Errorf(string, ...any)
	FailNow()
}

func (*Assertions) Equal(any, any, ...any) {}
func (*Assertions) NoError(error, ...any) {}
func Equal(TestingT, any, any, ...any) {}
func NoError(TestingT, error, ...any) {}
