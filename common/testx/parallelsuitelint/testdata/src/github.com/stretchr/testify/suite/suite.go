package suite

import "testing"

type TestingSuite interface {
	T() *testing.T
	SetT(*testing.T)
	SetS(TestingSuite)
}

type Suite struct{}

func (*Suite) T() *testing.T { return nil }
func (*Suite) SetT(*testing.T) {}
func (*Suite) SetS(TestingSuite) {}
func Run(*testing.T, TestingSuite) {}
