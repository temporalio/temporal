package racing

import "testing"

func TestRace(t *testing.T) {
	x := 0
	go func() { x++ }()
	_ = x
}
