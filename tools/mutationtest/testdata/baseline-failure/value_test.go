package baselinefailure

import "testing"

func TestValue(t *testing.T) {
	t.Fatal("baseline failed")
}
