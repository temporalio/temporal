package smoke

import "testing"

func TestValue(t *testing.T) {
	if !value(1) {
		t.Fatal("value is false")
	}
	if !other(1) {
		t.Fatal("other is false")
	}
	if !booleanLiteral() {
		t.Fatal("boolean literal is false")
	}
}
