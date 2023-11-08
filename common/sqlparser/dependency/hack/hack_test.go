/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hack

import "testing"

func TestStringArena(t *testing.T) {
	sarena := NewStringArena(10)

	s0 := sarena.NewString(nil)
	checkint(t, len(sarena.buf), 0)
	checkint(t, sarena.SpaceLeft(), 10)
	checkstring(t, s0, "")

	s1 := sarena.NewString([]byte("01234"))
	checkint(t, len(sarena.buf), 5)
	checkint(t, sarena.SpaceLeft(), 5)
	checkstring(t, s1, "01234")

	s2 := sarena.NewString([]byte("5678"))
	checkint(t, len(sarena.buf), 9)
	checkint(t, sarena.SpaceLeft(), 1)
	checkstring(t, s2, "5678")

	// s3 will be allocated outside of sarena
	s3 := sarena.NewString([]byte("ab"))
	checkint(t, len(sarena.buf), 9)
	checkint(t, sarena.SpaceLeft(), 1)
	checkstring(t, s3, "ab")

	// s4 should still fit in sarena
	s4 := sarena.NewString([]byte("9"))
	checkint(t, len(sarena.buf), 10)
	checkint(t, sarena.SpaceLeft(), 0)
	checkstring(t, s4, "9")

	sarena.buf[0] = 'A'
	checkstring(t, s1, "A1234")

	sarena.buf[5] = 'B'
	checkstring(t, s2, "B678")

	sarena.buf[9] = 'C'
	// s3 will not change
	checkstring(t, s3, "ab")
	checkstring(t, s4, "C")
	checkstring(t, sarena.str, "A1234B678C")
}

func checkstring(t *testing.T, actual, expected string) {
	if actual != expected {
		t.Errorf("received %s, expecting %s", actual, expected)
	}
}

func checkint(t *testing.T, actual, expected int) {
	if actual != expected {
		t.Errorf("received %d, expecting %d", actual, expected)
	}
}

func TestByteToString(t *testing.T) {
	v1 := []byte("1234")
	if s := String(v1); s != "1234" {
		t.Errorf("String(\"1234\"): %q, want 1234", s)
	}

	v1 = []byte("")
	if s := String(v1); s != "" {
		t.Errorf("String(\"\"): %q, want empty", s)
	}

	v1 = nil
	if s := String(v1); s != "" {
		t.Errorf("String(\"\"): %q, want empty", s)
	}
}
