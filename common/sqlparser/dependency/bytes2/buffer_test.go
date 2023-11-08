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

package bytes2

import "testing"

func TestBuffer(t *testing.T) {
	b := NewBuffer(nil)
	b.Write([]byte("ab"))
	b.WriteString("cd")
	b.WriteByte('e')
	want := "abcde"
	if got := string(b.Bytes()); got != want {
		t.Errorf("b.Bytes(): %s, want %s", got, want)
	}
	if got := b.String(); got != want {
		t.Errorf("b.String(): %s, want %s", got, want)
	}
	if got := b.Len(); got != 5 {
		t.Errorf("b.Len(): %d, want 5", got)
	}
}
