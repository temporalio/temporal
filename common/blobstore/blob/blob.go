// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package blob

import "fmt"

// Blob is the entity that gets handled by blobstore
type Blob struct {
	Body []byte
	Tags map[string]string
}

// NewBlob constructs a new blob with body and tags
func NewBlob(body []byte, tags map[string]string) *Blob {
	return &Blob{
		Body: body,
		Tags: tags,
	}
}

// DeepCopy returns a deep copy of blob
func (b *Blob) DeepCopy() *Blob {
	if b == nil {
		return nil
	}
	tagsCopy := make(map[string]string, len(b.Tags))
	for k, v := range b.Tags {
		tagsCopy[k] = v
	}
	bodyCopy := make([]byte, len(b.Body), len(b.Body))
	for i, b := range b.Body {
		bodyCopy[i] = b
	}
	return &Blob{
		Body: bodyCopy,
		Tags: tagsCopy,
	}
}

// Equal returns true if input blob is equal, false otherwise.
func (b *Blob) Equal(other *Blob) bool {
	equal, _ := b.EqualWithDetails(other)
	return equal
}

// EqualWithDetails returns true if input blob is equal, false otherwise.
// Additionally returns reason for the inequality.
func (b *Blob) EqualWithDetails(other *Blob) (bool, string) {
	if b == nil && other == nil {
		return true, ""
	}
	if b == nil {
		return false, "reference blob is nil while argument blob is not"
	}
	if other == nil {
		return false, "reference blob is not nil while argument blob is"
	}

	if len(b.Tags) != len(other.Tags) {
		return false, fmt.Sprintf("blob tag sizes do not match {reference:%v, argument:%v}", b.Tags, other.Tags)
	}
	for k, v := range b.Tags {
		otherVal, ok := other.Tags[k]
		if !ok || otherVal != v {
			return false, fmt.Sprintf("blob tags do not match {reference:%v, argument:%v}", b.Tags, other.Tags)
		}
	}

	if len(b.Body) != len(other.Body) {
		return false, fmt.Sprintf("blob body sizes do not match {reference:%v, argument:%v}", len(b.Body), len(other.Body))
	}
	for i := 0; i < len(b.Body); i++ {
		if b.Body[i] != other.Body[i] {
			return false, "blob bodies do not match"
		}
	}
	return true, ""
}
