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

package metrics

const (
	instance       = "instance"
	domain         = "domain"
	domainAllValue = "all"
)

// Tag is an interface to define metrics tags
type Tag interface {
	Key() string
	Value() string
}

type domainTag struct {
	value string
}

// DomainTag returns a new domain tag
func DomainTag(value string) Tag {
	return domainTag{value}
}

// Key returns the key of the domain tag
func (d domainTag) Key() string {
	return domain
}

// Value returns the value of a domain tag
func (d domainTag) Value() string {
	return d.value
}

type domainAllTag struct{}

// DomainAllTag returns a new domain all tag-value
func DomainAllTag() Tag {
	return domainAllTag{}
}

// Key returns the key of the domain all tag
func (d domainAllTag) Key() string {
	return domain
}

// Value returns the value of the domain all tag
func (d domainAllTag) Value() string {
	return domainAllValue
}

type instanceTag struct {
	value string
}

// InstanceTag returns a new instance tag
func InstanceTag(value string) Tag {
	return instanceTag{value}
}

// Key returns the key of the instance tag
func (i instanceTag) Key() string {
	return instance
}

// Value returns the value of a instance tag
func (i instanceTag) Value() string {
	return i.value
}
