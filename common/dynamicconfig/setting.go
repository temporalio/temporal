// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

//go:generate go run ../../cmd/tools/gendynamicconfig

package dynamicconfig

type (
	// Type is an enum for the data type of a dynamic config setting.
	Type int

	// Precedence is an enum for the search order precedence of a dynamic config setting.
	// E.g., use the global value, check namespace then global, check task queue then
	// namespace then global, etc.
	Precedence int

	// setting is one dynamic config setting. setting should not be used or created directly,
	// but use one of the generated constructors for instantiated Setting types in
	// setting_gen.go, e.g. NewNamespaceBoolSetting.
	// T is the data type of the setting. P is a go type representing the precedence, which is
	// just used to make the types more unique.
	setting[T any, P any] struct {
		key         Key // string value of key. case-insensitive.
		def         T   // default value. cdef is used in preference to def if non-nil.
		cdef        []TypedConstrainedValue[T]
		convert     func(any) (T, error) // converter function
		description string               // documentation
	}

	// GenericSetting is an interface that all instances of Setting implement (by generated
	// code in setting_gen.go). It can be used to refer to settings of any type and deal with
	// them generically..
	GenericSetting interface {
		Key() Key
		Precedence() Precedence
	}
)
