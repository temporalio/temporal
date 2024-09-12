// The MIT License
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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination events_mock.go

package events

type (
	EventBuilder interface {
		WithString(string, string) EventBuilder
		WithFloat(string, float64) EventBuilder
		WithInt(string, int64) EventBuilder
		Emit()
	}

	EventHandler interface {
		CreateEvent(string) EventBuilder
	}

	NullEventHandler struct{}
	NullEventBuilder struct{}
)

func (h *NullEventHandler) CreateEvent(_ string) *NullEventBuilder {
	return &NullEventBuilder{}
}

func (b *NullEventBuilder) Emit() {}

func (b *NullEventBuilder) WithString(_ string, _ string) *NullEventBuilder {
	return b
}

func (b *NullEventBuilder) WithFloat(_ string, _ float64) *NullEventBuilder {
	return b
}

func (b *NullEventBuilder) WithInt(_ string, _ int64) *NullEventBuilder {
	return b
}
