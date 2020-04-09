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

package archiver

import (
	"net/url"
)

type (
	// URI identifies the archival resource to which records are written to and read from.
	URI interface {
		Scheme() string
		Path() string
		Hostname() string
		Port() string
		Username() string
		Password() string
		String() string
		Opaque() string
		Query() map[string][]string
	}

	uri struct {
		url *url.URL
	}
)

// NewURI constructs a new archiver URI from string.
func NewURI(s string) (URI, error) {
	url, err := url.ParseRequestURI(s)
	if err != nil {
		return nil, err
	}
	return &uri{url: url}, nil
}

func (u *uri) Scheme() string {
	return u.url.Scheme
}

func (u *uri) Path() string {
	return u.url.Path
}

func (u *uri) Hostname() string {
	return u.url.Hostname()
}

func (u *uri) Port() string {
	return u.url.Port()
}

func (u *uri) Username() string {
	if u.url.User == nil {
		return ""
	}
	return u.url.User.Username()
}

func (u *uri) Password() string {
	if u.url.User == nil {
		return ""
	}
	password, exist := u.url.User.Password()
	if !exist {
		return ""
	}
	return password
}

func (u *uri) Opaque() string {
	return u.url.Opaque
}

func (u *uri) Query() map[string][]string {
	return u.url.Query()
}

func (u *uri) String() string {
	return u.url.String()
}
