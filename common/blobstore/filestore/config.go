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

package filestore

import (
	"errors"
)

type (
	// Config describes the configuration needed to construct a blobstore client backed by file system
	Config struct {
		StoreDirectory string   `yaml:"storeDirectory"`
		DefaultBucket  string   `yaml:"defaultBucket"`
		CustomBuckets  []string `yaml:"customBuckets"`
	}
)

// Validate validates config
func (c *Config) Validate() error {
	if len(c.StoreDirectory) == 0 {
		return errors.New("empty store directory")
	}
	if len(c.DefaultBucket) == 0 {
		return errors.New("empty bucket name")
	}
	for _, b := range c.CustomBuckets {
		if len(b) == 0 {
			return errors.New("empty bucket name")
		}
	}
	return nil
}
