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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ConfigSuite struct {
	*require.Assertions
	suite.Suite
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

func (s *ConfigSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *ConfigSuite) TestValidate() {
	testCases := []struct {
		config  *Config
		isValid bool
	}{
		{
			config: &Config{
				StoreDirectory: "",
			},
			isValid: false,
		},
		{
			config: &Config{
				StoreDirectory: "test-store-directory",
				DefaultBucket:  BucketConfig{},
			},
			isValid: false,
		},
		{
			config: &Config{
				StoreDirectory: "test-store-directory",
				DefaultBucket: BucketConfig{
					Name: "test-default-bucket-name",
				},
			},
			isValid: false,
		},
		{
			config: &Config{
				StoreDirectory: "test-store-directory",
				DefaultBucket: BucketConfig{
					Name:  "test-default-bucket-name",
					Owner: "test-default-bucket-owner",
				},
			},
			isValid: true,
		},
		{
			config: &Config{
				StoreDirectory: "test-store-directory",
				DefaultBucket: BucketConfig{
					Name:          "test-default-bucket-name",
					Owner:         "test-default-bucket-owner",
					RetentionDays: -1,
				},
			},
			isValid: false,
		},
		{
			config: &Config{
				StoreDirectory: "test-store-directory",
				DefaultBucket: BucketConfig{
					Name:          "test-default-bucket-name",
					Owner:         "test-default-bucket-owner",
					RetentionDays: 10,
				},
			},
			isValid: true,
		},
		{
			config: &Config{
				StoreDirectory: "test-store-directory",
				DefaultBucket: BucketConfig{
					Name:          "test-default-bucket-name",
					Owner:         "test-default-bucket-owner",
					RetentionDays: 10,
				},
				CustomBuckets: []BucketConfig{
					{},
				},
			},
			isValid: false,
		},
		{
			config: &Config{
				StoreDirectory: "test-store-directory",
				DefaultBucket: BucketConfig{
					Name:          "test-default-bucket-name",
					Owner:         "test-default-bucket-owner",
					RetentionDays: 10,
				},
				CustomBuckets: []BucketConfig{
					{
						Name:          "test-custom-bucket-name",
						Owner:         "test-custom-bucket-owner",
						RetentionDays: 10,
					},
				},
			},
			isValid: true,
		},
	}

	for _, tc := range testCases {
		if tc.isValid {
			s.NoError(tc.config.Validate())
		} else {
			s.Error(tc.config.Validate())
		}
	}
}
