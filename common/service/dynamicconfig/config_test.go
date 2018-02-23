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

package dynamicconfig

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type inMemoryClient struct {
	values map[Key]interface{}
	sync.RWMutex
}

func newInMemoryClient() *inMemoryClient {
	return &inMemoryClient{values: make(map[Key]interface{})}
}

func (mc *inMemoryClient) SetValue(key Key, value interface{}) {
	mc.Lock()
	defer mc.Unlock()
	mc.values[key] = value
}

func (mc *inMemoryClient) GetValue(key Key) (interface{}, error) {
	mc.RLock()
	defer mc.RUnlock()
	if val, ok := mc.values[key]; ok {
		return val, nil
	}
	return nil, errors.New("unable to find key")
}

func (mc *inMemoryClient) GetValueWithFilters(
	name Key, filters map[Filter]interface{},
) (interface{}, error) {
	return mc.GetValue(name)
}

type configSuite struct {
	suite.Suite
	client *inMemoryClient
	cln    *Collection
}

func TestConfigSuite(t *testing.T) {
	s := new(configSuite)
	suite.Run(t, s)
}

func (s *configSuite) SetupSuite() {
	s.client = newInMemoryClient()
	s.cln = NewCollection(s.client)
}

func (s *configSuite) TestGetIntProperty() {
	key := MaxTaskBatchSize
	size := s.cln.GetIntProperty(key, 10)
	s.Equal(10, size())
	s.client.SetValue(key, 50)
	s.Equal(50, size())
	s.client.SetValue(key, "hello world")
	s.Panics(func() { size() }, "Should panic")
}

func (s *configSuite) TestGetDurationProperty() {
	key := MatchingLongPollExpirationInterval
	interval := s.cln.GetDurationProperty(key, time.Second)
	s.Equal(time.Second, interval())
	s.client.SetValue(key, time.Minute)
	s.Equal(time.Minute, interval())
	s.client.SetValue(key, 10)
	s.Panics(func() { interval() }, "Should panic")
}
