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

package metrics

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	// baggageBenchTest Was used to compare the behavior of sync.Map vs map+mutex in the context of usage for
	// metricsContext.
	// See testMapBaggage for test logic.
	// As a summary, using mutex performed considerably faster than sync.Map.
	// These results are to be reviewed if the usage scenario of metricsContext changes.
	baggageBenchTest struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller
	}

	testBaggage interface {
		Add(k string, v int64)
		Get(k string) int64
	}

	baggageSyncMap struct {
		data *sync.Map
	}

	baggageMutexMap struct {
		sync.Mutex
		data map[string]int64
	}
)

func TestBaggageBenchSuite(t *testing.T) {
	s := new(baggageBenchTest)
	suite.Run(t, s)
}

func (s *baggageBenchTest) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *baggageBenchTest) TearDownTest() {}

func (b *baggageSyncMap) Add(k string, v int64) {
	for done := false; !done; {
		metricInterface, _ := b.data.LoadAndDelete(k)
		var newValue = v
		if metricInterface != nil {
			newValue += metricInterface.(int64)
		}
		_, loaded := b.data.LoadOrStore(k, newValue)
		done = !loaded
	}
}

func (b *baggageSyncMap) Get(k string) int64 {
	metricInterface, _ := b.data.LoadAndDelete(k)
	if metricInterface == nil {
		return 0
	}
	return metricInterface.(int64)
}

func (b *baggageMutexMap) Add(k string, v int64) {
	b.Lock()
	defer b.Unlock()

	value := b.data[k]
	value += v
	b.data[k] = value
}

func (b *baggageMutexMap) Get(k string) int64 {
	b.Lock()
	defer b.Unlock()
	return b.data[k]
}

// roughly 1.7s/7.5s for mutex/sync
// baggageCount := 1000
// threadCount := 20
// updatesPerThread := 1000
func testMapBaggage(createTestObj func() testBaggage) {
	baggageCount := 10
	threadCount := 10
	updatesPerThread := 10

	keys := []string{"k1", "k2", "k3", "k4", "k5"}
	start := time.Now()
	sum := int64(0)
	for bag := 0; bag < baggageCount; bag++ {
		testObj := createTestObj()
		wg := sync.WaitGroup{}
		wg.Add(threadCount)
		for th := 0; th < threadCount; th++ {
			go func(key string) {
				for upd := 0; upd < updatesPerThread; upd++ {
					testObj.Add(key, rand.Int63())
				}
				wg.Done()
			}(keys[th%len(keys)])
		}
		wg.Wait()
		val := testObj.Get(keys[0])
		sum += val
	}
	println("sum: ", sum)
	println("duration: ", time.Since(start))
}

func (s *baggageBenchTest) TestSyncMapBaggage() {
	testMapBaggage(func() testBaggage { return &baggageSyncMap{data: &sync.Map{}} })
}

func (s *baggageBenchTest) TestMutexMapBaggage() {
	testMapBaggage(func() testBaggage { return &baggageMutexMap{data: make(map[string]int64)} })
}
