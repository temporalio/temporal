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

package execution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
)

func TestFindAutoResetPoint(t *testing.T) {
	timeSource := clock.NewRealTimeSource()

	// case 1: nil
	_, pt := FindAutoResetPoint(timeSource, nil, nil)
	assert.Nil(t, pt)

	// case 2: empty
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{}, &workflow.ResetPoints{})
	assert.Nil(t, pt)

	pt0 := &workflow.ResetPointInfo{
		BinaryChecksum: common.StringPtr("abc"),
		Resettable:     common.BoolPtr(true),
	}
	pt1 := &workflow.ResetPointInfo{
		BinaryChecksum: common.StringPtr("def"),
		Resettable:     common.BoolPtr(true),
	}
	pt3 := &workflow.ResetPointInfo{
		BinaryChecksum: common.StringPtr("ghi"),
		Resettable:     common.BoolPtr(false),
	}

	expiredNowNano := time.Now().UnixNano() - int64(time.Hour)
	notExpiredNowNano := time.Now().UnixNano() + int64(time.Hour)
	pt4 := &workflow.ResetPointInfo{
		BinaryChecksum:   common.StringPtr("expired"),
		Resettable:       common.BoolPtr(true),
		ExpiringTimeNano: common.Int64Ptr(expiredNowNano),
	}

	pt5 := &workflow.ResetPointInfo{
		BinaryChecksum:   common.StringPtr("notExpired"),
		Resettable:       common.BoolPtr(true),
		ExpiringTimeNano: common.Int64Ptr(notExpiredNowNano),
	}

	// case 3: two intersection
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"abc": {},
			"def": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt0.String())

	// case 4: one intersection
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none":    {},
			"def":     {},
			"expired": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt4, pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt1.String())

	// case 4: no intersection
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none1": {},
			"none2": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 5: not resettable
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none1": {},
			"ghi":   {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 6: one intersection of expired
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none":    {},
			"expired": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Nil(t, pt)

	// case 7: one intersection of not expired
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none":       {},
			"notExpired": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Equal(t, pt.String(), pt5.String())
}
