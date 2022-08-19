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

package queues

import (
	"sync"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/tasks"
)

var _ Monitor = (*monitorImpl)(nil)

const (
	monitorWatermarkPrecision = time.Second
)

type (
	Monitor interface {
		GetReaderWatermark(readerID int32) (tasks.Key, bool)
		SetReaderWatermark(readerID int32, watermark tasks.Key)
	}

	MonitorOptions struct {
		CriticalReaderWatermarkAttempts dynamicconfig.IntPropertyFn
	}

	monitorImpl struct {
		sync.Mutex

		mitigator Mitigator

		readerStats

		categoryType tasks.CategoryType
		options      *MonitorOptions
	}

	readerStats struct {
		progressByReader map[int32]*readerProgess
	}

	readerProgess struct {
		watermark tasks.Key
		attempts  int
	}
)

func newMonitor(
	categoryType tasks.CategoryType,
	options *MonitorOptions,
) *monitorImpl {
	return &monitorImpl{
		readerStats: readerStats{
			progressByReader: make(map[int32]*readerProgess),
		},
		categoryType: categoryType,
		options:      options,
	}
}

func (m *monitorImpl) GetReaderWatermark(readerID int32) (tasks.Key, bool) {
	m.Lock()
	defer m.Unlock()

	progress, ok := m.progressByReader[readerID]
	if !ok {
		return tasks.MinimumKey, false
	}
	return progress.watermark, true
}

func (m *monitorImpl) SetReaderWatermark(readerID int32, watermark tasks.Key) {
	// TODO: currently only tracking default reader progress for scheduled queue
	if readerID != defaultReaderId || m.categoryType != tasks.CategoryTypeScheduled {
		return
	}

	m.Lock()
	defer m.Unlock()

	watermark.FireTime = watermark.FireTime.Truncate(monitorWatermarkPrecision)
	watermark.TaskID = 0

	progress := m.progressByReader[readerID]
	if progress == nil {
		m.progressByReader[readerID] = &readerProgess{
			watermark: watermark,
			attempts:  1,
		}
		return
	}
	if progress.watermark.CompareTo(watermark) != 0 {
		progress.watermark = watermark
		progress.attempts = 1
		return
	}

	progress.attempts++

	ciriticalAttempts := m.options.CriticalReaderWatermarkAttempts()
	if ciriticalAttempts > 0 && progress.attempts > ciriticalAttempts && m.mitigator != nil {
		m.mitigator.Alert(Alert{
			AlertType: AlertTypeReaderWatermark,
			AlertReaderWatermarkAttributes: &AlertReaderWatermarkAttributes{
				ReaderID:         readerID,
				CurrentWatermark: progress.watermark,
			},
		})
	}
}

func (m *monitorImpl) registerMitigator(
	mitigator Mitigator,
) {
	if m.mitigator != nil {
		panic("Mitigator already registered on queue monitor")
	}

	m.mitigator = mitigator
}
