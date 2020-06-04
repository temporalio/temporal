// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package shard

import (
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

type FixerSuite struct {
	*require.Assertions
	suite.Suite
	controller *gomock.Controller
}

func TestFixerSuite(t *testing.T) {
	suite.Run(t, new(FixerSuite))
}

func (s *FixerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *FixerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *FixerSuite) TestFix_Failure_FirstIteratorError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	mockItr.EXPECT().HasNext().Return(true).Times(1)
	mockItr.EXPECT().Next().Return(nil, errors.New("iterator error")).Times(1)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "blobstore iterator returned error",
				InfoDetails: "iterator error",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Failure_NonFirstError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	iteratorCallNumber := 0
	mockItr.EXPECT().HasNext().DoAndReturn(func() bool {
		return iteratorCallNumber < 5
	}).Times(5)
	mockItr.EXPECT().Next().DoAndReturn(func() (*common.ScanOutputEntity, error) {
		defer func() {
			iteratorCallNumber++
		}()
		if iteratorCallNumber < 4 {
			return &common.ScanOutputEntity{}, nil
		}
		return nil, fmt.Errorf("iterator got error on: %v", iteratorCallNumber)
	}).Times(5)
	mockInvariantManager := common.NewMockInvariantManager(s.controller)
	mockInvariantManager.EXPECT().RunFixes(gomock.Any()).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeFixed,
	}).Times(4)
	fixedWriter := common.NewMockExecutionWriter(s.controller)
	fixedWriter.EXPECT().Add(gomock.Any()).Return(nil).Times(4)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		invariantManager: mockInvariantManager,
		fixedWriter:      fixedWriter,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Stats: common.ShardFixStats{
			ExecutionCount: 4,
			FixedCount:     4,
		},
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "blobstore iterator returned error",
				InfoDetails: "iterator got error on: 4",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Failure_SkippedWriterError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	mockItr.EXPECT().HasNext().Return(true).Times(1)
	mockItr.EXPECT().Next().Return(&common.ScanOutputEntity{}, nil).Times(1)
	mockInvariantManager := common.NewMockInvariantManager(s.controller)
	mockInvariantManager.EXPECT().RunFixes(gomock.Any()).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeSkipped,
	}).Times(1)
	skippedWriter := common.NewMockExecutionWriter(s.controller)
	skippedWriter.EXPECT().Add(gomock.Any()).Return(errors.New("skipped writer error")).Times(1)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		skippedWriter:    skippedWriter,
		invariantManager: mockInvariantManager,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Stats: common.ShardFixStats{
			ExecutionCount: 1,
		},
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "blobstore add failed for skipped execution fix",
				InfoDetails: "skipped writer error",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Failure_FailedWriterError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	mockItr.EXPECT().HasNext().Return(true).Times(1)
	mockItr.EXPECT().Next().Return(&common.ScanOutputEntity{}, nil).Times(1)
	mockInvariantManager := common.NewMockInvariantManager(s.controller)
	mockInvariantManager.EXPECT().RunFixes(gomock.Any()).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeFailed,
	}).Times(1)
	failedWriter := common.NewMockExecutionWriter(s.controller)
	failedWriter.EXPECT().Add(gomock.Any()).Return(errors.New("failed writer error")).Times(1)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		failedWriter:     failedWriter,
		invariantManager: mockInvariantManager,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Stats: common.ShardFixStats{
			ExecutionCount: 1,
		},
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "blobstore add failed for failed execution fix",
				InfoDetails: "failed writer error",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Failure_FixedWriterError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	mockItr.EXPECT().HasNext().Return(true).Times(1)
	mockItr.EXPECT().Next().Return(&common.ScanOutputEntity{}, nil).Times(1)
	mockInvariantManager := common.NewMockInvariantManager(s.controller)
	mockInvariantManager.EXPECT().RunFixes(gomock.Any()).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeFixed,
	}).Times(1)
	fixedWriter := common.NewMockExecutionWriter(s.controller)
	fixedWriter.EXPECT().Add(gomock.Any()).Return(errors.New("fixed writer error")).Times(1)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		fixedWriter:      fixedWriter,
		invariantManager: mockInvariantManager,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Stats: common.ShardFixStats{
			ExecutionCount: 1,
		},
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "blobstore add failed for fixed execution fix",
				InfoDetails: "fixed writer error",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Failure_FixedWriterFlushError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	mockItr.EXPECT().HasNext().Return(false).Times(1)
	fixedWriter := common.NewMockExecutionWriter(s.controller)
	fixedWriter.EXPECT().Flush().Return(errors.New("fix writer flush failed")).Times(1)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		fixedWriter:      fixedWriter,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "failed to flush for fixed execution fixes",
				InfoDetails: "fix writer flush failed",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Failure_SkippedWriterFlushError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	mockItr.EXPECT().HasNext().Return(false).Times(1)
	fixedWriter := common.NewMockExecutionWriter(s.controller)
	fixedWriter.EXPECT().Flush().Return(nil)
	skippedWriter := common.NewMockExecutionWriter(s.controller)
	skippedWriter.EXPECT().Flush().Return(errors.New("skip writer flush failed")).Times(1)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		fixedWriter:      fixedWriter,
		skippedWriter:    skippedWriter,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "failed to flush for skipped execution fixes",
				InfoDetails: "skip writer flush failed",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Failure_FailedWriterFlushError() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	mockItr.EXPECT().HasNext().Return(false).Times(1)
	fixedWriter := common.NewMockExecutionWriter(s.controller)
	fixedWriter.EXPECT().Flush().Return(nil)
	skippedWriter := common.NewMockExecutionWriter(s.controller)
	skippedWriter.EXPECT().Flush().Return(nil).Times(1)
	failedWriter := common.NewMockExecutionWriter(s.controller)
	failedWriter.EXPECT().Flush().Return(errors.New("fail writer flush failed")).Times(1)
	fixer := &fixer{
		shardID:          0,
		itr:              mockItr,
		fixedWriter:      fixedWriter,
		skippedWriter:    skippedWriter,
		failedWriter:     failedWriter,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Result: common.ShardFixResult{
			ControlFlowFailure: &common.ControlFlowFailure{
				Info:        "failed to flush for failed execution fixes",
				InfoDetails: "fail writer flush failed",
			},
		},
	}, result)
}

func (s *FixerSuite) TestFix_Success() {
	mockItr := common.NewMockScanOutputIterator(s.controller)
	iteratorCallNumber := 0
	mockItr.EXPECT().HasNext().DoAndReturn(func() bool {
		return iteratorCallNumber < 10
	}).Times(11)
	mockItr.EXPECT().Next().DoAndReturn(func() (*common.ScanOutputEntity, error) {
		defer func() {
			iteratorCallNumber++
		}()
		switch iteratorCallNumber {
		case 0, 1, 2, 3:
			return &common.ScanOutputEntity{
				Execution: common.Execution{
					DomainID: "skipped",
				},
			}, nil
		case 4, 5:
			return &common.ScanOutputEntity{
				Execution: common.Execution{
					DomainID: "history_missing",
				},
			}, nil
		case 6:
			return &common.ScanOutputEntity{
				Execution: common.Execution{
					DomainID: "first_history_event",
				},
			}, nil
		case 7:
			return &common.ScanOutputEntity{
				Execution: common.Execution{
					DomainID: "orphan_execution",
				},
			}, nil
		case 8, 9:
			return &common.ScanOutputEntity{
				Execution: common.Execution{
					DomainID: "failed",
				},
			}, nil
		default:
			panic("should not get here")
		}
	}).Times(10)
	mockInvariantManager := common.NewMockInvariantManager(s.controller)
	mockInvariantManager.EXPECT().RunFixes(common.Execution{
		DomainID: "skipped",
	}).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeSkipped,
		FixResults: []common.FixResult{
			{
				FixResultType: common.FixResultTypeSkipped,
				InvariantType: common.HistoryExistsInvariantType,
			},
			{
				FixResultType: common.FixResultTypeSkipped,
				InvariantType: common.ValidFirstEventInvariantType,
			},
			{
				FixResultType: common.FixResultTypeSkipped,
				InvariantType: common.OpenCurrentExecutionInvariantType,
			},
		},
	}).Times(4)
	mockInvariantManager.EXPECT().RunFixes(common.Execution{
		DomainID: "history_missing",
	}).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeFixed,
		FixResults: []common.FixResult{
			{
				FixResultType: common.FixResultTypeFixed,
				InvariantType: common.HistoryExistsInvariantType,
				Info:          "history did not exist",
			},
		},
	}).Times(2)
	mockInvariantManager.EXPECT().RunFixes(common.Execution{
		DomainID: "first_history_event",
	}).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeFixed,
		FixResults: []common.FixResult{
			{
				FixResultType: common.FixResultTypeSkipped,
				InvariantType: common.HistoryExistsInvariantType,
			},
			{
				FixResultType: common.FixResultTypeFixed,
				InvariantType: common.ValidFirstEventInvariantType,
				Info:          "first event is not valid",
			},
		},
	}).Times(1)
	mockInvariantManager.EXPECT().RunFixes(common.Execution{
		DomainID: "orphan_execution",
		State:    persistence.WorkflowStateCreated,
	}).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeFixed,
		FixResults: []common.FixResult{
			{
				FixResultType: common.FixResultTypeSkipped,
				InvariantType: common.HistoryExistsInvariantType,
			},
			{
				FixResultType: common.FixResultTypeSkipped,
				InvariantType: common.ValidFirstEventInvariantType,
			},
			{
				FixResultType: common.FixResultTypeFixed,
				InvariantType: common.OpenCurrentExecutionInvariantType,
				Info:          "execution was orphan",
			},
		},
	}).Times(1)
	mockInvariantManager.EXPECT().RunFixes(common.Execution{
		DomainID: "failed",
	}).Return(common.ManagerFixResult{
		FixResultType: common.FixResultTypeFailed,
		FixResults: []common.FixResult{
			{
				FixResultType: common.FixResultTypeFailed,
				InvariantType: common.HistoryExistsInvariantType,
				Info:          "failed to check if history exists",
			},
		},
	}).Times(2)

	mockFixedWriter := common.NewMockExecutionWriter(s.controller)
	mockFixedWriter.EXPECT().Add(common.FixOutputEntity{
		Execution: common.Execution{
			DomainID: "history_missing",
		},
		Input: common.ScanOutputEntity{
			Execution: common.Execution{
				DomainID: "history_missing",
			},
		},
		Result: common.ManagerFixResult{
			FixResultType: common.FixResultTypeFixed,
			FixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeFixed,
					InvariantType: common.HistoryExistsInvariantType,
					Info:          "history did not exist",
				},
			},
		},
	}).Times(2)
	mockFixedWriter.EXPECT().Add(common.FixOutputEntity{
		Execution: common.Execution{
			DomainID: "first_history_event",
		},
		Input: common.ScanOutputEntity{
			Execution: common.Execution{
				DomainID: "first_history_event",
			},
		},
		Result: common.ManagerFixResult{
			FixResultType: common.FixResultTypeFixed,
			FixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeSkipped,
					InvariantType: common.HistoryExistsInvariantType,
				},
				{
					FixResultType: common.FixResultTypeFixed,
					InvariantType: common.ValidFirstEventInvariantType,
					Info:          "first event is not valid",
				},
			},
		},
	}).Times(1)
	mockFixedWriter.EXPECT().Add(common.FixOutputEntity{
		Execution: common.Execution{
			DomainID: "orphan_execution",
		},
		Input: common.ScanOutputEntity{
			Execution: common.Execution{
				DomainID: "orphan_execution",
			},
		},
		Result: common.ManagerFixResult{
			FixResultType: common.FixResultTypeFixed,
			FixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeSkipped,
					InvariantType: common.HistoryExistsInvariantType,
				},
				{
					FixResultType: common.FixResultTypeSkipped,
					InvariantType: common.ValidFirstEventInvariantType,
				},
				{
					FixResultType: common.FixResultTypeFixed,
					InvariantType: common.OpenCurrentExecutionInvariantType,
					Info:          "execution was orphan",
				},
			},
		},
	}).Times(1)
	mockFailedWriter := common.NewMockExecutionWriter(s.controller)
	mockFailedWriter.EXPECT().Add(common.FixOutputEntity{
		Execution: common.Execution{
			DomainID: "failed",
		},
		Input: common.ScanOutputEntity{
			Execution: common.Execution{
				DomainID: "failed",
			},
		},
		Result: common.ManagerFixResult{
			FixResultType: common.FixResultTypeFailed,
			FixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeFailed,
					InvariantType: common.HistoryExistsInvariantType,
					Info:          "failed to check if history exists",
				},
			},
		},
	}).Times(2)
	mockSkippedWriter := common.NewMockExecutionWriter(s.controller)
	mockSkippedWriter.EXPECT().Add(common.FixOutputEntity{
		Execution: common.Execution{
			DomainID: "skipped",
		},
		Input: common.ScanOutputEntity{
			Execution: common.Execution{
				DomainID: "skipped",
			},
		},
		Result: common.ManagerFixResult{
			FixResultType: common.FixResultTypeSkipped,
			FixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeSkipped,
					InvariantType: common.HistoryExistsInvariantType,
				},
				{
					FixResultType: common.FixResultTypeSkipped,
					InvariantType: common.ValidFirstEventInvariantType,
				},
				{
					FixResultType: common.FixResultTypeSkipped,
					InvariantType: common.OpenCurrentExecutionInvariantType,
				},
			},
		},
	}).Times(4)
	mockSkippedWriter.EXPECT().Flush().Return(nil)
	mockFailedWriter.EXPECT().Flush().Return(nil)
	mockFixedWriter.EXPECT().Flush().Return(nil)
	mockSkippedWriter.EXPECT().FlushedKeys().Return(&common.Keys{UUID: "skipped_keys_uuid"})
	mockFailedWriter.EXPECT().FlushedKeys().Return(&common.Keys{UUID: "failed_keys_uuid"})
	mockFixedWriter.EXPECT().FlushedKeys().Return(&common.Keys{UUID: "fixed_keys_uuid"})

	fixer := &fixer{
		shardID:          0,
		invariantManager: mockInvariantManager,
		skippedWriter:    mockSkippedWriter,
		failedWriter:     mockFailedWriter,
		fixedWriter:      mockFixedWriter,
		itr:              mockItr,
		progressReportFn: func() {},
	}
	result := fixer.Fix()
	s.Equal(common.ShardFixReport{
		ShardID: 0,
		Stats: common.ShardFixStats{
			ExecutionCount: 10,
			FixedCount:     4,
			SkippedCount:   4,
			FailedCount:    2,
		},
		Result: common.ShardFixResult{
			ShardFixKeys: &common.ShardFixKeys{
				Fixed:   &common.Keys{UUID: "fixed_keys_uuid"},
				Failed:  &common.Keys{UUID: "failed_keys_uuid"},
				Skipped: &common.Keys{UUID: "skipped_keys_uuid"},
			},
		},
	}, result)
}
