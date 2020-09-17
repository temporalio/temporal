// The MIT License (MIT)
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

package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore/filestore"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/reconciliation/iterator"
	"github.com/uber/cadence/common/service/config"
)

var (
	validBranchToken  = []byte{89, 11, 0, 10, 0, 0, 0, 12, 116, 101, 115, 116, 45, 116, 114, 101, 101, 45, 105, 100, 11, 0, 20, 0, 0, 0, 14, 116, 101, 115, 116, 45, 98, 114, 97, 110, 99, 104, 45, 105, 100, 0}
	executionPageSize = 10
	testShardID       = 1
)

type WriterIteratorSuite struct {
	*require.Assertions
	suite.Suite
}

func TestWriterIteratorSuite(t *testing.T) {
	suite.Run(t, new(WriterIteratorSuite))
}

func (s *WriterIteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *WriterIteratorSuite) TestWriterIterator() {
	pr := persistence.NewPersistenceRetryer(getMockExecutionManager(10, 10), nil, common.CreatePersistenceRetryPolicy())
	pItr := iterator.ConcreteExecution(pr, executionPageSize)

	uuid := "uuid"
	extension := Extension("test")
	outputDir, err := ioutil.TempDir("", "TestWriterIterator")
	s.NoError(err)
	defer os.RemoveAll(outputDir)
	cfg := &config.FileBlobstore{
		OutputDirectory: outputDir,
	}
	blobstore, err := filestore.NewFilestoreClient(cfg)
	s.NoError(err)
	blobstoreWriter := NewBlobstoreWriter(uuid, extension, blobstore, 10)
	var outputs []*ScanOutputEntity
	for pItr.HasNext() {
		exec, err := pItr.Next()
		s.NoError(err)
		soe := &ScanOutputEntity{
			Execution: exec,
		}
		outputs = append(outputs, soe)
		s.NoError(blobstoreWriter.Add(soe))
	}
	s.NoError(blobstoreWriter.Flush())
	s.Len(outputs, 100)
	s.False(pItr.HasNext())
	_, err = pItr.Next()
	s.Equal(pagination.ErrIteratorFinished, err)
	flushedKeys := blobstoreWriter.FlushedKeys()
	s.Equal(uuid, flushedKeys.UUID)
	s.Equal(0, flushedKeys.MinPage)
	s.Equal(9, flushedKeys.MaxPage)
	s.Equal(Extension("test"), flushedKeys.Extension)
	blobstoreItr := NewBlobstoreIterator(blobstore, *flushedKeys, &entity.ConcreteExecution{})
	i := 0
	s.True(blobstoreItr.HasNext())
	for blobstoreItr.HasNext() {
		exec, err := blobstoreItr.Next()
		s.NoError(err)
		s.Equal(*outputs[i], *exec)
		i++
	}
}

func getMockExecutionManager(pages int, countPerPage int) persistence.ExecutionManager {
	execManager := &mocks.ExecutionManager{}
	for i := 0; i < pages; i++ {
		req := &persistence.ListConcreteExecutionsRequest{
			PageToken: []byte(fmt.Sprintf("token_%v", i)),
			PageSize:  executionPageSize,
		}
		if i == 0 {
			req.PageToken = nil
		}
		resp := &persistence.ListConcreteExecutionsResponse{
			Executions: getExecutions(countPerPage),
			PageToken:  []byte(fmt.Sprintf("token_%v", i+1)),
		}
		if i == pages-1 {
			resp.PageToken = nil
		}
		execManager.On("ListConcreteExecutions", req).Return(resp, nil)
		execManager.On("GetShardID").Return(testShardID)
	}
	return execManager
}

func getExecutions(count int) []*persistence.ListConcreteExecutionsEntity {
	var result []*persistence.ListConcreteExecutionsEntity
	for i := 0; i < count; i++ {
		execution := &persistence.ListConcreteExecutionsEntity{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				DomainID:    uuid.New(),
				WorkflowID:  uuid.New(),
				RunID:       uuid.New(),
				BranchToken: validBranchToken,
				State:       0,
			},
		}
		if i%2 == 0 {
			execution.ExecutionInfo.BranchToken = nil
			execution.VersionHistories = &persistence.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*persistence.VersionHistory{
					{
						BranchToken: validBranchToken,
					},
				},
			}
		}
		result = append(result, execution)
	}
	return result
}
