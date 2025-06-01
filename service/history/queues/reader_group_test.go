package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common"
)

type (
	readerGroupSuite struct {
		suite.Suite
		*require.Assertions

		readerGroup *ReaderGroup
	}

	testReader struct {
		status int32
	}
)

func TestReaderGroupSuite(t *testing.T) {
	s := new(readerGroupSuite)
	suite.Run(t, s)
}

func (s *readerGroupSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.readerGroup = NewReaderGroup(
		func(_ int64, _ []Slice) Reader {
			return newTestReader()
		},
	)
}

func (s *readerGroupSuite) TestStartStop() {
	readerID := DefaultReaderId
	r := s.readerGroup.NewReader(readerID)
	s.Equal(common.DaemonStatusInitialized, r.(*testReader).status)

	s.readerGroup.Start()
	s.Equal(common.DaemonStatusStarted, r.(*testReader).status)

	readerID = DefaultReaderId + 1
	r = s.readerGroup.NewReader(readerID)
	s.Equal(common.DaemonStatusStarted, r.(*testReader).status)

	var readers []*testReader
	for _, reader := range s.readerGroup.Readers() {
		readers = append(readers, reader.(*testReader))
	}
	s.readerGroup.Stop()
	s.Len(readers, 2)
	for _, r := range readers {
		s.Equal(common.DaemonStatusStopped, r.status)
	}

	readerID = DefaultReaderId + 2
	r = s.readerGroup.NewReader(readerID)
	s.Equal(common.DaemonStatusInitialized, r.(*testReader).status)
}

func (s *readerGroupSuite) TestAddGetReader() {
	s.Empty(s.readerGroup.Readers())

	r, ok := s.readerGroup.ReaderByID(DefaultReaderId)
	s.False(ok)
	s.Nil(r)

	for i := int64(0); i < 3; i++ {
		r := s.readerGroup.NewReader(i)

		readers := s.readerGroup.Readers()
		s.Len(readers, int(i)+1)
		s.Equal(r, readers[i])

		retrievedReader, ok := s.readerGroup.ReaderByID(i)
		s.True(ok)
		s.Equal(r, retrievedReader)
	}

	s.Panics(func() {
		s.readerGroup.NewReader(DefaultReaderId)
	})
}

func (s *readerGroupSuite) TestRemoveReader() {
	s.readerGroup.Start()
	defer s.readerGroup.Stop()

	readerID := DefaultReaderId

	r := s.readerGroup.NewReader(readerID)
	s.readerGroup.RemoveReader(readerID)

	s.Equal(common.DaemonStatusStopped, r.(*testReader).status)
	s.Len(s.readerGroup.Readers(), 0)
}

func (s *readerGroupSuite) TestForEach() {
	readerIDs := []int64{1, 2, 3}
	for _, readerID := range readerIDs {
		_ = s.readerGroup.NewReader(readerID)
	}

	forEachResult := make(map[int64]Reader)
	s.readerGroup.ForEach(func(i int64, r Reader) {
		forEachResult[i] = r
	})

	s.Equal(s.readerGroup.Readers(), forEachResult)
}

func newTestReader() Reader {
	return &testReader{
		status: common.DaemonStatusInitialized,
	}
}

func (r *testReader) Start()                       { r.status = common.DaemonStatusStarted }
func (r *testReader) Stop()                        { r.status = common.DaemonStatusStopped }
func (r *testReader) Scopes() []Scope              { panic("not implemented") }
func (r *testReader) WalkSlices(SliceIterator)     { panic("not implemented") }
func (r *testReader) SplitSlices(SliceSplitter)    { panic("not implemented") }
func (r *testReader) MergeSlices(...Slice)         { panic("not implemented") }
func (r *testReader) AppendSlices(...Slice)        { panic("not implemented") }
func (r *testReader) ClearSlices(SlicePredicate)   { panic("not implemented") }
func (r *testReader) CompactSlices(SlicePredicate) { panic("not implemented") }
func (r *testReader) ShrinkSlices() int            { panic("not implemented") }
func (r *testReader) Notify()                      { panic("not implemented") }
func (r *testReader) Pause(time.Duration)          { panic("not implemented") }
