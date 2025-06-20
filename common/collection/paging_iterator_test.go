package collection

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	pagingIteratorSuite struct {
		suite.Suite
	}
)

func TestPagingIteratorSuite(t *testing.T) {
	s := new(pagingIteratorSuite)
	suite.Run(t, s)
}

func (s *pagingIteratorSuite) SetupSuite() {
}

func (s *pagingIteratorSuite) TearDownSuite() {

}

func (s *pagingIteratorSuite) SetupTest() {

}

func (s *pagingIteratorSuite) TearDownTest() {

}

func (s *pagingIteratorSuite) TestIteration_NoErr() {
	phase := 0
	outputs := [][]int{
		{1, 2, 3, 4, 5},
		{},
		{6},
		{},
	}
	tokens := [][]byte{
		[]byte("some random token 1"),
		[]byte("some random token 2"),
		[]byte("some random token 3"),
		[]byte(nil),
	}
	pagingFn := func(token []byte) ([]int, []byte, error) {
		switch phase {
		case 0:
			s.Equal(0, len(token))
			defer func() { phase++ }()
			return outputs[phase], tokens[phase], nil
		case 1:
			s.Equal(tokens[0], token)
			defer func() { phase++ }()
			return outputs[phase], tokens[phase], nil
		case 2:
			s.Equal(tokens[1], token)
			defer func() { phase++ }()
			return outputs[phase], tokens[phase], nil
		case 3:
			s.Equal(tokens[2], token)
			defer func() { phase++ }()
			return outputs[phase], tokens[phase], nil
		default:
			panic("should not reach here during test")
		}
	}

	result := []int{}
	ite := NewPagingIterator(pagingFn)
	for ite.HasNext() {
		num, err := ite.Next()
		s.Nil(err)
		result = append(result, num)
	}
	s.Equal([]int{1, 2, 3, 4, 5, 6}, result)
}

func (s *pagingIteratorSuite) TestIteration_Err_Beginging() {
	phase := 0
	ite := NewPagingIterator(func(token []byte) ([]interface{}, []byte, error) {
		switch phase {
		case 0:
			defer func() { phase++ }()
			return nil, nil, errors.New("some random error")
		default:
			panic("should not reach here during test")
		}
	})

	s.True(ite.HasNext())
	item, err := ite.Next()
	s.Nil(item)
	s.NotNil(err)
	s.False(ite.HasNext())
}

func (s *pagingIteratorSuite) TestIteration_Err_NotBegining() {

	phase := 0
	outputs := [][]interface{}{
		{1, 2, 3, 4, 5},
	}
	tokens := [][]byte{
		[]byte("some random token 1"),
	}
	pagingFn := func(token []byte) ([]interface{}, []byte, error) {
		switch phase {
		case 0:
			s.Equal(0, len(token))
			defer func() { phase++ }()
			return outputs[phase], tokens[phase], nil
		case 1:
			s.Equal(tokens[0], token)
			defer func() { phase++ }()
			return nil, nil, errors.New("some random error")
		default:
			panic("should not reach here during test")
		}
	}

	result := []int{}
	ite := NewPagingIterator(pagingFn)
	for ite.HasNext() {
		item, err := ite.Next()
		if err != nil {
			break
		}
		num, ok := item.(int)
		s.True(ok)
		result = append(result, num)
	}
	s.Equal([]int{1, 2, 3, 4, 5}, result)
}
