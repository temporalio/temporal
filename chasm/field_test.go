// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package chasm

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

type fieldSuite struct {
	suite.Suite
	*require.Assertions
	protorequire.ProtoAssertions

	controller  *gomock.Controller
	nodeBackend *MockNodeBackend

	registry        *Registry
	timeSource      *clock.EventTimeSource
	nodePathEncoder NodePathEncoder
	logger          log.Logger
}

func TestFieldSuite(t *testing.T) {
	suite.Run(t, new(fieldSuite))
}

func (s *fieldSuite) SetupTest() {
	s.initAssertions()
	s.controller = gomock.NewController(s.T())
	s.nodeBackend = NewMockNodeBackend(s.controller)

	s.registry = NewRegistry()
	err := s.registry.Register(newTestLibrary())
	s.NoError(err)

	s.timeSource = clock.NewEventTimeSource()
	s.nodePathEncoder = &testNodePathEncoder{}
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
}

func (s *fieldSuite) SetupSubTest() {
	s.initAssertions()
}

func (s *fieldSuite) initAssertions() {
	// `s.Assertions` (as well as other test helpers which depends on `s.T()`) must be initialized on
	// both test and subtest levels (but not suite level, where `s.T()` is `nil`).
	//
	// If these helpers are not reinitialized on subtest level, any failed `assert` in
	// subtest will fail the entire test (not subtest) immediately without running other subtests.

	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

func (s *fieldSuite) TestChasmFieldTypePrefix() {
	f := Field[any]{}
	fT := reflect.TypeOf(f)
	s.True(strings.HasPrefix(fT.String(), chasmFieldTypePrefix))
}

func (s *fieldSuite) TestChasmCollectionTypePrefix() {
	c := Collection[any]{}
	cT := reflect.TypeOf(c)
	s.True(strings.HasPrefix(cT.String(), chasmCollectionTypePrefix))
}

func (s *fieldSuite) TestInternalFieldName() {
	f := Field[any]{}
	fT := reflect.TypeOf(f)

	_, ok := fT.FieldByName(internalFieldName)
	s.True(ok, "expected field %s not found", internalFieldName)
}

func (s *fieldSuite) TestGenericTypePrefix() {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "Field type",
			input:    Field[string]{},
			expected: chasmFieldTypePrefix,
		},
		{
			name:     "Collection type",
			input:    Collection[int]{},
			expected: chasmCollectionTypePrefix,
		},
		{
			name:     "Non-generic type",
			input:    0,
			expected: "",
		},
		{
			name:     "Map type",
			input:    map[string]int{},
			expected: "map[",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			typ := reflect.TypeOf(tt.input)
			result := genericTypePrefix(typ)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *fieldSuite) TestFieldGetSimple() {
	tests := []struct {
		name     string
		field    Field[*TestSubComponent1]
		expected *TestSubComponent1
	}{
		{
			name: "Get with non-nil value",
			field: Field[*TestSubComponent1]{
				Internal: fieldInternal{
					v: &TestSubComponent1{SubComponent1Data: &protoMessageType{
						ActivityId: "component-data",
					}},
					ft: fieldTypeComponent,
				},
			},
			expected: &TestSubComponent1{SubComponent1Data: &protoMessageType{
				ActivityId: "component-data",
			}},
		},
		{
			name: "Get with nil value and nil node",
			field: Field[*TestSubComponent1]{
				Internal: fieldInternal{
					v:    nil,
					node: nil,
				},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result, err := tt.field.Get(nil)
			s.NoError(err)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *fieldSuite) TestFieldGetComponent() {
	serializedNodes := testComponentSerializedNodes()

	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	chasmContext := NewMutableContext(context.Background(), node)

	c, err := node.Component(chasmContext, ComponentRef{componentPath: RootPath})
	s.NoError(err)
	s.NotNil(c)

	tc := c.(*TestComponent)

	sc1, err := tc.SubComponent1.Get(chasmContext)
	s.NoError(err)
	s.NotNil(sc1)
	s.ProtoEqual(&protoMessageType{
		ActivityId: "sub-component1-data",
	}, sc1.SubComponent1Data)

	sd1, err := tc.SubData1.Get(chasmContext)
	s.NoError(err)
	s.NotNil(sd1)
	s.ProtoEqual(&protoMessageType{
		ActivityId: "sub-data1",
	}, sd1)
}
