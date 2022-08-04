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

package translator

import (
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
)

type (
	fixedTranslatorPluginTestSuite struct {
		suite.Suite
		controller *gomock.Controller
	}
)

func TestSessionTestSuite(t *testing.T) {
	s := new(fixedTranslatorPluginTestSuite)
	suite.Run(t, s)
}

func (s *fixedTranslatorPluginTestSuite) SetupSuite() {

}

func (s *fixedTranslatorPluginTestSuite) TearDownSuite() {

}

func (s *fixedTranslatorPluginTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
}

func (s *fixedTranslatorPluginTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *fixedTranslatorPluginTestSuite) TestFixedAddressTranslator() {
	plugin := &FixedAddressTranslatorPlugin{}

	configuration := &config.Cassandra{
		AddressTranslator: &config.CassandraAddressTranslator{
			Translator: fixedTranslatorName,
			Options:    map[string]string{advertisedHostnameKey: "temporal.io"},
		},
	}

	lookupIP, err := net.LookupIP("temporal.io")
	if err != nil {
		s.Errorf(err, "fail to lookup IP for temporal.io")
	}

	ipToExpect := lookupIP[0].To4()

	translator, err := plugin.GetTranslator(configuration)
	translatedHost, translatedPort := translator.Translate(net.ParseIP("1.1.1.1"), 6001)

	s.Equal(ipToExpect, translatedHost)
	s.Equal(6001, translatedPort)

	s.Equal(nil, err)
}
