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

package plugin

import (
	"testing"

	hplugin "github.com/hashicorp/go-plugin"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/tools/cli/dataconverter"
)

func (s *pluginSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}
func TestPluginSuite(t *testing.T) {
	suite.Run(t, new(pluginSuite))
}

type pluginSuite struct {
	*require.Assertions
	suite.Suite
}

func (s *pluginSuite) newDataConverterRPC() converter.DataConverter {
	rpcClient, _ := hplugin.TestPluginRPCConn(s.T(), map[string]hplugin.Plugin{
		"test": &DataConverterPlugin{Impl: dataconverter.GetCurrent()},
	}, nil)
	client, err := rpcClient.Dispense("test")
	s.NoError(err)

	dataConverter, ok := client.(converter.DataConverter)
	s.Equal(ok, true)

	return dataConverter
}

func (s *pluginSuite) TestFromPayloadRoundtrip() {
	dc := dataconverter.GetCurrent()

	input := map[string]string{
		"one":   "two",
		"three": "four",
	}

	payload, err := dc.ToPayload(input)
	s.NoError(err)

	dataConverterPlugin := s.newDataConverterRPC()

	result := map[string]string{}
	err = dataConverterPlugin.FromPayload(payload, &result)
	s.NoError(err)

	s.Equal(input, result)
}

func (s *pluginSuite) TestFromPayloadsRoundtrip() {
	dc := dataconverter.GetCurrent()

	input1 := "test"
	input2 := map[string]string{
		"one":   "two",
		"three": "four",
	}

	payloads, err := dc.ToPayloads(input1, input2)
	s.NoError(err)

	dataConverterPlugin := s.newDataConverterRPC()

	var result1 string
	result2 := map[string]string{}

	err = dataConverterPlugin.FromPayloads(payloads, &result1, &result2)
	s.NoError(err)

	s.Equal(input1, result1)
	s.Equal(input2, result2)
}
