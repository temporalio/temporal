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

package ringpop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
)

type RingpopSuite struct {
	*require.Assertions
	suite.Suite
}

func TestRingpopSuite(t *testing.T) {
	suite.Run(t, new(RingpopSuite))
}

func (s *RingpopSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *RingpopSuite) TestHostsMode() {
	var cfg config.Membership
	err := yaml.Unmarshal([]byte(getHostsConfig()), &cfg)
	s.Nil(err)
	s.Equal("1.2.3.4", cfg.BroadcastAddress)
	s.Equal(time.Second*30, cfg.MaxJoinDuration)
	err = ValidateRingpopConfig(&cfg)
	s.Nil(err)
	f, err := NewRingpopFactory(&cfg, nil, "test", nil, log.NewNoopLogger(), nil)
	s.Nil(err)
	s.NotNil(f)
}

type mockResolver struct {
	Hosts map[string][]string
}

func (resolver *mockResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	addrs, ok := resolver.Hosts[host]
	if !ok {
		return nil, fmt.Errorf("Host was not resolved: %s", host)
	}
	return addrs, nil
}

func (s *RingpopSuite) TestInvalidConfig() {
	var cfg config.Membership
	cfg.MaxJoinDuration = time.Minute
	s.NoError(ValidateRingpopConfig(&cfg))
	cfg.BroadcastAddress = "sjhdfskdjhf"
	s.Error(ValidateRingpopConfig(&cfg))
}

func getHostsConfig() string {
	return `name: "test"
broadcastAddress: "1.2.3.4"
maxJoinDuration: 30s`
}
