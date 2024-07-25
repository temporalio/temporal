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

package frontend

import (
	"context"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
)

type (
	HealthChecker interface {
		Check(ctx context.Context) (enumsspb.HealthState, error)
	}

	healthCheckerImpl struct {
		serviceName           primitives.ServiceName
		membershipMonitor     membership.Monitor
		hostFailurePercentage dynamicconfig.FloatPropertyFn
		healthCheckFn         func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error)
		logger                log.Logger
	}
)

func NewHealthChecker(
	serviceName primitives.ServiceName,
	membershipMonitor membership.Monitor,
	hostFailurePercentage dynamicconfig.FloatPropertyFn,
	healthCheckFn func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error),
	logger log.Logger,
) HealthChecker {
	return &healthCheckerImpl{
		serviceName:           serviceName,
		membershipMonitor:     membershipMonitor,
		hostFailurePercentage: hostFailurePercentage,
		healthCheckFn:         healthCheckFn,
		logger:                logger,
	}
}

func (h *healthCheckerImpl) Check(ctx context.Context) (enumsspb.HealthState, error) {
	resolver, err := h.membershipMonitor.GetResolver(h.serviceName)
	if err != nil {
		return enumsspb.HEALTH_STATE_UNSPECIFIED, err
	}
	hosts := resolver.AvailableMembers()
	receiveCh := make(chan enumsspb.HealthState, len(hosts))
	for _, host := range hosts {
		go func(hostAddress string) {
			resp, err := h.healthCheckFn(
				ctx,
				hostAddress,
			)
			if err != nil {
				h.logger.Warn("Failed to ping deep health check", tag.Error(err), tag.ServerName(string(h.serviceName)))
			}
			receiveCh <- resp
		}(host.GetAddress())
	}

	var failedHostCount float64
	for i := 0; i < len(hosts); i++ {
		healthState := <-receiveCh
		if healthState == enumsspb.HEALTH_STATE_NOT_SERVING {
			failedHostCount++
		}
	}
	close(receiveCh)

	if (failedHostCount / float64(len(hosts))) > h.hostFailurePercentage() {
		return enumsspb.HEALTH_STATE_NOT_SERVING, nil
	}

	return enumsspb.HEALTH_STATE_SERVING, nil
}
