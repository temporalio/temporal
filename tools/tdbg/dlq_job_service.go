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

package tdbg

import (
	"encoding/base64"
	"fmt"
	"io"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
)

type (
	DLQJobService struct {
		clientFactory ClientFactory
		writer        io.Writer
	}
)

func NewDLQJobService(
	clientFactory ClientFactory,
	writer io.Writer,
) *DLQV2Service {
	return &DLQV2Service{
		clientFactory: clientFactory,
		writer:        writer,
	}
}

func (ac *DLQJobService) DescribeJob(c *cli.Context) error {
	adminClient := ac.clientFactory.AdminClient(c)
	jobToken := c.String(FlagJobToken)
	jobTokenBytes, err := base64.StdEncoding.DecodeString(jobToken)
	if err != nil {
		return fmt.Errorf("unable to parse job token: %w", err)
	}
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.DescribeDLQJob(ctx, &adminservice.DescribeDLQJobRequest{
		JobToken: jobTokenBytes,
	})
	if err != nil {
		return fmt.Errorf("call to DescribeDLQJob failed: %w", err)
	}
	err = newEncoder(ac.writer).Encode(response)
	if err != nil {
		return fmt.Errorf("unable to encode DescribeDLQJob response: %w", err)
	}
	return nil
}

func (ac *DLQJobService) CancelJob(c *cli.Context) error {
	adminClient := ac.clientFactory.AdminClient(c)
	jobToken := c.String(FlagJobToken)
	jobTokenBytes, err := base64.StdEncoding.DecodeString(jobToken)
	if err != nil {
		return fmt.Errorf("unable to parse job token: %w", err)
	}
	reason := c.String(FlagReason)
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.CancelDLQJob(ctx, &adminservice.CancelDLQJobRequest{
		JobToken: jobTokenBytes,
		Reason:   reason,
	})
	if err != nil {
		return fmt.Errorf("call to CancelDLQJob failed: %w", err)
	}
	err = newEncoder(ac.writer).Encode(response)
	if err != nil {
		return fmt.Errorf("unable to encode CancelDLQJob response: %w", err)
	}
	return nil
}
