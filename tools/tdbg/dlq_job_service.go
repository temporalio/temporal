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
	"fmt"
	"io"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
)

type (
	DLQJobService struct {
		clientFactory ClientFactory
		writer        io.Writer
		//prompter        *Prompter
		//taskBlobEncoder TaskBlobEncoder
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
	jobToken, err := ac.getJobToken(c)
	if err != nil {
		return err
	}
	ctx, cancel := newContext(c)
	defer cancel()

	response, err := adminClient.DescribeDLQJob(ctx, &adminservice.DescribeDLQJobRequest{
		JobToken: jobToken,
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
	jobToken, err := ac.getJobToken(c)
	if err != nil {
		return err
	}
	reason, err := ac.getReason(c)
	if err != nil {
		return err
	}
	ctx, cancel := newContext(c)
	defer cancel()

	response, err := adminClient.CancelDLQJob(ctx, &adminservice.CancelDLQJobRequest{
		JobToken: jobToken,
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

func (ac *DLQJobService) getJobToken(c *cli.Context) (string, error) {
	if !c.IsSet(FlagJobToken) {
		return "", fmt.Errorf(
			"--%s must be set",
			FlagJobToken,
		)
	}
	jobID := c.String(FlagJobToken)
	return jobID, nil
}

func (ac *DLQJobService) getReason(c *cli.Context) (string, error) {
	if !c.IsSet(FlagReason) {
		return "", fmt.Errorf(
			"--%s must be set",
			FlagReason,
		)
	}
	reason := c.String(FlagReason)
	return reason, nil
}
