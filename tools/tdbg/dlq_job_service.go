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
	response, err := adminClient.DescribeDLQJob(ctx, adminservice.DescribeDLQJobRequest_builder{
		JobToken: jobTokenBytes,
	}.Build())
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
	response, err := adminClient.CancelDLQJob(ctx, adminservice.CancelDLQJobRequest_builder{
		JobToken: jobTokenBytes,
		Reason:   reason,
	}.Build())
	if err != nil {
		return fmt.Errorf("call to CancelDLQJob failed: %w", err)
	}
	err = newEncoder(ac.writer).Encode(response)
	if err != nil {
		return fmt.Errorf("unable to encode CancelDLQJob response: %w", err)
	}
	return nil
}
