package admin

import (
	"github.com/temporalio/temporal/.gen/proto/adminservice"
)

// Client is the interface exposed by admin service client
type Client interface {
	adminservice.AdminServiceClient
}
