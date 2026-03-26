//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination listener_mock.go

package common

import (
	"net"
)

// ListenerProvider is an interface which produces gRPC and membership/ringpop listeners. Used in the server FX
// initialization routines.
//
// The production implementation of ListenerProvider is provided by ConfigListenerProvider, which uses the Config
// to determine the ports it should listen on. Testing implementations are provided by the nettest package.
type ListenerProvider interface {
	GetGRPCListener() net.Listener
	GetMembershipListener() net.Listener
}
