//go:build gomad

package config

import (
	"errors"
	"net"
)

func ListenIP() (net.IP, error) {
	return nil, errors.New("gomad: host interface discovery is unavailable; configure a listen IP")
}
