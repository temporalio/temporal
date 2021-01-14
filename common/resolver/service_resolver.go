//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination service_resolver_mock.go

package resolver

import (
	"time"
)

type (
	ServiceResolver interface {
		RefreshInterval() time.Duration
		Resolve(service string) []string
	}
)
