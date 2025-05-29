//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination service_resolver_mock.go

package resolver

type (
	// ServiceResolver interface can be implemented to support custom name resolving
	// for any dependency service.
	ServiceResolver interface {
		// Resolve implementation should return list of addresses for the service
		// (not necessary IP addresses but addresses that service dependency library accepts).
		Resolve(service string) []string
	}
)
