//go:build !temporal_minimal || temporal_with_cassandra

package config

func validateCassandra(*Cassandra) error {
	return nil
}
