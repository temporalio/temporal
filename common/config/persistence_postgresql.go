//go:build !temporal_minimal || temporal_with_postgresql

package config

func validatePostgreSQL(string) error {
	return nil
}
