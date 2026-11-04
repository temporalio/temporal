//go:build !temporal_minimal || temporal_with_mysql

package config

func validateMySQL(string) error {
	return nil
}
