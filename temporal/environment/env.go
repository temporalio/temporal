package environment

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// Server code does not use environment variables directly.
// Server uses a config file which can be build using config_template.yaml and environment variables.
// Helpers in this file are used by:
//   - persistence tests,
//   - functional tests,
//   - database tools (default values for flags).
// The only exception is GetLocalhostIP() which is used to bind listeners (gRPC and ringpop).

const (
	localhostIPEnv     = "LOCALHOST_IP"
	localhostIPDefault = "127.0.0.1"

	cassandraSeedsEnv    = "CASSANDRA_SEEDS"
	cassandraPortEnv     = "CASSANDRA_PORT"
	cassandraDefaultPort = 9042

	mySQLSeedsEnv    = "MYSQL_SEEDS"
	mySQLPortEnv     = "MYSQL_PORT"
	mySQLDefaultPort = 3306

	esSeedsEnv       = "ES_SEEDS"
	esPortEnv        = "ES_PORT"
	esVersion        = "ES_VERSION"
	esDefaultPortEnv = 9200
	esDefaultVersion = "v7"

	postgresSeedsEnv    = "POSTGRES_SEEDS"
	postgresPortEnv     = "POSTGRES_PORT"
	postgresDefaultPort = 5432
)

func lookupLocalhostIP(domain string) string {
	// lookup localhost and favor the first ipv4 address
	// unless there are only ipv6 addresses available
	ips, err := net.LookupIP(domain)
	if err != nil || len(ips) == 0 {
		// fallback to default instead of error
		return localhostIPDefault
	}
	for _, ip := range ips {
		if ip4 := ip.To4(); ip4 != nil {
			return ip4.String()
		}
	}
	return ips[len(ips)-1].String()
}

// GetLocalhostIP returns the ip address of the localhost domain
func GetLocalhostIP() string {
	localhostIP := os.Getenv(localhostIPEnv)
	ip := net.ParseIP(localhostIP)
	if ip != nil {
		// if localhost is an ip return it
		return ip.String()
	}
	// otherwise, ignore the value and lookup `localhost`
	return lookupLocalhostIP("localhost")
}

// GetCassandraAddress return the cassandra address
func GetCassandraAddress() string {
	addr := os.Getenv(cassandraSeedsEnv)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

// GetCassandraPort return the cassandra port
func GetCassandraPort() int {
	port := os.Getenv(cassandraPortEnv)
	if port == "" {
		return cassandraDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		//nolint:forbidigo // used in test code only
		panic(fmt.Sprintf("error getting env %v", cassandraPortEnv))
	}
	return p
}

func GetESAddress() string {
	addr := os.Getenv(esSeedsEnv)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

func GetESPort() int {
	port := os.Getenv(esPortEnv)
	if port == "" {
		return esDefaultPortEnv
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		//nolint:forbidigo // used in test code only
		panic(fmt.Sprintf("error getting env %v", esPortEnv))
	}
	return p
}

func GetESVersion() string {
	version := os.Getenv(esVersion)
	if version == "" {
		version = esDefaultVersion
	}
	return version
}

// GetMySQLAddress return the cassandra address
func GetMySQLAddress() string {
	addr := os.Getenv(mySQLSeedsEnv)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

// GetMySQLPort return the MySQL port
func GetMySQLPort() int {
	port := os.Getenv(mySQLPortEnv)
	if port == "" {
		return mySQLDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		//nolint:forbidigo // used in test code only
		panic(fmt.Sprintf("error getting env %v", mySQLPortEnv))
	}
	return p
}

// GetPostgreSQLAddress return the Postgres address
func GetPostgreSQLAddress() string {
	addr := os.Getenv(postgresSeedsEnv)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

// GetPostgreSQLPort return the Postgres port
func GetPostgreSQLPort() int {
	port := os.Getenv(postgresPortEnv)
	if port == "" {
		return postgresDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		//nolint:forbidigo // used in test code only
		panic(fmt.Sprintf("error getting env %v", postgresPortEnv))
	}
	return p
}
