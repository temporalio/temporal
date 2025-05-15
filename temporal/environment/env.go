package environment

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

const (
	// LocalhostIP default localhost
	LocalhostIP = "LOCALHOST_IP"
	// Localhost default hostname
	LocalhostIPDefault = "127.0.0.1"

	// CassandraSeeds env
	CassandraSeeds = "CASSANDRA_SEEDS"
	// CassandraPort env
	CassandraPort = "CASSANDRA_PORT"
	// CassandraDefaultPort Cassandra default port
	CassandraDefaultPort = 9042

	// MySQLSeeds env
	MySQLSeeds = "MYSQL_SEEDS"
	// MySQLPort env
	MySQLPort = "MYSQL_PORT"
	// MySQLDefaultPort MySQL default port
	MySQLDefaultPort = 3306

	// ESSeeds env
	ESSeeds = "ES_SEEDS"
	// ESPort env
	ESPort = "ES_PORT"
	// ESDefaultPort ES default port
	ESDefaultPort = 9200
	// ESVersion is the ElasticSearch version
	ESVersion = "ES_VERSION"
	// ESDefaultVersion is the default version
	ESDefaultVersion = "v7"

	// PostgresSeeds env
	PostgresSeeds = "POSTGRES_SEEDS"
	// PostgresPort env
	PostgresPort = "POSTGRES_PORT"
	// PostgresDefaultPort Postgres default port
	PostgresDefaultPort = 5432
)

type varSpec struct {
	name       string
	getDefault func() string
}

var envVars = []varSpec{
	{
		name:       LocalhostIP,
		getDefault: func() string { return lookupLocalhostIP("localhost") },
	},
	{
		name:       CassandraSeeds,
		getDefault: GetLocalhostIP,
	},
	{
		name:       CassandraPort,
		getDefault: func() string { return strconv.Itoa(CassandraDefaultPort) },
	},
	{
		name:       MySQLSeeds,
		getDefault: GetLocalhostIP,
	},
	{
		name:       MySQLPort,
		getDefault: func() string { return strconv.Itoa(MySQLDefaultPort) },
	},
	{
		name:       PostgresSeeds,
		getDefault: GetLocalhostIP,
	},
	{
		name:       PostgresPort,
		getDefault: func() string { return strconv.Itoa(PostgresDefaultPort) },
	},
	{
		name:       ESSeeds,
		getDefault: GetLocalhostIP,
	},
	{
		name:       ESPort,
		getDefault: func() string { return strconv.Itoa(ESDefaultPort) },
	},
	{
		name:       ESVersion,
		getDefault: func() string { return ESDefaultVersion },
	},
}

// SetupEnv setup the necessary env
func SetupEnv() {
	for _, envVar := range envVars {
		if os.Getenv(envVar.name) == "" {
			if err := os.Setenv(envVar.name, envVar.getDefault()); err != nil {
				panic(fmt.Sprintf("error setting env var %s: %s", envVar.name, err))
			}
		}
	}
}

func lookupLocalhostIP(domain string) string {
	// lookup localhost and favor the first ipv4 address
	// unless there are only ipv6 addresses available
	ips, err := net.LookupIP(domain)
	if err != nil || len(ips) == 0 {
		// fallback to default instead of error
		return LocalhostIPDefault
	}
	var listenIp net.IP
	for _, ip := range ips {
		listenIp = ip
		if listenIp.To4() != nil {
			break
		}
	}
	return listenIp.String()
}

// GetLocalhostIP returns the ip address of the localhost domain
func GetLocalhostIP() string {
	localhostIP := os.Getenv(LocalhostIP)
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
	addr := os.Getenv(CassandraSeeds)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

// GetCassandraPort return the cassandra port
func GetCassandraPort() int {
	port := os.Getenv(CassandraPort)
	if port == "" {
		return CassandraDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", CassandraPort))
	}
	return p
}

func GetESAddress() string {
	addr := os.Getenv(ESSeeds)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

func GetESPort() int {
	port := os.Getenv(ESPort)
	if port == "" {
		return ESDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", ESPort))
	}
	return p
}

func GetESVersion() string {
	version := os.Getenv(ESVersion)
	if version == "" {
		version = ESDefaultVersion
	}
	return version
}

// GetMySQLAddress return the cassandra address
func GetMySQLAddress() string {
	addr := os.Getenv(MySQLSeeds)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

// GetMySQLPort return the MySQL port
func GetMySQLPort() int {
	port := os.Getenv(MySQLPort)
	if port == "" {
		return MySQLDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", MySQLPort))
	}
	return p
}

// GetPostgreSQLAddress return the Postgres address
func GetPostgreSQLAddress() string {
	addr := os.Getenv(PostgresSeeds)
	if addr == "" {
		addr = GetLocalhostIP()
	}
	return addr
}

// GetPostgreSQLPort return the Postgres port
func GetPostgreSQLPort() int {
	port := os.Getenv(PostgresPort)
	if port == "" {
		return PostgresDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", PostgresPort))
	}
	return p
}
