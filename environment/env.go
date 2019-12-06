// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package environment

import (
	"fmt"
	"os"
	"strconv"
)

const (
	// Localhost default localhost
	Localhost = "127.0.0.1"

	// CassandraSeeds env
	CassandraSeeds = "CASSANDRA_SEEDS"
	// CassandraPort env
	CassandraPort = "CASSANDRA_PORT"
	// CassandraDefaultPort Cassandra default port
	CassandraDefaultPort = "9042"

	// MySQLSeeds env
	MySQLSeeds = "MYSQL_SEEDS"
	// MySQLPort env
	MySQLPort = "MYSQL_PORT"
	// MySQLDefaultPort MySQL default port
	MySQLDefaultPort = "3306"

	// KafkaSeeds env
	KafkaSeeds = "KAFKA_SEEDS"
	// KafkaPort env
	KafkaPort = "KAFKA_PORT"
	// KafkaDefaultPort Kafka default port
	KafkaDefaultPort = "9092"

	// ESSeeds env
	ESSeeds = "ES_SEEDS"
	// ESPort env
	ESPort = "ES_PORT"
	// ESDefaultPort ES default port
	ESDefaultPort = "9200"

	// PostgresSeeds env
	PostgresSeeds = "POSTGRES_SEEDS"
	// PostgresPort env
	PostgresPort = "POSTGRES_PORT"
	// PostgresDefaultPort Postgres default port
	PostgresDefaultPort = "5432"
)

// SetupEnv setup the necessary env
func SetupEnv() {
	if os.Getenv(CassandraSeeds) == "" {
		err := os.Setenv(CassandraSeeds, Localhost)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", CassandraSeeds))
		}
	}

	if os.Getenv(CassandraPort) == "" {
		err := os.Setenv(CassandraPort, CassandraDefaultPort)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", CassandraPort))
		}
	}

	if os.Getenv(MySQLSeeds) == "" {
		err := os.Setenv(MySQLSeeds, Localhost)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", MySQLSeeds))
		}
	}

	if os.Getenv(MySQLPort) == "" {
		err := os.Setenv(MySQLPort, MySQLDefaultPort)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", MySQLPort))
		}
	}

	if os.Getenv(PostgresSeeds) == "" {
		err := os.Setenv(PostgresSeeds, Localhost)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", PostgresSeeds))
		}
	}

	if os.Getenv(PostgresPort) == "" {
		err := os.Setenv(PostgresPort, PostgresDefaultPort)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", PostgresPort))
		}
	}

	if os.Getenv(KafkaSeeds) == "" {
		err := os.Setenv(KafkaSeeds, Localhost)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", KafkaSeeds))
		}
	}

	if os.Getenv(KafkaPort) == "" {
		err := os.Setenv(KafkaPort, KafkaDefaultPort)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", KafkaPort))
		}
	}

	if os.Getenv(ESSeeds) == "" {
		err := os.Setenv(ESSeeds, Localhost)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", ESSeeds))
		}
	}

	if os.Getenv(ESPort) == "" {
		err := os.Setenv(ESPort, ESDefaultPort)
		if err != nil {
			panic(fmt.Sprintf("error setting env %v", ESPort))
		}
	}
}

// GetCassandraAddress return the cassandra address
func GetCassandraAddress() string {
	addr := os.Getenv(CassandraSeeds)
	if addr == "" {
		addr = Localhost
	}
	return addr
}

// GetCassandraPort return the cassandra port
func GetCassandraPort() int {
	port := os.Getenv(CassandraPort)
	if port == "" {
		port = CassandraDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", CassandraPort))
	}
	return p
}

// GetMySQLAddress return the cassandra address
func GetMySQLAddress() string {
	addr := os.Getenv(MySQLSeeds)
	if addr == "" {
		addr = Localhost
	}
	return addr
}

// GetMySQLPort return the MySQL port
func GetMySQLPort() int {
	port := os.Getenv(MySQLPort)
	if port == "" {
		port = MySQLDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", MySQLPort))
	}
	return p
}

// GetPostgresAddress return the cassandra address
func GetPostgresAddress() string {
	addr := os.Getenv(PostgresSeeds)
	if addr == "" {
		addr = Localhost
	}
	return addr
}

// GetPostgresPort return the Postgres port
func GetPostgresPort() int {
	port := os.Getenv(PostgresPort)
	if port == "" {
		port = PostgresDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", PostgresPort))
	}
	return p
}

// GetKafkaAddress return the kafka address
func GetKafkaAddress() string {
	addr := os.Getenv(KafkaSeeds)
	if addr == "" {
		addr = Localhost
	}
	return addr
}

// GetKafkaPort return the Kafka port
func GetKafkaPort() int {
	port := os.Getenv(KafkaPort)
	if port == "" {
		port = KafkaDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", KafkaPort))
	}
	return p
}

// GetESAddress return the kafka address
func GetESAddress() string {
	addr := os.Getenv(ESSeeds)
	if addr == "" {
		addr = Localhost
	}
	return addr
}

// GetESPort return the Kafka port
func GetESPort() int {
	port := os.Getenv(ESPort)
	if port == "" {
		port = ESDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", ESPort))
	}
	return p
}
