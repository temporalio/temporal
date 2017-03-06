package common

import (
	"fmt"
	"strings"

	"github.com/uber/cadence/common/logging"

	"bytes"
	"os/exec"

	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
)

// NewCassandraCluster creates a cassandra cluster given comma separated list of clusterHosts
func NewCassandraCluster(clusterHosts string, dc string) *gocql.ClusterConfig {
	var hosts []string
	for _, h := range strings.Split(clusterHosts, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.ProtoVersion = 4
	if dc != "" {
		cluster.HostFilter = gocql.DataCentreHostFilter(dc)
	}
	return cluster
}

// CreateCassandraKeyspace creates the keyspace using this session for given replica count
func CreateCassandraKeyspace(s *gocql.Session, keyspace string, replicas int, overwrite bool) (err error) {
	// if overwrite flag is set, drop the keyspace and create a new one
	if overwrite {
		DropCassandraKeyspace(s, keyspace)
	}
	err = s.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
		'class' : 'SimpleStrategy', 'replication_factor' : %d}`, keyspace, replicas)).Exec()
	if err != nil {
		log.WithField(logging.TagErr, err).Error(`create keyspace error`)
		return
	}
	log.WithField(`keyspace`, keyspace).Debug(`created namespace`)

	return
}

// DropCassandraKeyspace drops the given keyspace, if it exists
func DropCassandraKeyspace(s *gocql.Session, keyspace string) (err error) {
	err = s.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	if err != nil {
		log.WithField(logging.TagErr, err).Error(`drop keyspace error`)
		return
	}
	log.WithField(`keyspace`, keyspace).Info(`dropped namespace`)
	return
}

// LoadCassandraSchema loads the schema from the given .cql file on this keyspace using cqlsh
func LoadCassandraSchema(cqlshpath string, fileName string, keyspace string) (err error) {
	// Using cqlsh as I couldn't find a way to execute multiple commands through gocql.Session
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(cqlshpath, fmt.Sprintf("--keyspace=%v", keyspace), fmt.Sprintf("--file=%v", fileName))
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err = cmd.Run()

	// CQLSH doesn't return non-zero for some errors
	if err != nil || len(stderr.String()) > 0 {
		err = fmt.Errorf("LoadSchema %v returned %v. STDERR: %v", cmd.Path, err, stderr.String())
	}
	return
}

// CQLTimestampToUnixNano converts CQL timestamp to UnixNano
func CQLTimestampToUnixNano(milliseconds int64) int64 {
	return milliseconds * 1000 * 1000 // Milliseconds are 10⁻³, nanoseconds are 10⁻⁹, (-3) - (-9) = 6, so multiply by 10⁶
}

// UnixNanoToCQLTimestamp converts UnixNano to CQL timestamp
func UnixNanoToCQLTimestamp(timestamp int64) int64 {
	return timestamp / (1000 * 1000) // Milliseconds are 10⁻³, nanoseconds are 10⁻⁹, (-9) - (-3) = -6, so divide by 10⁶
}
