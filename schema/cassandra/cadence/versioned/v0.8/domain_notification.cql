CREATE TABLE domains_by_name_v2 (
  domains_partition             int,
  name                          text,
  domain                        frozen<domain>,
  config                        frozen<domain_config>,
  replication_config            frozen<domain_replication_config>, -- indicating active cluster and standby cluster used for replication
  is_global_domain              boolean, -- indicating whether a domain is a global domain
  config_version                bigint, -- indicating the version of domain config, excluding the failover / change of active cluster name
  failover_version              bigint, -- indicating the version of active domain only, used for domain failover
  failover_notification_version bigint, -- indicating the last change related to domain failover
  notification_version          bigint,
  PRIMARY KEY (domains_partition, name)
)  WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
   };

ALTER TYPE shard ADD domain_notification_version bigint;