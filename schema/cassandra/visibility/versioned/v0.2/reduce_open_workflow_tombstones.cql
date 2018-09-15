ALTER TABLE open_executions WITH gc_grace_seconds=60 AND compaction = {
  'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
  'tombstone_threshold': 0.4
};