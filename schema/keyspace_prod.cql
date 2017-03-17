--- We should really migrate to NetworkTopologyStrategy with DC level RFs once we have dc awareness in place
--- RF of 5 allows us to lose two replicas and still achieve quorum (with 3 nodes)
--- When we move to the multi-dc setup, each DC can have a RF of 3

CREATE KEYSPACE IF NOT EXISTS cadence_staging_dca1a WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dca1' : 3};