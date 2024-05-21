-- Stores information about Nexus endpoints
CREATE TABLE nexus_endpoints (
    id             BINARY(16) NOT NULL,
    data           MEDIUMBLOB NOT NULL,  -- temporal.server.api.persistence.v1.NexusEndpoint
    data_encoding  VARCHAR(16) NOT NULL, -- Encoding type used for serialization, in practice this should always be proto3
    version        BIGINT NOT NULL,      -- Version of this row, used for optimistic concurrency
    PRIMARY KEY (id)
);

-- Stores the version of Nexus incoming endpointe as a whole
CREATE TABLE nexus_endpoints_partition_status (
    id      INT NOT NULL DEFAULT 0 CHECK (id = 0),  -- Restrict the primary key to a single value since it will only be used for enpoints
    version BIGINT NOT NULL,                        -- Version of the nexus_endpoints table
    PRIMARY KEY (id)
);
