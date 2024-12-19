-- Stores information about Nexus incoming services
CREATE TABLE nexus_incoming_services (
    service_id      BINARY(16) NOT NULL,
    data            MEDIUMBLOB NOT NULL,  -- temporal.server.api.persistence.v1.NexusIncomingService
    data_encoding   VARCHAR(16) NOT NULL, -- Encoding type used for serialization, in practice this should always be proto3
    version         BIGINT NOT NULL,      -- Version of this row, used for optimistic concurrency
    PRIMARY KEY (service_id)
);

-- Stores the version of Nexus incoming services table as a whole
CREATE TABLE nexus_incoming_services_partition_status (
    id      INT NOT NULL DEFAULT 0 CHECK (id = 0),  -- Restrict the primary key to a single value since it will only be used for incoming services
    version BIGINT NOT NULL,                        -- Version of the nexus_incoming_services table
    PRIMARY KEY (id)
);
