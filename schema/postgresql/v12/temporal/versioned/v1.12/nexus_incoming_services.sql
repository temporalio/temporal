-- Stores information about Nexus incoming services
CREATE TABLE nexus_incoming_services (
    service_id      BYTEA NOT NULL,
    data            BYTEA NOT NULL,  -- temporal.server.api.persistence.v1.NexusIncomingService
    data_encoding   VARCHAR(16) NOT NULL, -- Encoding type used for serialization, in practice this should always be proto3
    version         BIGINT NOT NULL,      -- Version of this row, used for optimistic concurrency
    PRIMARY KEY (service_id)
);

-- Stores the version of Nexus services tables as a whole. Should only be one row per Nexus service type.
CREATE TABLE nexus_services_table_versions (
    service_type    INT NOT NULL,       -- Enum for which table this row contains the version of (e.g. Incoming)
    version         BIGINT NOT NULL,    -- Version of the referenced table
    PRIMARY KEY (service_type)
);