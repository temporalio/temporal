CREATE TABLE queues (
    queue_type INT NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    metadata_payload MEDIUMBLOB NOT NULL,
    metadata_encoding VARCHAR(16) NOT NULL,
    PRIMARY KEY (queue_type, queue_name)
);

CREATE TABLE queue_messages (
    queue_type INT NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    queue_partition BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    message_payload MEDIUMBLOB NOT NULL,
    message_encoding VARCHAR(16) NOT NULL,
    PRIMARY KEY (
        queue_type,
        queue_name,
        queue_partition,
        message_id
    )
);
