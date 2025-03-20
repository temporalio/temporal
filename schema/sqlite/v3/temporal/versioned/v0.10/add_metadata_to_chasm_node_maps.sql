ALTER TABLE chasm_node_maps ADD COLUMN metadata MEDIUMBLOB NOT NULL;
ALTER TABLE chasm_node_maps ADD COLUMN metadata_encoding VARCHAR(16);
