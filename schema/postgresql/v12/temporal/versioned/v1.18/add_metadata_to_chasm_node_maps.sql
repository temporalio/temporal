ALTER TABLE chasm_node_maps ADD COLUMN metadata BYTEA NOT NULL;
ALTER TABLE chasm_node_maps ADD COLUMN metadata_encoding VARCHAR(16);
ALTER TABLE chasm_node_maps ALTER COLUMN data DROP NOT NULL; -- make nullable
