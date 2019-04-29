
ALTER TYPE domain_config ADD bad_binaries blob;
ALTER TYPE domain_config ADD bad_binaries_encoding text;

ALTER TYPE workflow_execution ADD auto_reset_points blob;
ALTER TYPE workflow_execution ADD auto_reset_points_encoding text;